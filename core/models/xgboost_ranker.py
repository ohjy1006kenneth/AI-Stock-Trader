"""Layer 2 XGBoost cross-sectional return ranker.

Trains an XGBoost regressor on Layer 1 features and forward-return labels,
producing cross-sectionally ranked ScoreRecords for daily inference. The
model is serialized to a portable byte bundle for R2 storage.

Typical usage:
    ranker = XGBoostRanker().fit(feature_records, label_records)
    scores = ranker.score(today_features, as_of_date="2025-01-10")
    payload = ranker.to_bytes()  # store in R2
    ranker2 = XGBoostRanker.from_bytes(payload)  # restore
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import pickle
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from core.contracts.schemas import FeatureRecord, ScoreRecord

if TYPE_CHECKING:
    pass

TARGET_LABEL = "forward_return_5d"

# Label columns that must never appear in the model feature matrix.
# Mirrors LABEL_FEATURE_COLUMNS from core.labels.forward_returns (stable contract).
_LABEL_COLUMNS: frozenset[str] = frozenset({
    "forward_return_1d",
    "forward_return_5d",
    "forward_return_20d",
    "forward_log_return_1d",
    "forward_log_return_5d",
    "forward_log_return_20d",
    "survives_to_t1",
    "survives_to_t5",
    "survives_to_t20",
})

# Features excluded from the model input: labels and the string regime tag.
_EXCLUDED_FEATURE_KEYS: frozenset[str] = _LABEL_COLUMNS | {"regime_label"}


@dataclass(frozen=True)
class XGBoostRankerConfig:
    """Hyperparameters for the XGBoost return ranker."""

    n_estimators: int = 200
    max_depth: int = 4
    learning_rate: float = 0.05
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    min_child_weight: int = 10
    reg_alpha: float = 0.1
    reg_lambda: float = 1.0
    random_state: int = 42


class XGBoostRanker:
    """XGBoost regressor that scores stocks cross-sectionally within a date.

    The ranker predicts 5-day forward returns for each stock in a given
    universe, ranks them cross-sectionally to produce rank_score ∈ [0, 1],
    and calibrates a logistic layer to emit pos_prob.
    """

    def __init__(self, config: XGBoostRankerConfig | None = None) -> None:
        self.config = config or XGBoostRankerConfig()
        self._model: Any = None
        self._calibrator: Any = None
        self._feature_columns: tuple[str, ...] = ()
        self._model_version: str = ""

    @property
    def model_version(self) -> str:
        return self._model_version

    @property
    def feature_columns(self) -> tuple[str, ...]:
        return self._feature_columns

    @property
    def is_fitted(self) -> bool:
        return self._model is not None

    def fit(
        self,
        feature_records: Sequence[FeatureRecord],
        label_records: Sequence[FeatureRecord],
    ) -> XGBoostRanker:
        """Fit the ranker on paired feature and label FeatureRecord sequences.

        Args:
            feature_records: Layer 1 FeatureRecords (any date range).
            label_records: FeatureRecords from the label archive, each
                containing ``forward_return_5d`` in their ``features`` dict.

        Returns:
            Self, for chaining.
        """
        xgb = _require_xgboost()
        lr_cls = _require_sklearn_lr()
        import numpy as np

        X, y, feature_columns = _build_training_arrays(feature_records, label_records)
        if len(X) == 0:
            raise ValueError("No training samples after joining features with valid labels")

        self._feature_columns = feature_columns

        model = xgb.XGBRegressor(
            n_estimators=self.config.n_estimators,
            max_depth=self.config.max_depth,
            learning_rate=self.config.learning_rate,
            subsample=self.config.subsample,
            colsample_bytree=self.config.colsample_bytree,
            min_child_weight=self.config.min_child_weight,
            reg_alpha=self.config.reg_alpha,
            reg_lambda=self.config.reg_lambda,
            random_state=self.config.random_state,
            n_jobs=-1,
            verbosity=0,
        )
        model.fit(X, y)
        self._model = model

        raw_preds = model.predict(X)
        y_binary = (np.asarray(y) > 0).astype(int)
        calibrator = lr_cls(max_iter=1000, C=1.0)
        calibrator.fit(raw_preds.reshape(-1, 1), y_binary)
        self._calibrator = calibrator

        train_dates = sorted({r.date for r in feature_records})
        self._model_version = _compute_version(
            config=self.config,
            feature_columns=self._feature_columns,
            train_start=train_dates[0] if train_dates else "",
            train_end=train_dates[-1] if train_dates else "",
            n_samples=int(len(X)),
        )
        return self

    def score(
        self,
        feature_records: Sequence[FeatureRecord],
        as_of_date: str,
    ) -> list[ScoreRecord]:
        """Score all feature records for one date.

        Args:
            feature_records: All available FeatureRecords (any date).
            as_of_date: YYYY-MM-DD date to score; only records matching
                this date are evaluated.

        Returns:
            One ScoreRecord per ticker present on ``as_of_date``.
        """
        if not self.is_fitted:
            raise RuntimeError("Call fit() before score()")

        import numpy as np

        records_on_date = [r for r in feature_records if r.date == as_of_date]
        if not records_on_date:
            return []

        X = _build_inference_array(records_on_date, self._feature_columns)
        raw_preds = self._model.predict(X)
        pos_probs = self._calibrator.predict_proba(raw_preds.reshape(-1, 1))[:, 1]

        n = len(raw_preds)
        ranks = _rank_array(raw_preds)
        rank_scores = ranks / max(n - 1, 1)

        results: list[ScoreRecord] = []
        for i, record in enumerate(records_on_date):
            pos_prob = float(np.clip(pos_probs[i], 0.0, 1.0))
            confidence = min(abs(pos_prob - 0.5) * 2.0, 1.0)
            results.append(
                ScoreRecord(
                    date=as_of_date,
                    ticker=record.ticker,
                    return_score=float(raw_preds[i]),
                    pos_prob=pos_prob,
                    rank_score=float(rank_scores[i]),
                    regime=_extract_regime(record),
                    confidence=confidence,
                    model_version=self._model_version,
                )
            )
        return results

    def to_bytes(self) -> bytes:
        """Serialize the fitted model to a portable byte string for R2 storage."""
        if not self.is_fitted:
            raise RuntimeError("Cannot serialize an unfitted model")

        xgb_buf = io.BytesIO()
        self._model.save_model(xgb_buf)

        bundle = {
            "schema": "xgboost_ranker_v1",
            "xgb_model_bytes": xgb_buf.getvalue(),
            "calibrator_pkl": pickle.dumps(self._calibrator, protocol=4),
            "feature_columns": list(self._feature_columns),
            "config": _config_to_dict(self.config),
            "model_version": self._model_version,
        }
        return pickle.dumps(bundle, protocol=4)

    @classmethod
    def from_bytes(cls, data: bytes) -> XGBoostRanker:
        """Restore a serialized XGBoostRanker from bytes loaded from R2."""
        xgb = _require_xgboost()

        bundle = pickle.loads(data)  # noqa: S301
        if bundle.get("schema") != "xgboost_ranker_v1":
            raise ValueError(f"Unrecognized model bundle schema: {bundle.get('schema')!r}")

        xgb_buf = io.BytesIO(bundle["xgb_model_bytes"])
        model = xgb.XGBRegressor()
        model.load_model(xgb_buf)

        calibrator = pickle.loads(bundle["calibrator_pkl"])  # noqa: S301

        ranker = cls(config=XGBoostRankerConfig(**bundle["config"]))
        ranker._model = model
        ranker._calibrator = calibrator
        ranker._feature_columns = tuple(bundle["feature_columns"])
        ranker._model_version = bundle["model_version"]
        return ranker


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_training_arrays(
    feature_records: Sequence[FeatureRecord],
    label_records: Sequence[FeatureRecord],
) -> tuple[Any, list[float], tuple[str, ...]]:
    """Join features with labels and return (X_ndarray, y_list, feature_columns)."""
    import numpy as np

    # Build label lookup: (date, ticker) -> forward_return_5d
    label_lookup: dict[tuple[str, str], float] = {}
    for record in label_records:
        raw = record.features.get(TARGET_LABEL)
        if raw is None:
            continue
        try:
            val = float(raw)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(val):
            continue
        label_lookup[(record.date, record.ticker)] = val

    # Discover the union of numeric, non-excluded feature keys
    all_keys: set[str] = set()
    for record in feature_records:
        if (record.date, record.ticker) not in label_lookup:
            continue
        for key, value in record.features.items():
            if key in _EXCLUDED_FEATURE_KEYS:
                continue
            if not isinstance(value, (int, float)):
                continue
            if isinstance(value, float) and not math.isfinite(value):
                continue
            all_keys.add(key)

    feature_columns: tuple[str, ...] = tuple(sorted(all_keys))
    if not feature_columns:
        return np.empty((0, 0)), [], ()

    # Build X and y
    rows: list[list[float]] = []
    y_values: list[float] = []

    for record in feature_records:
        key = (record.date, record.ticker)
        if key not in label_lookup:
            continue
        row = [
            _safe_float(record.features.get(col)) for col in feature_columns
        ]
        rows.append(row)
        y_values.append(label_lookup[key])

    if not rows:
        return np.empty((0, len(feature_columns))), [], feature_columns

    X = np.array(rows, dtype=float)
    return X, y_values, feature_columns


def _build_inference_array(
    records: Sequence[FeatureRecord],
    feature_columns: tuple[str, ...],
) -> Any:
    """Build X matrix for inference using the training-time column order."""
    import numpy as np

    rows = [
        [_safe_float(record.features.get(col)) for col in feature_columns]
        for record in records
    ]
    return np.array(rows, dtype=float)


def _rank_array(arr: Any) -> Any:
    """Return 0-based ascending ranks (0 = lowest) as a float array."""
    import numpy as np

    arr = np.asarray(arr, dtype=float)
    n = len(arr)
    sorted_indices = np.argsort(arr)
    ranks = np.empty(n, dtype=float)
    ranks[sorted_indices] = np.arange(n, dtype=float)
    return ranks


def _extract_regime(record: FeatureRecord) -> str | None:
    """Return the string regime label from a FeatureRecord if present."""
    raw = record.features.get("regime_label")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return None


def _safe_float(value: Any) -> float:
    """Convert a feature value to float, returning 0.0 for missing or non-finite."""
    if value is None:
        return 0.0
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0
    return f if math.isfinite(f) else 0.0


def _config_to_dict(config: XGBoostRankerConfig) -> dict[str, Any]:
    """Serialize config to a JSON-safe dict."""
    return {
        "n_estimators": config.n_estimators,
        "max_depth": config.max_depth,
        "learning_rate": config.learning_rate,
        "subsample": config.subsample,
        "colsample_bytree": config.colsample_bytree,
        "min_child_weight": config.min_child_weight,
        "reg_alpha": config.reg_alpha,
        "reg_lambda": config.reg_lambda,
        "random_state": config.random_state,
    }


def _compute_version(
    *,
    config: XGBoostRankerConfig,
    feature_columns: tuple[str, ...],
    train_start: str,
    train_end: str,
    n_samples: int,
) -> str:
    """Compute a short deterministic model version hash."""
    payload = json.dumps(
        {
            "config": _config_to_dict(config),
            "features": list(feature_columns),
            "train_start": train_start,
            "train_end": train_end,
            "n_samples": n_samples,
        },
        sort_keys=True,
    )
    return "v1-" + hashlib.sha256(payload.encode()).hexdigest()[:12]


def _require_xgboost() -> Any:
    try:
        import xgboost as xgb

        return xgb
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "xgboost is required for Layer 2 model training and inference. "
            "Install it via requirements/modal.txt."
        ) from exc


def _require_sklearn_lr() -> Any:
    try:
        from sklearn.linear_model import LogisticRegression

        return LogisticRegression
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "scikit-learn is required for pos_prob calibration."
        ) from exc

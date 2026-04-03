from __future__ import annotations

import base64
import json
import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import xgboost as xgb
from sklearn.linear_model import LogisticRegression


FEATURE_NAMES = [
    "return_1d",
    "return_5d",
    "return_10d",
    "return_21d",
    "return_63d",
    "range_ratio_last",
    "gap_ratio_last",
    "intraday_return_last",
    "volume_ratio_last",
    "volume_ratio_5d",
    "realized_vol_5d",
    "realized_vol_21d",
    "realized_vol_63d",
    "atr_ratio_14d",
    "sma_5_over_21",
    "sma_10_over_21",
    "sma_21_over_63",
    "drawdown_21d",
    "beta_to_spy_21d",
    "corr_to_spy_21d",
    "relative_strength_vs_spy_21d",
    "macro_spy_return_5d",
    "macro_spy_return_21d",
    "macro_spy_return_63d",
    "macro_spy_realized_vol_21d",
    "macro_spy_realized_vol_63d",
    "macro_spy_drawdown_21d",
    "macro_spy_sma_21_over_63",
    "market_cap_log",
    "average_volume_log",
    "net_margin",
    "debt_to_equity",
    "revenue_growth",
    "operating_margin",
    "return_on_equity",
    "free_cash_flow_yield",
    "sector_peer_return_21d_mean",
    "sector_peer_vol_21d_mean",
    "sector_relative_return_21d",
    "sector_relative_vol_21d",
    "sector_peer_count",
    "news_count",
    "news_volume",
    "same_day_news_count",
    "rolling_news_window_days",
    "news_count_3d",
    "news_count_7d",
    "news_days_with_coverage_7d",
    "news_source_count_7d",
    "days_since_last_news_7d",
    "finbert_positive_prob_mean",
    "finbert_negative_prob_mean",
    "finbert_neutral_prob_mean",
    "finbert_sentiment_score_mean",
    "finbert_positive_prob_recency_weighted",
    "finbert_negative_prob_recency_weighted",
    "finbert_neutral_prob_recency_weighted",
    "finbert_sentiment_score_recency_weighted",
    "finbert_sentiment_score_mean_3d",
    "finbert_sentiment_score_mean_7d",
    "finbert_sentiment_score_recency_weighted_3d",
    "finbert_sentiment_score_recency_weighted_7d",
    "sentiment_acceleration_3d_vs_7d",
    "news_count_surprise_3d_vs_7d",
    "finbert_article_age_hours_min",
    "finbert_article_age_hours_max",
    "finbert_article_age_hours_mean",
    "finbert_recency_weight_sum",
]


@dataclass
class DatasetSplit:
    name: str
    rows: np.ndarray
    targets: np.ndarray
    metadata: list[dict[str, Any]]


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


class LegacyHybridSignalEnsemble:
    def __init__(self, *, feature_names: list[str], means: list[float], stds: list[float], members: list[dict[str, Any]]):
        self.feature_names = feature_names
        self.means = np.asarray(means, dtype=np.float32)
        self.stds = np.asarray([value if abs(value) > 1e-12 else 1.0 for value in stds], dtype=np.float32)
        self.members = members

    def predict(self, feature_row: list[float]) -> dict[str, float]:
        features = np.asarray(feature_row, dtype=np.float32)
        scaled = (features - self.means) / self.stds
        member_probs = []
        for member in self.members:
            weights = np.asarray(member["weights"], dtype=np.float32)
            bias = float(member.get("bias", 0.0))
            margin = float(np.dot(scaled, weights) + bias)
            member_probs.append(_sigmoid(margin))
        probs = np.asarray(member_probs, dtype=np.float32)
        signal_probability = float(np.mean(probs)) if len(probs) else 0.5
        predictive_variance = float(np.var(probs)) if len(probs) > 1 else float(signal_probability * (1.0 - signal_probability))
        confidence = min(1.0, max(0.0, abs(signal_probability - 0.5) * 2.0))
        raw_margin = float(signal_probability - 0.5)
        return {
            "signal_probability": signal_probability,
            "confidence": confidence,
            "predictive_variance": predictive_variance,
            "raw_margin": raw_margin,
        }


class CalibratedXGBoostSignalModel:
    def __init__(
        self,
        *,
        booster: xgb.Booster,
        feature_names: list[str],
        calibration_coef: float,
        calibration_intercept: float,
    ):
        self.booster = booster
        self.feature_names = feature_names
        self.calibration_coef = float(calibration_coef)
        self.calibration_intercept = float(calibration_intercept)

    def predict(self, feature_row: list[float]) -> dict[str, float]:
        array = np.asarray([feature_row], dtype=np.float32)
        matrix = xgb.DMatrix(array, feature_names=self.feature_names)
        raw_margin = float(self.booster.predict(matrix, output_margin=True)[0])
        signal_probability = _sigmoid((self.calibration_coef * raw_margin) + self.calibration_intercept)
        confidence = min(1.0, max(0.0, abs(signal_probability - 0.5) * 2.0))
        predictive_variance = signal_probability * (1.0 - signal_probability)
        return {
            "signal_probability": signal_probability,
            "confidence": confidence,
            "predictive_variance": predictive_variance,
            "raw_margin": raw_margin,
        }

    def predict_many(self, rows: np.ndarray) -> dict[str, np.ndarray]:
        matrix = xgb.DMatrix(rows.astype(np.float32), feature_names=self.feature_names)
        raw_margins = self.booster.predict(matrix, output_margin=True)
        calibrated = 1.0 / (1.0 + np.exp(-((self.calibration_coef * raw_margins) + self.calibration_intercept)))
        confidence = np.clip(np.abs(calibrated - 0.5) * 2.0, 0.0, 1.0)
        variance = calibrated * (1.0 - calibrated)
        return {
            "signal_probability": calibrated,
            "confidence": confidence,
            "predictive_variance": variance,
            "raw_margin": raw_margins,
        }

    def feature_importances(self) -> list[dict[str, float | str]]:
        gain_map = self.booster.get_score(importance_type="gain")
        weight_map = self.booster.get_score(importance_type="weight")
        cover_map = self.booster.get_score(importance_type="cover")
        rows = []
        for name in self.feature_names:
            rows.append({
                "feature": name,
                "gain": float(gain_map.get(name, 0.0)),
                "weight": float(weight_map.get(name, 0.0)),
                "cover": float(cover_map.get(name, 0.0)),
            })
        rows.sort(key=lambda row: (row["gain"], row["weight"]), reverse=True)
        return rows

    def to_artifact(self) -> dict[str, Any]:
        raw_model = self.booster.save_raw(raw_format="json")
        return {
            "model_type": "xgboost_signal_calibrated_v1",
            "feature_names": self.feature_names,
            "booster_format": "json_base64",
            "booster_bytes_base64": base64.b64encode(raw_model).decode("ascii"),
            "calibration": {
                "method": "platt_on_validation_margin",
                "coef": self.calibration_coef,
                "intercept": self.calibration_intercept,
            },
        }

    @classmethod
    def from_artifact(cls, payload: dict[str, Any]) -> "CalibratedXGBoostSignalModel | LegacyHybridSignalEnsemble":
        if "booster_bytes_base64" not in payload:
            scaler = payload.get("scaler", {})
            return LegacyHybridSignalEnsemble(
                feature_names=list(payload["feature_names"]),
                means=list(scaler.get("means", [])),
                stds=list(scaler.get("stds", [])),
                members=list(payload.get("members", [])),
            )
        booster = xgb.Booster()
        booster.load_model(bytearray(base64.b64decode(payload["booster_bytes_base64"])))
        calibration = payload.get("calibration", {})
        return cls(
            booster=booster,
            feature_names=list(payload["feature_names"]),
            calibration_coef=float(calibration.get("coef", 1.0)),
            calibration_intercept=float(calibration.get("intercept", 0.0)),
        )


def _safe_float(sample: dict[str, Any], key: str) -> float:
    value = sample.get(key)
    if value in (None, ""):
        return 0.0
    return float(value)


def extract_feature_row(sample: dict[str, Any]) -> list[float]:
    return [_safe_float(sample, name) for name in FEATURE_NAMES]


def vectorize_samples(samples: list[dict[str, Any]]) -> tuple[np.ndarray, np.ndarray, list[dict[str, Any]]]:
    rows = np.asarray([extract_feature_row(sample) for sample in samples], dtype=np.float32)
    targets = np.asarray([int(sample.get("target_positive_return", 0)) for sample in samples], dtype=np.int32)
    metadata = [
        {
            "ticker": sample.get("ticker"),
            "as_of_date": sample.get("as_of_date"),
            "target_date": sample.get("target_date"),
        }
        for sample in samples
    ]
    return rows, targets, metadata


def _time_ordered_splits(samples: list[dict[str, Any]]) -> tuple[DatasetSplit, DatasetSplit, DatasetSplit]:
    ordered = sorted(samples, key=lambda row: (str(row.get("as_of_date") or ""), str(row.get("ticker") or "")))
    total = len(ordered)
    if total < 300:
        raise ValueError("need_at_least_300_samples_for_time_split_xgboost_training")

    unique_dates = sorted({str(row.get("as_of_date") or "") for row in ordered})
    if len(unique_dates) < 3:
        raise ValueError("need_at_least_3_unique_dates_for_time_split_xgboost_training")

    train_date_end = max(int(len(unique_dates) * 0.70), 1)
    valid_date_end = max(int(len(unique_dates) * 0.85), train_date_end + 1)
    valid_date_end = min(valid_date_end, len(unique_dates) - 1)

    train_dates = set(unique_dates[:train_date_end])
    valid_dates = set(unique_dates[train_date_end:valid_date_end])
    test_dates = set(unique_dates[valid_date_end:])

    train_rows = [row for row in ordered if str(row.get("as_of_date") or "") in train_dates]
    valid_rows = [row for row in ordered if str(row.get("as_of_date") or "") in valid_dates]
    test_rows = [row for row in ordered if str(row.get("as_of_date") or "") in test_dates]
    if not train_rows or not valid_rows or not test_rows:
        raise ValueError("invalid_time_split_for_xgboost_training")

    def build(name: str, subset: list[dict[str, Any]]) -> DatasetSplit:
        rows, targets, metadata = vectorize_samples(subset)
        return DatasetSplit(name=name, rows=rows, targets=targets, metadata=metadata)

    return build("train", train_rows), build("validation", valid_rows), build("test", test_rows)


def _log_loss(targets: np.ndarray, probabilities: np.ndarray) -> float:
    probs = np.clip(probabilities, 1e-6, 1.0 - 1e-6)
    return float(-np.mean((targets * np.log(probs)) + ((1 - targets) * np.log(1 - probs))))


def _accuracy(targets: np.ndarray, probabilities: np.ndarray) -> float:
    return float(np.mean((probabilities >= 0.5) == targets))


def _brier_score(targets: np.ndarray, probabilities: np.ndarray) -> float:
    return float(np.mean((probabilities - targets) ** 2))


def _auc(targets: np.ndarray, probabilities: np.ndarray) -> float:
    positive = probabilities[targets == 1]
    negative = probabilities[targets == 0]
    if len(positive) == 0 or len(negative) == 0:
        return 0.5
    comparisons = 0.0
    total = 0
    for p in positive:
        comparisons += float(np.sum(p > negative)) + (0.5 * float(np.sum(p == negative)))
        total += len(negative)
    return float(comparisons / total) if total else 0.5


def _calibration_buckets(targets: np.ndarray, probabilities: np.ndarray, buckets: int = 10) -> list[dict[str, float | int]]:
    rows = []
    for bucket in range(buckets):
        lower = bucket / buckets
        upper = (bucket + 1) / buckets
        if bucket == buckets - 1:
            mask = (probabilities >= lower) & (probabilities <= upper)
        else:
            mask = (probabilities >= lower) & (probabilities < upper)
        if not np.any(mask):
            rows.append({"bucket": bucket, "count": 0, "avg_probability": 0.0, "observed_positive_rate": 0.0})
            continue
        bucket_probs = probabilities[mask]
        bucket_targets = targets[mask]
        rows.append({
            "bucket": bucket,
            "count": int(mask.sum()),
            "avg_probability": float(bucket_probs.mean()),
            "observed_positive_rate": float(bucket_targets.mean()),
        })
    return rows


def train_hybrid_signal_ensemble(
    samples: list[dict[str, Any]],
    *,
    ensemble_size: int = 1,
    learning_rate: float = 0.05,
    epochs: int = 250,
    l2: float = 1e-4,
    seed: int = 0,
) -> tuple[CalibratedXGBoostSignalModel, dict[str, Any]]:
    del ensemble_size  # legacy arg retained for CLI compatibility
    train_split, validation_split, test_split = _time_ordered_splits(samples)

    params = {
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "auc"],
        "eta": float(learning_rate),
        "max_depth": 4,
        "min_child_weight": 5,
        "subsample": 0.85,
        "colsample_bytree": 0.85,
        "lambda": max(float(l2), 1e-8),
        "alpha": 0.0,
        "seed": int(seed),
        "tree_method": "hist",
    }

    dtrain = xgb.DMatrix(train_split.rows, label=train_split.targets, feature_names=FEATURE_NAMES)
    dvalid = xgb.DMatrix(validation_split.rows, label=validation_split.targets, feature_names=FEATURE_NAMES)
    booster = xgb.train(
        params=params,
        dtrain=dtrain,
        num_boost_round=max(int(epochs), 10),
        evals=[(dtrain, "train"), (dvalid, "validation")],
        early_stopping_rounds=25,
        verbose_eval=False,
    )

    valid_margins = booster.predict(dvalid, output_margin=True)
    calibrator = LogisticRegression(random_state=seed)
    calibrator.fit(valid_margins.reshape(-1, 1), validation_split.targets)
    model = CalibratedXGBoostSignalModel(
        booster=booster,
        feature_names=FEATURE_NAMES,
        calibration_coef=float(calibrator.coef_[0][0]),
        calibration_intercept=float(calibrator.intercept_[0]),
    )

    split_metrics: dict[str, Any] = {}
    for split in (train_split, validation_split, test_split):
        preds = model.predict_many(split.rows)
        probabilities = preds["signal_probability"]
        split_metrics[split.name] = {
            "samples": int(len(split.targets)),
            "positive_rate": float(split.targets.mean()),
            "log_loss": _log_loss(split.targets, probabilities),
            "accuracy": _accuracy(split.targets, probabilities),
            "brier_score": _brier_score(split.targets, probabilities),
            "roc_auc": _auc(split.targets, probabilities),
            "calibration": _calibration_buckets(split.targets, probabilities),
        }

    test_preds = model.predict_many(test_split.rows)
    top_features = model.feature_importances()[:20]
    diagnostics = {
        "best_iteration": int(booster.best_iteration),
        "best_score": float(booster.best_score),
        "split_summary": {
            "train": {
                "start": train_split.metadata[0]["as_of_date"],
                "end": train_split.metadata[-1]["as_of_date"],
            },
            "validation": {
                "start": validation_split.metadata[0]["as_of_date"],
                "end": validation_split.metadata[-1]["as_of_date"],
            },
            "test": {
                "start": test_split.metadata[0]["as_of_date"],
                "end": test_split.metadata[-1]["as_of_date"],
            },
        },
        "feature_importance_top20": top_features,
        "test_probability_summary": {
            "min": float(np.min(test_preds["signal_probability"])),
            "max": float(np.max(test_preds["signal_probability"])),
            "mean": float(np.mean(test_preds["signal_probability"])),
        },
        "test_prediction_examples": [
            {
                **test_split.metadata[idx],
                "signal_probability": float(test_preds["signal_probability"][idx]),
                "confidence": float(test_preds["confidence"][idx]),
                "predictive_variance": float(test_preds["predictive_variance"][idx]),
                "target_positive_return": int(test_split.targets[idx]),
            }
            for idx in range(min(15, len(test_split.targets)))
        ],
    }

    return model, {
        "train_loss": split_metrics["train"]["log_loss"],
        "train_accuracy": split_metrics["train"]["accuracy"],
        "samples": int(len(samples)),
        "ensemble_size": 1,
        "feature_names": FEATURE_NAMES,
        "model_type": "xgboost_signal_calibrated_v1",
        "split_metrics": split_metrics,
        "diagnostics": diagnostics,
    }


HybridSignalEnsemble = CalibratedXGBoostSignalModel


def load_artifact(path: str) -> CalibratedXGBoostSignalModel:
    with open(path) as f:
        payload = json.load(f)
    return CalibratedXGBoostSignalModel.from_artifact(payload.get("artifact", payload))

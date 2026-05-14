"""Layer 1.5 HMM regime fitting and feature emission."""
from __future__ import annotations

import importlib
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from core.contracts.schemas import FeatureRecord
from core.features.regime_training import HMM_TRAINING_FEATURE_COLUMNS

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd

REGIME_LABELS: tuple[str, str, str] = ("bear", "sideways", "bull")
REGIME_PROBABILITY_COLUMNS: tuple[str, ...] = (
    "regime_prob_bear",
    "regime_prob_sideways",
    "regime_prob_bull",
)
HMM_REGIME_FEATURE_COLUMNS: tuple[str, ...] = (
    "regime_label",
    "regime_confidence",
    *REGIME_PROBABILITY_COLUMNS,
)
HMM_REGIME_COLUMNS: tuple[str, ...] = ("date", *HMM_REGIME_FEATURE_COLUMNS)
REGIME_PROBABILITY_SUM_TOLERANCE = 1e-4


@dataclass(frozen=True)
class HMMRegimeConfig:
    """Configuration for deterministic diagonal-Gaussian HMM fitting."""

    n_states: int = 3
    max_iterations: int = 100
    tolerance: float = 1e-4
    covariance_floor: float = 1e-4
    transition_smoothing: float = 1e-2
    min_training_rows: int = 30

    def __post_init__(self) -> None:
        """Validate HMM hyperparameters."""
        if self.n_states != len(REGIME_LABELS):
            raise ValueError("n_states must be 3 for bear/sideways/bull regime labels")
        if self.max_iterations <= 0:
            raise ValueError("max_iterations must be positive")
        if self.tolerance < 0.0:
            raise ValueError("tolerance must be non-negative")
        if self.covariance_floor <= 0.0:
            raise ValueError("covariance_floor must be positive")
        if self.transition_smoothing <= 0.0:
            raise ValueError("transition_smoothing must be positive")
        if self.min_training_rows < self.n_states:
            raise ValueError("min_training_rows must be at least n_states")


@dataclass(frozen=True)
class HMMRegimeModel:
    """Fitted HMM parameters and scaling metadata."""

    feature_columns: tuple[str, ...]
    train_start_date: str
    train_end_date: str
    center: np.ndarray
    scale: np.ndarray
    start_probability: np.ndarray
    transition_matrix: np.ndarray
    means: np.ndarray
    variances: np.ndarray
    label_by_state: tuple[str, ...]
    log_likelihood: float
    iterations: int


@dataclass(frozen=True)
class HMMRegimeReadiness:
    """Readiness summary for one bounded HMM training/inference window."""

    feature_columns: tuple[str, ...]
    dropped_feature_columns: tuple[str, ...]
    training_rows: int
    complete_training_rows: int
    min_training_rows: int
    inference_dates: tuple[str, ...]
    complete_inference_dates: tuple[str, ...]
    incomplete_inference_feature_gaps: dict[str, tuple[str, ...]]

    @property
    def has_active_feature_columns(self) -> bool:
        """Return True when the bounded training window exposes usable features."""
        return bool(self.feature_columns)

    @property
    def has_sufficient_training_rows(self) -> bool:
        """Return True when the bounded train window has enough complete rows."""
        return self.complete_training_rows >= self.min_training_rows

    @property
    def can_fit_model(self) -> bool:
        """Return True when the HMM can be fit for this bounded window."""
        return self.has_active_feature_columns and self.has_sufficient_training_rows


def fit_hmm_regime_model(
    training_frame: pd.DataFrame,
    *,
    train_end_date: str,
    train_start_date: str | None = None,
    config: HMMRegimeConfig | None = None,
    feature_columns: tuple[str, ...] = HMM_TRAINING_FEATURE_COLUMNS,
) -> HMMRegimeModel:
    """Fit a diagonal-Gaussian HMM on rows strictly before `train_end_date`."""
    np = _require_numpy()
    active_config = config or HMMRegimeConfig()
    _validate_training_frame(training_frame, feature_columns)
    active_feature_columns = _active_feature_columns(
        training_frame,
        feature_columns=feature_columns,
        train_start_date=train_start_date,
        train_end_date=train_end_date,
    )
    if not active_feature_columns:
        raise ValueError("HMM training window has no usable feature columns")
    train_rows = _bounded_complete_rows(
        training_frame,
        feature_columns=active_feature_columns,
        train_start_date=train_start_date,
        train_end_date=train_end_date,
    )
    if len(train_rows) < active_config.min_training_rows:
        raise ValueError(
            "HMM training window has fewer complete rows than min_training_rows: "
            f"{len(train_rows)} < {active_config.min_training_rows}"
        )

    raw_matrix = train_rows.loc[:, list(active_feature_columns)].astype(float).to_numpy()
    normalized_matrix, center, scale = _standardize(raw_matrix, np)
    parameters = _fit_gaussian_hmm(normalized_matrix, active_config, np)
    labels = _semantic_labels(parameters["means"], active_feature_columns)
    return HMMRegimeModel(
        feature_columns=active_feature_columns,
        train_start_date=str(train_rows.iloc[0]["date"]),
        train_end_date=train_end_date,
        center=center,
        scale=scale,
        start_probability=parameters["start_probability"],
        transition_matrix=parameters["transition_matrix"],
        means=parameters["means"],
        variances=parameters["variances"],
        label_by_state=labels,
        log_likelihood=float(parameters["log_likelihood"]),
        iterations=int(parameters["iterations"]),
    )


def inspect_hmm_regime_readiness(
    training_frame: pd.DataFrame,
    *,
    train_end_date: str,
    inference_dates: list[str] | None = None,
    train_start_date: str | None = None,
    config: HMMRegimeConfig | None = None,
    feature_columns: tuple[str, ...] = HMM_TRAINING_FEATURE_COLUMNS,
) -> HMMRegimeReadiness:
    """Inspect whether a bounded HMM window can emit non-null regime features."""
    pd = _require_pandas()
    active_config = config or HMMRegimeConfig()
    _validate_training_frame(training_frame, feature_columns)
    active_feature_columns = _active_feature_columns(
        training_frame,
        feature_columns=feature_columns,
        train_start_date=train_start_date,
        train_end_date=train_end_date,
    )
    dropped_feature_columns = tuple(
        column for column in feature_columns if column not in active_feature_columns
    )
    bounded_rows = _bounded_rows(
        training_frame,
        train_start_date=train_start_date,
        train_end_date=train_end_date,
    )
    complete_rows = (
        bounded_rows[_complete_row_mask(bounded_rows, active_feature_columns)]
        if active_feature_columns
        else bounded_rows.iloc[0:0].copy()
    )
    infer_rows = _requested_inference_rows(
        training_frame,
        train_end_date=train_end_date,
        inference_dates=inference_dates,
    )
    incomplete_inference_feature_gaps: dict[str, tuple[str, ...]] = {}
    complete_inference_dates: list[str] = []
    if len(infer_rows) > 0:
        numeric = (
            infer_rows.loc[:, list(active_feature_columns)].apply(pd.to_numeric, errors="coerce")
            if active_feature_columns
            else pd.DataFrame(index=infer_rows.index)
        )
        for row_index, row in infer_rows.reset_index(drop=True).iterrows():
            date_text = str(row["date"])
            if not active_feature_columns:
                incomplete_inference_feature_gaps[date_text] = tuple(feature_columns)
                continue
            missing = tuple(
                column
                for column in active_feature_columns
                if pd.isna(numeric.iloc[row_index][column])
            )
            if missing:
                incomplete_inference_feature_gaps[date_text] = missing
            else:
                complete_inference_dates.append(date_text)
    return HMMRegimeReadiness(
        feature_columns=active_feature_columns,
        dropped_feature_columns=dropped_feature_columns,
        training_rows=len(bounded_rows),
        complete_training_rows=len(complete_rows),
        min_training_rows=active_config.min_training_rows,
        inference_dates=tuple(str(value) for value in infer_rows["date"].tolist()),
        complete_inference_dates=tuple(complete_inference_dates),
        incomplete_inference_feature_gaps=incomplete_inference_feature_gaps,
    )


def emit_hmm_regime_features(
    training_frame: pd.DataFrame,
    *,
    model: HMMRegimeModel,
    inference_dates: list[str] | None = None,
) -> pd.DataFrame:
    """Emit market-wide regime probabilities for dates after the model window."""
    pd = _require_pandas()
    np = _require_numpy()
    _validate_training_frame(training_frame, model.feature_columns)
    infer_rows = _inference_rows(training_frame, model=model, inference_dates=inference_dates)
    if len(infer_rows) == 0:
        return pd.DataFrame(columns=list(HMM_REGIME_COLUMNS))

    complete_mask = infer_rows["is_complete"].astype(bool)
    result = pd.DataFrame({"date": infer_rows["date"].tolist()})
    result["regime_label"] = None
    for column in HMM_REGIME_FEATURE_COLUMNS[1:]:
        result[column] = math.nan

    if complete_mask.any():
        matrix = infer_rows.loc[complete_mask, list(model.feature_columns)].astype(float).to_numpy()
        normalized = (matrix - model.center) / model.scale
        posterior = _posterior_probabilities(normalized, model, np)
        complete_indices = result.index[complete_mask].tolist()
        for row_offset, result_index in enumerate(complete_indices):
            probabilities_by_label = _probabilities_by_label(posterior[row_offset], model)
            regime_label = max(probabilities_by_label, key=probabilities_by_label.get)
            result.loc[result_index, "regime_label"] = regime_label
            result.loc[result_index, "regime_confidence"] = probabilities_by_label[regime_label]
            for label in REGIME_LABELS:
                result.loc[result_index, f"regime_prob_{label}"] = probabilities_by_label[label]

    return result[list(HMM_REGIME_COLUMNS)]


def fit_and_emit_hmm_regime_features(
    training_frame: pd.DataFrame,
    *,
    train_end_date: str,
    inference_dates: list[str] | None = None,
    train_start_date: str | None = None,
    config: HMMRegimeConfig | None = None,
) -> pd.DataFrame:
    """Fit an explicitly bounded HMM and emit downstream regime features."""
    model = fit_hmm_regime_model(
        training_frame,
        train_start_date=train_start_date,
        train_end_date=train_end_date,
        config=config,
    )
    return emit_hmm_regime_features(
        training_frame,
        model=model,
        inference_dates=inference_dates,
    )


def regime_features_to_records(features: pd.DataFrame) -> list[FeatureRecord]:
    """Convert HMM regime output rows into validated FeatureRecords."""
    _validate_regime_feature_frame(features)
    records: list[FeatureRecord] = []
    for row in features.to_dict(orient="records"):
        records.append(
            FeatureRecord(
                date=str(row["date"]),
                ticker="__REGIME__",
                features={
                    "regime_label": _normalize_optional_string(row.get("regime_label")),
                    "regime_confidence": _normalize_optional_float(row.get("regime_confidence")),
                    "regime_prob_bear": _normalize_optional_float(row.get("regime_prob_bear")),
                    "regime_prob_sideways": _normalize_optional_float(
                        row.get("regime_prob_sideways")
                    ),
                    "regime_prob_bull": _normalize_optional_float(row.get("regime_prob_bull")),
                },
            )
        )
    return records


def validate_hmm_regime_probabilities(
    features: pd.DataFrame,
    *,
    tolerance: float = REGIME_PROBABILITY_SUM_TOLERANCE,
) -> list[str]:
    """Return validation errors for populated HMM regime rows."""
    _validate_regime_feature_frame(features)
    errors: list[str] = []
    for row in features.to_dict(orient="records"):
        date_text = str(row["date"])
        label = _normalize_optional_string(row.get("regime_label"))
        confidence = _normalize_optional_float(row.get("regime_confidence"))
        probabilities = {
            label_name: _normalize_optional_float(row.get(column_name))
            for label_name, column_name in zip(
                REGIME_LABELS,
                REGIME_PROBABILITY_COLUMNS,
                strict=True,
            )
        }
        present_count = sum(
            value is not None for value in (label, confidence, *probabilities.values())
        )
        if present_count == 0:
            continue
        if present_count != len(HMM_REGIME_FEATURE_COLUMNS):
            errors.append(f"{date_text}: partially populated regime fields")
            continue
        if label not in REGIME_LABELS:
            errors.append(f"{date_text}: invalid regime label {label!r}")
            continue
        if confidence is None or any(value is None for value in probabilities.values()):
            errors.append(f"{date_text}: regime fields must be fully populated or fully null")
            continue
        if confidence < 0.0 or confidence > 1.0:
            errors.append(f"{date_text}: regime_confidence out of range")
        if any(value < 0.0 or value > 1.0 for value in probabilities.values()):
            errors.append(f"{date_text}: regime probabilities out of range")
        probability_sum = sum(probabilities.values())
        if abs(probability_sum - 1.0) > tolerance:
            errors.append(f"{date_text}: regime probabilities sum to {probability_sum:.6f}")
        max_label = max(probabilities, key=probabilities.get)
        if label != max_label:
            errors.append(
                f"{date_text}: regime_label={label!r} does not match max probability {max_label!r}"
            )
        if abs(confidence - probabilities[label]) > tolerance:
            errors.append(
                f"{date_text}: regime_confidence does not match regime_prob_{label}"
            )
    return errors


def _fit_gaussian_hmm(
    matrix: np.ndarray,
    config: HMMRegimeConfig,
    np: Any,
) -> dict[str, Any]:
    """Fit a diagonal-Gaussian HMM with Baum-Welch updates."""
    labels = _initial_cluster_labels(matrix, config.n_states, np)
    start_probability, transition_matrix, means, variances = _initial_parameters(
        matrix,
        labels,
        config,
        np,
    )
    previous_log_likelihood = -math.inf
    iterations = 0

    for iteration in range(1, config.max_iterations + 1):
        log_emissions = _log_gaussian_emissions(matrix, means, variances, np)
        log_alpha, log_likelihood = _forward_log(
            log_emissions,
            start_probability,
            transition_matrix,
            np,
        )
        log_beta = _backward_log(log_emissions, transition_matrix, np)
        gamma = np.exp(log_alpha + log_beta - log_likelihood)
        xi_sum = _expected_transition_counts(
            log_alpha,
            log_beta,
            log_emissions,
            transition_matrix,
            log_likelihood,
            np,
        )

        start_probability = _normalize_vector(gamma[0] + config.transition_smoothing, np)
        transition_matrix = _normalize_rows(xi_sum + config.transition_smoothing, np)
        means, variances = _update_emissions(matrix, gamma, means, variances, config, np)
        iterations = iteration

        if abs(log_likelihood - previous_log_likelihood) <= config.tolerance:
            break
        previous_log_likelihood = log_likelihood

    return {
        "start_probability": start_probability,
        "transition_matrix": transition_matrix,
        "means": means,
        "variances": variances,
        "log_likelihood": log_likelihood,
        "iterations": iterations,
    }


def _initial_cluster_labels(matrix: np.ndarray, n_states: int, np: Any) -> np.ndarray:
    """Return deterministic k-means labels for HMM initialization."""
    score = matrix[:, 0]
    order = np.argsort(score)
    centers = matrix[order[np.linspace(0, len(order) - 1, n_states).astype(int)]].copy()
    labels = np.zeros(len(matrix), dtype=int)

    for _ in range(20):
        distances = ((matrix[:, None, :] - centers[None, :, :]) ** 2).sum(axis=2)
        next_labels = distances.argmin(axis=1)
        if np.array_equal(next_labels, labels):
            break
        labels = next_labels
        for state in range(n_states):
            state_rows = matrix[labels == state]
            if len(state_rows) > 0:
                centers[state] = state_rows.mean(axis=0)
    return labels


def _initial_parameters(
    matrix: np.ndarray,
    labels: np.ndarray,
    config: HMMRegimeConfig,
    np: Any,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Build initial HMM probabilities and diagonal emissions."""
    n_states = config.n_states
    start_probability = _normalize_vector(
        np.bincount(labels[:1], minlength=n_states).astype(float) + config.transition_smoothing,
        np,
    )
    transition_counts = np.full((n_states, n_states), config.transition_smoothing)
    for current_state, next_state in zip(labels[:-1], labels[1:], strict=False):
        transition_counts[current_state, next_state] += 1.0
    transition_matrix = _normalize_rows(transition_counts, np)

    global_mean = matrix.mean(axis=0)
    global_variance = matrix.var(axis=0) + config.covariance_floor
    means = np.zeros((n_states, matrix.shape[1]))
    variances = np.zeros((n_states, matrix.shape[1]))
    for state in range(n_states):
        state_rows = matrix[labels == state]
        if len(state_rows) == 0:
            means[state] = global_mean
            variances[state] = global_variance
            continue
        means[state] = state_rows.mean(axis=0)
        variances[state] = np.maximum(state_rows.var(axis=0), config.covariance_floor)
    return start_probability, transition_matrix, means, variances


def _log_gaussian_emissions(
    matrix: np.ndarray,
    means: np.ndarray,
    variances: np.ndarray,
    np: Any,
) -> np.ndarray:
    """Return log probability of each row under each state's diagonal Gaussian."""
    safe_variances = np.maximum(variances, 1e-12)
    diff = matrix[:, None, :] - means[None, :, :]
    return -0.5 * (
        np.log(2.0 * math.pi * safe_variances).sum(axis=1)[None, :]
        + ((diff**2) / safe_variances[None, :, :]).sum(axis=2)
    )


def _forward_log(
    log_emissions: np.ndarray,
    start_probability: np.ndarray,
    transition_matrix: np.ndarray,
    np: Any,
) -> tuple[np.ndarray, float]:
    """Run the HMM forward pass in log space."""
    log_start = np.log(start_probability)
    log_transition = np.log(transition_matrix)
    log_alpha = np.zeros_like(log_emissions)
    log_alpha[0] = log_start + log_emissions[0]
    for index in range(1, len(log_emissions)):
        log_alpha[index] = log_emissions[index] + _logsumexp(
            log_alpha[index - 1][:, None] + log_transition,
            axis=0,
            np=np,
        )
    return log_alpha, float(_logsumexp(log_alpha[-1], axis=None, np=np))


def _backward_log(
    log_emissions: np.ndarray,
    transition_matrix: np.ndarray,
    np: Any,
) -> np.ndarray:
    """Run the HMM backward pass in log space."""
    log_transition = np.log(transition_matrix)
    log_beta = np.zeros_like(log_emissions)
    for index in range(len(log_emissions) - 2, -1, -1):
        log_beta[index] = _logsumexp(
            log_transition + log_emissions[index + 1][None, :] + log_beta[index + 1][None, :],
            axis=1,
            np=np,
        )
    return log_beta


def _expected_transition_counts(
    log_alpha: np.ndarray,
    log_beta: np.ndarray,
    log_emissions: np.ndarray,
    transition_matrix: np.ndarray,
    log_likelihood: float,
    np: Any,
) -> np.ndarray:
    """Return expected transition counts from smoothed posteriors."""
    n_states = transition_matrix.shape[0]
    xi_sum = np.zeros((n_states, n_states))
    log_transition = np.log(transition_matrix)
    for index in range(len(log_emissions) - 1):
        log_xi = (
            log_alpha[index][:, None]
            + log_transition
            + log_emissions[index + 1][None, :]
            + log_beta[index + 1][None, :]
            - log_likelihood
        )
        xi_sum += np.exp(log_xi)
    return xi_sum


def _update_emissions(
    matrix: np.ndarray,
    gamma: np.ndarray,
    previous_means: np.ndarray,
    previous_variances: np.ndarray,
    config: HMMRegimeConfig,
    np: Any,
) -> tuple[np.ndarray, np.ndarray]:
    """Update state means and variances from posterior state weights."""
    weights = gamma.sum(axis=0)
    means = previous_means.copy()
    variances = previous_variances.copy()
    for state in range(config.n_states):
        if weights[state] <= config.covariance_floor:
            continue
        state_weight = gamma[:, state][:, None]
        means[state] = (state_weight * matrix).sum(axis=0) / weights[state]
        centered = matrix - means[state]
        variances[state] = np.maximum(
            (state_weight * centered * centered).sum(axis=0) / weights[state],
            config.covariance_floor,
        )
    return means, variances


def _posterior_probabilities(
    normalized_matrix: np.ndarray,
    model: HMMRegimeModel,
    np: Any,
) -> np.ndarray:
    """Return smoothed state probabilities under a fitted HMM."""
    log_emissions = _log_gaussian_emissions(
        normalized_matrix,
        model.means,
        model.variances,
        np,
    )
    log_alpha, log_likelihood = _forward_log(
        log_emissions,
        model.start_probability,
        model.transition_matrix,
        np,
    )
    log_beta = _backward_log(log_emissions, model.transition_matrix, np)
    return np.exp(log_alpha + log_beta - log_likelihood)


def _probabilities_by_label(probabilities: np.ndarray, model: HMMRegimeModel) -> dict[str, float]:
    """Map raw state probabilities onto semantic regime labels."""
    return {
        label: float(probabilities[state_index])
        for state_index, label in enumerate(model.label_by_state)
    }


def _semantic_labels(means: np.ndarray, feature_columns: tuple[str, ...]) -> tuple[str, ...]:
    """Assign bear/sideways/bull labels by the state's mean one-day return."""
    return_column_index = feature_columns.index("spy_log_return_1d")
    order = means[:, return_column_index].argsort()
    labels = [""] * len(REGIME_LABELS)
    for label, state_index in zip(REGIME_LABELS, order, strict=True):
        labels[int(state_index)] = label
    return tuple(labels)


def _bounded_complete_rows(
    training_frame: pd.DataFrame,
    *,
    feature_columns: tuple[str, ...],
    train_start_date: str | None,
    train_end_date: str,
) -> pd.DataFrame:
    """Return complete training rows inside an explicit date window."""
    rows = training_frame.copy()
    if train_start_date is not None:
        rows = rows[rows["date"] >= train_start_date]
    rows = rows[rows["date"] < train_end_date]
    rows = rows[_complete_row_mask(rows, feature_columns)]
    return rows.sort_values("date").reset_index(drop=True)


def _bounded_rows(
    training_frame: pd.DataFrame,
    *,
    train_start_date: str | None,
    train_end_date: str,
) -> pd.DataFrame:
    """Return all bounded training rows before completeness filtering."""
    rows = training_frame.copy()
    if train_start_date is not None:
        rows = rows[rows["date"] >= train_start_date]
    rows = rows[rows["date"] < train_end_date]
    return rows.sort_values("date").reset_index(drop=True)


def _active_feature_columns(
    training_frame: pd.DataFrame,
    *,
    feature_columns: tuple[str, ...],
    train_start_date: str | None,
    train_end_date: str,
) -> tuple[str, ...]:
    """Return feature columns with at least one usable value inside the train window."""
    rows = training_frame.copy()
    if train_start_date is not None:
        rows = rows[rows["date"] >= train_start_date]
    rows = rows[rows["date"] < train_end_date]
    active: list[str] = []
    for column in feature_columns:
        if column in rows and _series_has_finite_value(rows[column]):
            active.append(column)
    return tuple(active)


def _complete_row_mask(frame: pd.DataFrame, feature_columns: tuple[str, ...]):
    """Return True for rows where every requested feature is finite."""
    pd = _require_pandas()
    numeric = frame.loc[:, list(feature_columns)].apply(pd.to_numeric, errors="coerce")
    return numeric.notna().all(axis=1)


def _series_has_finite_value(series) -> bool:
    """Return True when a feature column has at least one finite value."""
    pd = _require_pandas()
    numeric = pd.to_numeric(series, errors="coerce")
    return bool(numeric.notna().any())


def _inference_rows(
    training_frame: pd.DataFrame,
    *,
    model: HMMRegimeModel,
    inference_dates: list[str] | None,
) -> pd.DataFrame:
    """Return requested inference rows and enforce train-before-infer ordering."""
    rows = training_frame.copy()
    if inference_dates is None:
        rows = rows[rows["date"] > model.train_end_date]
    else:
        requested_dates = sorted(set(inference_dates))
        invalid = [date for date in requested_dates if date <= model.train_end_date]
        if invalid:
            raise ValueError(
                "inference_dates must be strictly after train_end_date; "
                f"invalid dates: {invalid}"
            )
        rows = rows[rows["date"].isin(requested_dates)]
    return rows.sort_values("date").reset_index(drop=True)


def _requested_inference_rows(
    training_frame: pd.DataFrame,
    *,
    train_end_date: str,
    inference_dates: list[str] | None,
) -> pd.DataFrame:
    """Return requested inference rows before a fitted HMM exists."""
    rows = training_frame.copy()
    if inference_dates is None:
        rows = rows[rows["date"] > train_end_date]
    else:
        requested_dates = sorted(set(inference_dates))
        invalid = [date for date in requested_dates if date <= train_end_date]
        if invalid:
            raise ValueError(
                "inference_dates must be strictly after train_end_date; "
                f"invalid dates: {invalid}"
            )
        rows = rows[rows["date"].isin(requested_dates)]
    return rows.sort_values("date").reset_index(drop=True)


def _validate_training_frame(
    training_frame: pd.DataFrame,
    feature_columns: tuple[str, ...],
) -> None:
    """Raise if the HMM training frame lacks required columns."""
    required = {"date", "is_complete", *feature_columns}
    missing = sorted(required - set(training_frame.columns))
    if missing:
        raise ValueError(f"HMM training frame missing required columns: {missing}")


def _validate_regime_feature_frame(features: pd.DataFrame) -> None:
    """Raise if a regime feature frame lacks required output columns."""
    missing = sorted(set(HMM_REGIME_COLUMNS) - set(features.columns))
    if missing:
        raise ValueError(f"HMM regime output missing required columns: {missing}")


def _standardize(matrix: np.ndarray, np: Any) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return standardized matrix plus center and scale vectors."""
    center = matrix.mean(axis=0)
    scale = matrix.std(axis=0)
    scale = np.where(scale <= 0.0, 1.0, scale)
    return (matrix - center) / scale, center, scale


def _normalize_vector(values: np.ndarray, np: Any) -> np.ndarray:
    """Return a probability vector."""
    total = values.sum()
    if total <= 0.0:
        return np.full_like(values, 1.0 / len(values), dtype=float)
    return values / total


def _normalize_rows(values: np.ndarray, np: Any) -> np.ndarray:
    """Return a row-stochastic matrix."""
    totals = values.sum(axis=1, keepdims=True)
    return np.divide(values, totals, out=np.full_like(values, 1.0 / values.shape[1]), where=totals > 0)


def _logsumexp(values: np.ndarray, *, axis: int | None, np: Any) -> np.ndarray | float:
    """Compute log(sum(exp(values))) stably."""
    max_value = np.max(values, axis=axis, keepdims=True)
    shifted = np.exp(values - max_value)
    result = max_value + np.log(np.sum(shifted, axis=axis, keepdims=True))
    if axis is None:
        return float(result.squeeze())
    return result.squeeze(axis=axis)


def _normalize_optional_float(value: object) -> float | None:
    """Return a finite float or None for optional regime outputs."""
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _normalize_optional_string(value: object) -> str | None:
    """Return a stripped string or None for optional regime labels."""
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _require_pandas() -> Any:
    """Import pandas lazily with a clear error when absent."""
    try:
        return importlib.import_module("pandas")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError("pandas is required for HMM regime detection.") from exc


def _require_numpy() -> Any:
    """Import numpy lazily with a clear error when absent."""
    try:
        return importlib.import_module("numpy")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError("numpy is required for HMM regime detection.") from exc

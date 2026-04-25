"""Layer 1.5 HMM regime fitting and feature emission."""
from __future__ import annotations

import importlib
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from core.features.regime_training import HMM_TRAINING_FEATURE_COLUMNS

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd

REGIME_LABELS: tuple[str, str, str] = ("bear", "sideways", "bull")
HMM_REGIME_FEATURE_COLUMNS: tuple[str, ...] = (
    "regime_label",
    "regime_confidence",
    "regime_prob_bear",
    "regime_prob_sideways",
    "regime_prob_bull",
)
HMM_REGIME_COLUMNS: tuple[str, ...] = ("date", *HMM_REGIME_FEATURE_COLUMNS)


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
    train_rows = _bounded_complete_rows(
        training_frame,
        train_start_date=train_start_date,
        train_end_date=train_end_date,
    )
    if len(train_rows) < active_config.min_training_rows:
        raise ValueError(
            "HMM training window has fewer complete rows than min_training_rows: "
            f"{len(train_rows)} < {active_config.min_training_rows}"
        )

    raw_matrix = train_rows.loc[:, list(feature_columns)].astype(float).to_numpy()
    normalized_matrix, center, scale = _standardize(raw_matrix, np)
    parameters = _fit_gaussian_hmm(normalized_matrix, active_config, np)
    labels = _semantic_labels(parameters["means"], feature_columns)
    return HMMRegimeModel(
        feature_columns=feature_columns,
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
    train_start_date: str | None,
    train_end_date: str,
) -> pd.DataFrame:
    """Return complete training rows inside an explicit date window."""
    rows = training_frame[training_frame["is_complete"].astype(bool)].copy()
    if train_start_date is not None:
        rows = rows[rows["date"] >= train_start_date]
    rows = rows[rows["date"] < train_end_date]
    return rows.sort_values("date").reset_index(drop=True)


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


def _validate_training_frame(
    training_frame: pd.DataFrame,
    feature_columns: tuple[str, ...],
) -> None:
    """Raise if the HMM training frame lacks required columns."""
    required = {"date", "is_complete", *feature_columns}
    missing = sorted(required - set(training_frame.columns))
    if missing:
        raise ValueError(f"HMM training frame missing required columns: {missing}")


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

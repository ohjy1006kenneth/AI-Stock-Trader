"""Offline FinBERT dataset construction and evaluation helpers."""
from __future__ import annotations

import math
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from core.contracts.schemas import FeatureRecord, NewsSentimentRecord
from core.features.sentiment_features import SentimentScore, SentimentScorer

FINBERT_LABEL_ORDER: tuple[str, ...] = ("negative", "neutral", "positive")
_LABEL_TO_ID = {label: index for index, label in enumerate(FINBERT_LABEL_ORDER)}


@dataclass(frozen=True)
class ReturnLabelingConfig:
    """Return-threshold configuration for return-derived sentiment labels."""

    neutral_band_return: float

    def __post_init__(self) -> None:
        """Require a non-negative neutral return band."""
        if not math.isfinite(self.neutral_band_return) or self.neutral_band_return < 0.0:
            raise ValueError("neutral_band_return must be a non-negative finite number")


@dataclass(frozen=True)
class DatasetBuildStats:
    """Dataset construction counts for one offline FinBERT run."""

    rows_built: int
    skipped_missing_text: int
    skipped_missing_label_record: int
    skipped_missing_forward_return: int
    label_counts: Mapping[str, int]
    ticker_count: int
    date_count: int

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable stats mapping."""
        return {
            "rows_built": self.rows_built,
            "skipped_missing_text": self.skipped_missing_text,
            "skipped_missing_label_record": self.skipped_missing_label_record,
            "skipped_missing_forward_return": self.skipped_missing_forward_return,
            "label_counts": dict(self.label_counts),
            "ticker_count": self.ticker_count,
            "date_count": self.date_count,
        }


@dataclass(frozen=True)
class DatasetBuildResult:
    """Offline dataset plus construction statistics."""

    dataset: pd.DataFrame
    stats: DatasetBuildStats


@dataclass(frozen=True)
class ChronologicalDatasetSplit:
    """Date-ordered train/eval split for offline training."""

    train: pd.DataFrame
    eval: pd.DataFrame


@dataclass(frozen=True)
class ClassificationMetrics:
    """Compact multi-class classification metrics."""

    accuracy: float
    macro_precision: float
    macro_recall: float
    macro_f1: float
    label_support: Mapping[str, int]
    confusion_matrix: Mapping[str, Mapping[str, int]]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable metrics mapping."""
        return {
            "accuracy": self.accuracy,
            "macro_precision": self.macro_precision,
            "macro_recall": self.macro_recall,
            "macro_f1": self.macro_f1,
            "label_support": dict(self.label_support),
            "confusion_matrix": {
                label: dict(predictions)
                for label, predictions in self.confusion_matrix.items()
            },
        }


def build_return_labeled_dataset(
    news_records: Sequence[NewsSentimentRecord],
    label_records: Sequence[FeatureRecord] | Mapping[tuple[str, str], FeatureRecord],
    *,
    labeling_config: ReturnLabelingConfig,
) -> DatasetBuildResult:
    """Join preprocessed news rows with forward-return labels.

    The join key is `(date, ticker)`, where `date` is the point-in-time-safe
    trading bucket already assigned during Layer 1 news preprocessing. Labels
    are derived only from `forward_return_1d`, which is computed from the
    canonical Layer 0 OHLCV archive and kept outside production inference.
    """
    label_map = _label_record_map(label_records)

    rows: list[dict[str, object]] = []
    skipped_missing_text = 0
    skipped_missing_label_record = 0
    skipped_missing_forward_return = 0
    label_counts: Counter[str] = Counter()

    for record in news_records:
        text = _text_for_training(record)
        if text is None:
            skipped_missing_text += 1
            continue

        label_record = label_map.get((record.date, record.ticker))
        if label_record is None:
            skipped_missing_label_record += 1
            continue

        forward_return = _forward_return_1d(label_record)
        return_label = label_from_forward_return(
            forward_return,
            labeling_config=labeling_config,
        )
        if return_label is None:
            skipped_missing_forward_return += 1
            continue

        label_id, label_name = return_label
        label_counts[label_name] += 1
        rows.append(
            {
                "date": record.date,
                "ticker": record.ticker,
                "article_id": record.article_id,
                "sentence_index": record.sentence_index,
                "headline": record.headline,
                "source": record.source,
                "published_at": _published_at_text(record.published_at),
                "text": text,
                "forward_return_1d": forward_return,
                "label_id": label_id,
                "label_name": label_name,
            }
        )

    dataset = pd.DataFrame(
        rows,
        columns=[
            "date",
            "ticker",
            "article_id",
            "sentence_index",
            "headline",
            "source",
            "published_at",
            "text",
            "forward_return_1d",
            "label_id",
            "label_name",
        ],
    )
    if len(dataset) > 0:
        dataset = dataset.sort_values(
            ["date", "ticker", "article_id", "sentence_index"],
            kind="stable",
            na_position="last",
        ).reset_index(drop=True)

    stats = DatasetBuildStats(
        rows_built=len(dataset),
        skipped_missing_text=skipped_missing_text,
        skipped_missing_label_record=skipped_missing_label_record,
        skipped_missing_forward_return=skipped_missing_forward_return,
        label_counts={label: int(label_counts.get(label, 0)) for label in FINBERT_LABEL_ORDER},
        ticker_count=int(dataset["ticker"].nunique()) if len(dataset) > 0 else 0,
        date_count=int(dataset["date"].nunique()) if len(dataset) > 0 else 0,
    )
    return DatasetBuildResult(dataset=dataset, stats=stats)


def split_dataset_chronologically(
    dataset: pd.DataFrame,
    *,
    eval_fraction: float,
) -> ChronologicalDatasetSplit:
    """Split a dataset by date so evaluation always occurs after training dates."""
    if not math.isfinite(eval_fraction) or eval_fraction < 0.0 or eval_fraction >= 1.0:
        raise ValueError("eval_fraction must be in the range [0.0, 1.0)")
    if len(dataset) == 0:
        return ChronologicalDatasetSplit(train=dataset.copy(), eval=dataset.copy())

    unique_dates = sorted(str(value) for value in dataset["date"].dropna().unique().tolist())
    if not unique_dates:
        return ChronologicalDatasetSplit(train=dataset.copy(), eval=dataset.copy())

    if eval_fraction == 0.0:
        return ChronologicalDatasetSplit(train=dataset.copy(), eval=dataset.iloc[0:0].copy())

    eval_days = max(1, int(len(unique_dates) * eval_fraction))
    if len(unique_dates) > 1:
        eval_days = min(eval_days, len(unique_dates) - 1)

    eval_dates = set(unique_dates[-eval_days:])
    eval_dataset = dataset[dataset["date"].isin(eval_dates)].reset_index(drop=True)
    train_dataset = dataset[~dataset["date"].isin(eval_dates)].reset_index(drop=True)
    return ChronologicalDatasetSplit(train=train_dataset, eval=eval_dataset)


def evaluate_return_labeled_dataset(
    dataset: pd.DataFrame,
    *,
    scorer: SentimentScorer,
    batch_size: int,
) -> ClassificationMetrics:
    """Score a return-labeled dataset and compute classification metrics."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if len(dataset) == 0:
        raise ValueError("dataset must contain at least one row")

    texts = dataset["text"].astype(str).tolist()
    true_ids = [int(value) for value in dataset["label_id"].tolist()]
    predicted_ids: list[int] = []

    for start in range(0, len(texts), batch_size):
        batch = texts[start : start + batch_size]
        scores = list(scorer.score(batch))
        if len(scores) != len(batch):
            raise ValueError("sentiment scorer returned the wrong number of scores")
        predicted_ids.extend(predicted_label_id(score) for score in scores)

    return classification_metrics(true_ids, predicted_ids)


def label_from_forward_return(
    forward_return_1d: float | None,
    *,
    labeling_config: ReturnLabelingConfig,
) -> tuple[int, str] | None:
    """Map one forward return into the FinBERT three-class label space."""
    if forward_return_1d is None or not math.isfinite(forward_return_1d):
        return None
    band = labeling_config.neutral_band_return
    if forward_return_1d > band:
        return _LABEL_TO_ID["positive"], "positive"
    if forward_return_1d < (-1.0 * band):
        return _LABEL_TO_ID["negative"], "negative"
    return _LABEL_TO_ID["neutral"], "neutral"


def predicted_label_id(score: SentimentScore) -> int:
    """Convert FinBERT probabilities into the label id of the winning class."""
    probabilities = {
        "negative": score.negative,
        "neutral": score.neutral,
        "positive": score.positive,
    }
    return _LABEL_TO_ID[max(probabilities, key=probabilities.__getitem__)]


def classification_metrics(
    true_ids: Sequence[int],
    predicted_ids: Sequence[int],
) -> ClassificationMetrics:
    """Compute accuracy, macro metrics, and a labeled confusion matrix."""
    if len(true_ids) != len(predicted_ids):
        raise ValueError("true_ids and predicted_ids must have the same length")
    if not true_ids:
        raise ValueError("classification metrics require at least one sample")

    confusion: dict[str, dict[str, int]] = {
        label: {predicted_label: 0 for predicted_label in FINBERT_LABEL_ORDER}
        for label in FINBERT_LABEL_ORDER
    }
    support: Counter[str] = Counter()
    correct = 0

    for true_id, predicted_id in zip(true_ids, predicted_ids, strict=True):
        true_label = _label_name_from_id(true_id)
        predicted_label = _label_name_from_id(predicted_id)
        confusion[true_label][predicted_label] += 1
        support[true_label] += 1
        if true_id == predicted_id:
            correct += 1

    precisions: list[float] = []
    recalls: list[float] = []
    f1_scores: list[float] = []
    for label in FINBERT_LABEL_ORDER:
        tp = confusion[label][label]
        fp = sum(confusion[other][label] for other in FINBERT_LABEL_ORDER if other != label)
        fn = sum(confusion[label][other] for other in FINBERT_LABEL_ORDER if other != label)
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
        precisions.append(precision)
        recalls.append(recall)
        f1_scores.append(f1)

    total = len(true_ids)
    return ClassificationMetrics(
        accuracy=correct / total,
        macro_precision=sum(precisions) / len(precisions),
        macro_recall=sum(recalls) / len(recalls),
        macro_f1=sum(f1_scores) / len(f1_scores),
        label_support={label: int(support.get(label, 0)) for label in FINBERT_LABEL_ORDER},
        confusion_matrix=confusion,
    )


def _label_record_map(
    label_records: Sequence[FeatureRecord] | Mapping[tuple[str, str], FeatureRecord],
) -> Mapping[tuple[str, str], FeatureRecord]:
    """Normalize label inputs into a keyed lookup."""
    if isinstance(label_records, Mapping):
        return label_records
    return {(record.date, record.ticker): record for record in label_records}


def _forward_return_1d(record: FeatureRecord) -> float | None:
    """Return the normalized forward 1-day return from one label record."""
    value = record.features.get("forward_return_1d")
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _label_name_from_id(label_id: int) -> str:
    """Return the string label for one numeric class id."""
    if label_id < 0 or label_id >= len(FINBERT_LABEL_ORDER):
        raise ValueError(f"Unknown label id: {label_id}")
    return FINBERT_LABEL_ORDER[label_id]


def _published_at_text(value: Any) -> str | None:
    """Return a serialized published-at timestamp for diagnostic outputs."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _text_for_training(record: NewsSentimentRecord) -> str | None:
    """Return the sentence/headline text used for offline evaluation or tuning."""
    for candidate in (record.text, record.headline):
        if candidate is None:
            continue
        text = str(candidate).strip()
        if text:
            return text
    return None

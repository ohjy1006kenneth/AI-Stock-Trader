from __future__ import annotations

import pandas as pd
import pytest

from app.lab.training.finbert_finetuning import (
    ReturnLabelingConfig,
    build_return_labeled_dataset,
    classification_metrics,
    evaluate_return_labeled_dataset,
    split_dataset_chronologically,
)
from core.contracts.schemas import FeatureRecord, NewsSentimentRecord
from core.features.sentiment_features import SentimentScore


class _FakeScorer:
    """Deterministic scorer used for offline-evaluation tests."""

    def __init__(self, labels: list[str]) -> None:
        """Record one predicted label per dataset row."""
        self._labels = labels
        self._index = 0

    def score(self, texts: list[str]) -> list[SentimentScore]:
        """Return one probability triple per supplied text."""
        batch = self._labels[self._index : self._index + len(texts)]
        self._index += len(texts)
        scores: list[SentimentScore] = []
        for label in batch:
            if label == "positive":
                scores.append(SentimentScore(positive=0.8, negative=0.1, neutral=0.1))
            elif label == "negative":
                scores.append(SentimentScore(positive=0.1, negative=0.8, neutral=0.1))
            else:
                scores.append(SentimentScore(positive=0.1, negative=0.1, neutral=0.8))
        return scores


def test_build_return_labeled_dataset_joins_news_to_forward_returns() -> None:
    """Preprocessed news rows inherit labels from matching `(date, ticker)` returns."""
    news_records = [
        NewsSentimentRecord(
            date="2024-04-10",
            ticker="AAPL",
            text="Apple beats estimates.",
            article_id="a1",
            sentence_index=0,
            source="Reuters",
        ),
        NewsSentimentRecord(
            date="2024-04-10",
            ticker="MSFT",
            text="Microsoft misses estimates.",
            article_id="m1",
            sentence_index=0,
            source="Reuters",
        ),
        NewsSentimentRecord(
            date="2024-04-11",
            ticker="AAPL",
            headline="Apple guidance unchanged.",
            article_id="a2",
            sentence_index=0,
            source="Reuters",
        ),
        NewsSentimentRecord(date="2024-04-11", ticker="GOOG"),
        NewsSentimentRecord(
            date="2024-04-11",
            ticker="NVDA",
            text="Unlabeled ticker should be skipped.",
            article_id="n1",
            sentence_index=0,
            source="Reuters",
        ),
    ]
    label_records = [
        FeatureRecord(
            date="2024-04-10",
            ticker="AAPL",
            features={"forward_return_1d": 0.02},
        ),
        FeatureRecord(
            date="2024-04-10",
            ticker="MSFT",
            features={"forward_return_1d": -0.03},
        ),
        FeatureRecord(
            date="2024-04-11",
            ticker="AAPL",
            features={"forward_return_1d": 0.005},
        ),
    ]

    result = build_return_labeled_dataset(
        news_records,
        label_records,
        labeling_config=ReturnLabelingConfig(neutral_band_return=0.01),
    )

    assert result.stats.rows_built == 3
    assert result.stats.skipped_missing_text == 1
    assert result.stats.skipped_missing_label_record == 1
    assert result.stats.label_counts == {"negative": 1, "neutral": 1, "positive": 1}
    assert result.dataset[["date", "ticker", "label_name"]].to_dict(orient="records") == [
        {"date": "2024-04-10", "ticker": "AAPL", "label_name": "positive"},
        {"date": "2024-04-10", "ticker": "MSFT", "label_name": "negative"},
        {"date": "2024-04-11", "ticker": "AAPL", "label_name": "neutral"},
    ]


def test_split_dataset_chronologically_holds_out_latest_dates() -> None:
    """Evaluation rows come only from the most recent unique dates."""
    dataset = pd.DataFrame(
        [
            {"date": "2024-04-10", "text": "a", "label_id": 0},
            {"date": "2024-04-11", "text": "b", "label_id": 1},
            {"date": "2024-04-12", "text": "c", "label_id": 2},
        ]
    )

    split = split_dataset_chronologically(dataset, eval_fraction=0.34)

    assert split.train["date"].tolist() == ["2024-04-10", "2024-04-11"]
    assert split.eval["date"].tolist() == ["2024-04-12"]


def test_evaluate_return_labeled_dataset_computes_multiclass_metrics() -> None:
    """Offline evaluation reports accuracy and macro metrics over three labels."""
    dataset = pd.DataFrame(
        [
            {"date": "2024-04-10", "text": "bullish", "label_id": 2},
            {"date": "2024-04-10", "text": "bearish", "label_id": 0},
            {"date": "2024-04-11", "text": "flat", "label_id": 1},
        ]
    )

    metrics = evaluate_return_labeled_dataset(
        dataset,
        scorer=_FakeScorer(["positive", "negative", "negative"]),
        batch_size=2,
    )

    assert metrics.accuracy == pytest.approx(2.0 / 3.0)
    assert metrics.label_support == {"negative": 1, "neutral": 1, "positive": 1}
    assert metrics.confusion_matrix["neutral"]["negative"] == 1
    assert metrics.macro_f1 < 1.0


def test_classification_metrics_rejects_length_mismatch() -> None:
    """Metric computation fails closed on malformed prediction arrays."""
    with pytest.raises(ValueError, match="same length"):
        classification_metrics([0], [0, 1])

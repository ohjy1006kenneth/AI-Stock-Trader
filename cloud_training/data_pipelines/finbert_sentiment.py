from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
from typing import Any, Protocol


FINBERT_MODEL_NAME = "ProsusAI/finbert"
DEFAULT_RECENCY_HALFLIFE_HOURS = 12.0


class SentimentScorer(Protocol):
    def score(self, text: str) -> dict[str, float]: ...


@dataclass
class ArticleSentiment:
    timestamp: datetime
    headline: str
    summary: str
    source: str
    positive_prob: float
    negative_prob: float
    neutral_prob: float
    sentiment_label: str
    sentiment_score: float
    age_hours: float
    recency_weight: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "headline": self.headline,
            "summary": self.summary,
            "source": self.source,
            "positive_prob": self.positive_prob,
            "negative_prob": self.negative_prob,
            "neutral_prob": self.neutral_prob,
            "sentiment_label": self.sentiment_label,
            "sentiment_score": self.sentiment_score,
            "age_hours": self.age_hours,
            "recency_weight": self.recency_weight,
        }


class FinBERTSentimentScorer:
    def __init__(self, model_name: str = FINBERT_MODEL_NAME):
        try:
            from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline
        except Exception as exc:  # pragma: no cover - dependency-driven
            raise RuntimeError(
                "finbert_dependencies_missing: install transformers and torch to enable FinBERT scoring"
            ) from exc

        self._pipeline = pipeline(
            "text-classification",
            model=AutoModelForSequenceClassification.from_pretrained(model_name),
            tokenizer=AutoTokenizer.from_pretrained(model_name),
            return_all_scores=True,
        )
        self._cache: dict[str, dict[str, float]] = {}

    def score(self, text: str) -> dict[str, float]:
        normalized_text = text[:4096]
        cached = self._cache.get(normalized_text)
        if cached is not None:
            return dict(cached)
        result = self._pipeline(normalized_text)[0]
        probs = {str(item["label"]).lower(): float(item["score"]) for item in result}
        positive = probs.get("positive", 0.0)
        negative = probs.get("negative", 0.0)
        neutral = probs.get("neutral", 0.0)
        total = positive + negative + neutral
        if total <= 0:
            positive = negative = neutral = 1.0 / 3.0
            total = 1.0
        scored = {
            "positive": positive / total,
            "negative": negative / total,
            "neutral": neutral / total,
        }
        self._cache[normalized_text] = scored
        return dict(scored)


class KeywordMockSentimentScorer:
    """Deterministic lightweight scorer for tests/dev when FinBERT weights are unavailable."""

    POSITIVE_TERMS = {"beat", "beats", "surge", "gain", "gains", "bullish", "upgrade", "profit"}
    NEGATIVE_TERMS = {"miss", "misses", "drop", "drops", "loss", "losses", "bearish", "downgrade", "fraud"}

    def score(self, text: str) -> dict[str, float]:
        tokens = {token.strip(".,:;!?()[]{}\"'").lower() for token in text.split()}
        pos_hits = len(tokens & self.POSITIVE_TERMS)
        neg_hits = len(tokens & self.NEGATIVE_TERMS)
        if pos_hits == neg_hits == 0:
            return {"positive": 0.2, "negative": 0.2, "neutral": 0.6}
        raw_positive = 0.2 + (0.2 * pos_hits)
        raw_negative = 0.2 + (0.2 * neg_hits)
        raw_neutral = max(0.05, 1.0 - raw_positive - raw_negative)
        total = raw_positive + raw_negative + raw_neutral
        return {
            "positive": raw_positive / total,
            "negative": raw_negative / total,
            "neutral": raw_neutral / total,
        }


def _parse_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _article_text(item: dict[str, Any]) -> str:
    headline = str(item.get("headline") or "").strip()
    summary = str(item.get("summary") or "").strip()
    combined = ". ".join(part for part in [headline, summary] if part)
    return combined.strip()


def _end_of_day_utc(date_str: str) -> datetime:
    return datetime.fromisoformat(f"{date_str}T23:59:59+00:00")


def _exp_recency_weight(age_hours: float, halflife_hours: float) -> float:
    if halflife_hours <= 0:
        return 1.0
    return math.exp(-math.log(2.0) * max(age_hours, 0.0) / halflife_hours)


def _score_text_cached(scorer: SentimentScorer, text: str) -> dict[str, float]:
    normalized_text = str(text or "").strip()[:4096]
    if not normalized_text:
        return {"positive": 0.0, "negative": 0.0, "neutral": 1.0}
    cache = getattr(scorer, "_openclaw_sentiment_cache", None)
    if cache is None:
        cache = {}
        setattr(scorer, "_openclaw_sentiment_cache", cache)
    cached = cache.get(normalized_text)
    if cached is not None:
        return dict(cached)
    scored = scorer.score(normalized_text)
    normalized = {
        "positive": float(scored.get("positive", 0.0)),
        "negative": float(scored.get("negative", 0.0)),
        "neutral": float(scored.get("neutral", 0.0)),
    }
    cache[normalized_text] = normalized
    return dict(normalized)


def score_news_articles(
    items: list[dict[str, Any]],
    *,
    scorer: SentimentScorer,
    as_of_date: str,
    recency_halflife_hours: float = DEFAULT_RECENCY_HALFLIFE_HOURS,
) -> list[ArticleSentiment]:
    day_end = _end_of_day_utc(as_of_date)
    scored: list[ArticleSentiment] = []
    for item in items:
        timestamp_raw = str(item.get("timestamp") or item.get("date") or "").strip()
        if not timestamp_raw:
            continue
        timestamp = _parse_timestamp(timestamp_raw)
        text = _article_text(item)
        if not text:
            continue
        probs = _score_text_cached(scorer, text)
        positive = float(probs.get("positive", 0.0))
        negative = float(probs.get("negative", 0.0))
        neutral = float(probs.get("neutral", 0.0))
        total = positive + negative + neutral
        if total <= 0:
            positive = negative = neutral = 1.0 / 3.0
            total = 1.0
        positive /= total
        negative /= total
        neutral /= total
        sentiment_score = positive - negative
        age_hours = max(0.0, (day_end - timestamp).total_seconds() / 3600.0)
        recency_weight = _exp_recency_weight(age_hours, recency_halflife_hours)
        label = max(
            [("positive", positive), ("negative", negative), ("neutral", neutral)],
            key=lambda pair: pair[1],
        )[0]
        scored.append(
            ArticleSentiment(
                timestamp=timestamp,
                headline=str(item.get("headline") or ""),
                summary=str(item.get("summary") or ""),
                source=str(item.get("source") or "unknown"),
                positive_prob=positive,
                negative_prob=negative,
                neutral_prob=neutral,
                sentiment_label=label,
                sentiment_score=sentiment_score,
                age_hours=age_hours,
                recency_weight=recency_weight,
            )
        )
    scored.sort(key=lambda article: article.timestamp)
    return scored


def aggregate_ticker_day_sentiment_features(
    items: list[dict[str, Any]],
    *,
    scorer: SentimentScorer,
    as_of_date: str,
    recency_halflife_hours: float = DEFAULT_RECENCY_HALFLIFE_HOURS,
) -> dict[str, Any]:
    scored = score_news_articles(
        items,
        scorer=scorer,
        as_of_date=as_of_date,
        recency_halflife_hours=recency_halflife_hours,
    )
    if not scored:
        return {
            "news_count": 0,
            "news_volume": 0.0,
            "finbert_positive_prob_mean": 0.0,
            "finbert_negative_prob_mean": 0.0,
            "finbert_neutral_prob_mean": 0.0,
            "finbert_sentiment_score_mean": 0.0,
            "finbert_positive_prob_recency_weighted": 0.0,
            "finbert_negative_prob_recency_weighted": 0.0,
            "finbert_neutral_prob_recency_weighted": 0.0,
            "finbert_sentiment_score_recency_weighted": 0.0,
            "finbert_article_age_hours_min": None,
            "finbert_article_age_hours_max": None,
            "finbert_article_age_hours_mean": None,
            "finbert_recency_weight_sum": 0.0,
            "article_sentiment": [],
        }

    total = float(len(scored))
    weight_sum = sum(article.recency_weight for article in scored)
    safe_weight_sum = weight_sum if weight_sum > 0 else total
    return {
        "news_count": int(total),
        "news_volume": float(total),
        "finbert_positive_prob_mean": sum(article.positive_prob for article in scored) / total,
        "finbert_negative_prob_mean": sum(article.negative_prob for article in scored) / total,
        "finbert_neutral_prob_mean": sum(article.neutral_prob for article in scored) / total,
        "finbert_sentiment_score_mean": sum(article.sentiment_score for article in scored) / total,
        "finbert_positive_prob_recency_weighted": sum(article.positive_prob * article.recency_weight for article in scored) / safe_weight_sum,
        "finbert_negative_prob_recency_weighted": sum(article.negative_prob * article.recency_weight for article in scored) / safe_weight_sum,
        "finbert_neutral_prob_recency_weighted": sum(article.neutral_prob * article.recency_weight for article in scored) / safe_weight_sum,
        "finbert_sentiment_score_recency_weighted": sum(article.sentiment_score * article.recency_weight for article in scored) / safe_weight_sum,
        "finbert_article_age_hours_min": min(article.age_hours for article in scored),
        "finbert_article_age_hours_max": max(article.age_hours for article in scored),
        "finbert_article_age_hours_mean": sum(article.age_hours for article in scored) / total,
        "finbert_recency_weight_sum": weight_sum,
        "article_sentiment": [article.to_dict() for article in scored],
    }

from __future__ import annotations

from core.features.context_features import (
    CONTEXT_FEATURE_COLUMNS,
    compute_context_features,
    context_features_to_records,
)
from core.features.fundamentals_features import (
    FUNDAMENTAL_FEATURE_COLUMNS,
    compute_fundamentals_features,
    fundamentals_features_to_records,
)
from core.features.io import (
    feature_record_to_parquet_bytes,
    parquet_bytes_to_feature_record,
    read_feature_record,
    write_feature_record,
)
from core.features.loaders import load_fundamentals_frame, load_macro_frame, load_ohlcv_frame
from core.features.macro_features import (
    MACRO_FEATURE_COLUMNS,
    compute_macro_features,
    macro_features_to_records,
)
from core.features.market_features import (
    MARKET_FEATURE_COLUMNS,
    compute_market_features,
    market_features_to_records,
)
from core.features.regime_detection import (
    HMM_REGIME_COLUMNS,
    HMM_REGIME_FEATURE_COLUMNS,
    HMMRegimeConfig,
    HMMRegimeModel,
    emit_hmm_regime_features,
    fit_and_emit_hmm_regime_features,
    fit_hmm_regime_model,
)
from core.features.regime_training import (
    HMM_TRAINING_COLUMNS,
    HMM_TRAINING_FEATURE_COLUMNS,
    build_hmm_training_frame,
    complete_hmm_training_matrix,
)
from core.features.sentiment_features import (
    DEFAULT_SOURCE_CREDIBILITY_CONFIG_PATH,
    SENTIMENT_AGGREGATE_COLUMNS,
    SourceCredibilityConfig,
    aggregate_sentiment_by_ticker_day,
    load_source_credibility_config,
    sentiment_aggregates_to_records,
)

__all__ = [
    "CONTEXT_FEATURE_COLUMNS",
    "DEFAULT_SOURCE_CREDIBILITY_CONFIG_PATH",
    "FUNDAMENTAL_FEATURE_COLUMNS",
    "HMM_REGIME_COLUMNS",
    "HMM_REGIME_FEATURE_COLUMNS",
    "HMM_TRAINING_COLUMNS",
    "HMM_TRAINING_FEATURE_COLUMNS",
    "HMMRegimeConfig",
    "HMMRegimeModel",
    "MACRO_FEATURE_COLUMNS",
    "MARKET_FEATURE_COLUMNS",
    "SENTIMENT_AGGREGATE_COLUMNS",
    "SourceCredibilityConfig",
    "aggregate_sentiment_by_ticker_day",
    "build_hmm_training_frame",
    "complete_hmm_training_matrix",
    "compute_context_features",
    "compute_fundamentals_features",
    "compute_macro_features",
    "compute_market_features",
    "context_features_to_records",
    "feature_record_to_parquet_bytes",
    "emit_hmm_regime_features",
    "fit_and_emit_hmm_regime_features",
    "fit_hmm_regime_model",
    "fundamentals_features_to_records",
    "load_fundamentals_frame",
    "load_macro_frame",
    "load_ohlcv_frame",
    "load_source_credibility_config",
    "macro_features_to_records",
    "market_features_to_records",
    "parquet_bytes_to_feature_record",
    "read_feature_record",
    "sentiment_aggregates_to_records",
    "write_feature_record",
]

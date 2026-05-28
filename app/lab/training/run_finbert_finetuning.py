"""Offline FinBERT evaluation and optional fine-tuning runner."""
from __future__ import annotations

import argparse
import importlib
import io
import json
import os
import sys
import types
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from datetime import date as Date
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from loguru import logger


def _resolve_repo_root() -> Path:
    """Return the repository root for local runs and Modal-mounted runs."""
    env_root = os.getenv("AI_STOCK_TRADER_REPO_ROOT")
    if env_root:
        return Path(env_root).resolve()
    resolved = Path(__file__).resolve()
    return resolved.parents[3] if len(resolved.parents) > 3 else resolved.parent


_REPO_ROOT = _resolve_repo_root()
sys.path.insert(0, str(_REPO_ROOT))

from app.lab.training.finbert_finetuning import (  # noqa: E402
    FINBERT_LABEL_ORDER,
    ClassificationMetrics,
    ReturnLabelingConfig,
    build_return_labeled_dataset,
    evaluate_return_labeled_dataset,
    split_dataset_chronologically,
)
from core.contracts.schemas import (  # noqa: E402
    SCHEMA_VERSION,
    ArtifactManifestRecord,
    FeatureRecord,
    NewsSentimentRecord,
    PipelineManifestRecord,
    RunStatus,
)
from core.features.news_preprocessing import news_sentiment_frame_to_records  # noqa: E402
from core.features.sentiment_features import SentimentScore, SentimentScorer  # noqa: E402
from core.labels import read_label_record  # noqa: E402
from services.modal.secrets import (  # noqa: E402
    SIMFIN_MODAL_ENV_FILE,
    SIMFIN_MODAL_ENV_KEYS,
    build_modal_secrets,
)
from services.r2.paths import layer1_news_preprocessing_path  # noqa: E402
from services.r2.writer import R2Writer  # noqa: E402

if TYPE_CHECKING:
    import pandas as pd

FINBERT_FINETUNING_STAGE = "lab_finbert_finetuning"
ARTIFACT_MANIFEST_STAGE = "lab_finbert_finetuning_artifact"
FINETUNE_CONFIG_PATH = _REPO_ROOT / "config" / "finbert_finetuning.json"
MODAL_REPO_ROOT = "/workspace/AI-Stock-Trader"


class ObjectStore(Protocol):
    """Object-store operations required by the offline runner."""

    def get_object(self, key: str) -> bytes:
        """Read an object from storage."""


@dataclass(frozen=True)
class FinBERTFineTuneConfig:
    """Configuration for one offline evaluation/fine-tuning run."""

    run_id: str
    from_date: str
    to_date: str
    news_run_id: str
    fine_tune: bool = False
    tickers: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate date bounds and optional ticker filters."""
        if not self.run_id.strip():
            raise ValueError("run_id cannot be empty")
        if not self.news_run_id.strip():
            raise ValueError("news_run_id cannot be empty")
        start = _parse_iso_date(self.from_date, field_name="from_date")
        end = _parse_iso_date(self.to_date, field_name="to_date")
        if start > end:
            raise ValueError("from_date must be on or before to_date")
        for ticker in self.tickers:
            if not ticker.strip():
                raise ValueError("tickers cannot contain empty strings")


@dataclass(frozen=True)
class FinBERTFineTuneRuntimeConfig:
    """Modal/runtime settings for offline FinBERT work."""

    app_name: str
    r2_secret_name: str
    timeout_seconds: int
    python_version: str
    requirements_path: str
    base_model_name: str
    base_model_revision: str
    train_batch_size: int
    eval_batch_size: int
    learning_rate: float
    weight_decay: float
    epochs: int
    max_length: int
    eval_fraction: float
    neutral_band_return: float
    gpu_type: str | None = None

    def __post_init__(self) -> None:
        """Validate offline tuning settings."""
        if not self.app_name.strip():
            raise ValueError("app_name cannot be empty")
        if not self.r2_secret_name.strip():
            raise ValueError("r2_secret_name cannot be empty")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not self.python_version.strip():
            raise ValueError("python_version cannot be empty")
        if not self.requirements_path.strip():
            raise ValueError("requirements_path cannot be empty")
        if not self.base_model_name.strip():
            raise ValueError("base_model_name cannot be empty")
        if not self.base_model_revision.strip():
            raise ValueError("base_model_revision cannot be empty")
        if self.train_batch_size <= 0:
            raise ValueError("train_batch_size must be positive")
        if self.eval_batch_size <= 0:
            raise ValueError("eval_batch_size must be positive")
        if self.learning_rate <= 0.0:
            raise ValueError("learning_rate must be positive")
        if self.weight_decay < 0.0:
            raise ValueError("weight_decay must be non-negative")
        if self.epochs <= 0:
            raise ValueError("epochs must be positive")
        if self.max_length <= 0:
            raise ValueError("max_length must be positive")
        if self.eval_fraction < 0.0 or self.eval_fraction >= 1.0:
            raise ValueError("eval_fraction must be in the range [0.0, 1.0)")
        if self.neutral_band_return < 0.0:
            raise ValueError("neutral_band_return must be non-negative")
        if self.gpu_type not in (None, "", "T4"):
            raise ValueError("gpu_type must be omitted or set to T4")


@dataclass(frozen=True)
class FineTuneTrainingArtifact:
    """Outputs produced by the optional fine-tuning stage."""

    model_version: str
    bundle_subdir: str
    eval_metrics: ClassificationMetrics
    training_loss: float | None
    epochs_completed: int

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable training summary."""
        return {
            "model_version": self.model_version,
            "bundle_subdir": self.bundle_subdir,
            "eval_metrics": self.eval_metrics.to_dict(),
            "training_loss": self.training_loss,
            "epochs_completed": self.epochs_completed,
        }


class FineTuneTrainer(Protocol):
    """Optional model-training backend used by the runner."""

    def train(
        self,
        *,
        run_id: str,
        train_dataset: pd.DataFrame,
        eval_dataset: pd.DataFrame,
        output_dir: Path,
        runtime_config: FinBERTFineTuneRuntimeConfig,
    ) -> FineTuneTrainingArtifact:
        """Fine-tune a model and return artifact metadata."""


@dataclass(frozen=True)
class FinBERTFineTuneResult:
    """Local artifact summary for one completed offline run."""

    run_id: str
    dataset_rows: int
    train_rows: int
    eval_rows: int
    baseline_metrics_path: str
    dataset_report_path: str
    pipeline_manifest_path: str
    artifact_manifest_path: str
    bundle_path: str
    fine_tuned: bool
    missing_news_dates: tuple[str, ...]


def run_finbert_finetuning(
    config: FinBERTFineTuneConfig,
    *,
    writer: ObjectStore | None = None,
    scorer: SentimentScorer | None = None,
    trainer: FineTuneTrainer | None = None,
    runtime_config: FinBERTFineTuneRuntimeConfig | None = None,
    artifact_root: Path | None = None,
) -> FinBERTFineTuneResult:
    """Run offline baseline evaluation and optional FinBERT fine-tuning."""
    active_writer = writer or R2Writer()
    runtime = runtime_config or load_finbert_finetuning_runtime_config()
    active_scorer = scorer or TransformersSentimentScorer(runtime)
    active_trainer = trainer or TransformersFineTuneTrainer(active_scorer)
    started_at = datetime.now(UTC)
    root = (artifact_root or _REPO_ROOT).resolve()
    paths = _offline_output_paths(run_id=config.run_id)

    metadata: dict[str, object] = {
        "from_date": config.from_date,
        "to_date": config.to_date,
        "news_run_id": config.news_run_id,
        "fine_tune": config.fine_tune,
        "tickers": list(config.tickers),
        "base_model_name": runtime.base_model_name,
        "base_model_revision": runtime.base_model_revision,
        "bundle_path": paths["bundle_path"],
        "metrics_path": paths["metrics_path"],
        "dataset_report_path": paths["dataset_report_path"],
        "artifact_manifest_path": paths["artifact_manifest_path"],
        "gpu_type": runtime.gpu_type,
    }

    try:
        news_records, missing_news_dates = _load_news_records(
            writer=active_writer,
            news_run_id=config.news_run_id,
            from_date=config.from_date,
            to_date=config.to_date,
            tickers=config.tickers,
        )
        label_records = _load_label_records(
            writer=active_writer,
            news_records=news_records,
        )
        dataset_result = build_return_labeled_dataset(
            news_records,
            label_records,
            labeling_config=ReturnLabelingConfig(
                neutral_band_return=runtime.neutral_band_return,
            ),
        )
        if dataset_result.stats.rows_built == 0:
            raise ValueError("No return-labeled FinBERT rows were built for the requested window.")

        split = split_dataset_chronologically(
            dataset_result.dataset,
            eval_fraction=runtime.eval_fraction,
        )
        baseline_eval_dataset = split.eval if len(split.eval) > 0 else dataset_result.dataset
        baseline_metrics = evaluate_return_labeled_dataset(
            baseline_eval_dataset,
            scorer=active_scorer,
            batch_size=runtime.eval_batch_size,
        )

        training_artifact: FineTuneTrainingArtifact | None = None
        if config.fine_tune:
            if len(split.train) == 0 or len(split.eval) == 0:
                raise ValueError(
                    "Fine-tuning requires non-empty train and eval splits across distinct dates."
                )
            training_artifact = active_trainer.train(
                run_id=config.run_id,
                train_dataset=split.train,
                eval_dataset=split.eval,
                output_dir=root / paths["bundle_path"] / "model",
                runtime_config=runtime,
            )

        bundle_dir = root / paths["bundle_path"]
        bundle_dir.mkdir(parents=True, exist_ok=True)
        _write_json(
            bundle_dir / "bundle_metadata.json",
            {
                "run_id": config.run_id,
                "fine_tuned": config.fine_tune,
                "approved": False,
                "base_model_name": runtime.base_model_name,
                "base_model_revision": runtime.base_model_revision,
                "news_run_id": config.news_run_id,
                "date_window": {
                    "from_date": config.from_date,
                    "to_date": config.to_date,
                },
                "dataset_stats": dataset_result.stats.to_dict(),
                "missing_news_dates": list(missing_news_dates),
                "baseline_metrics": baseline_metrics.to_dict(),
                "fine_tuned_metrics": (
                    training_artifact.eval_metrics.to_dict()
                    if training_artifact is not None
                    else None
                ),
            },
        )

        dataset_report_payload = {
            "run_id": config.run_id,
            "date_window": {"from_date": config.from_date, "to_date": config.to_date},
            "news_run_id": config.news_run_id,
            "tickers": list(config.tickers),
            "point_in_time_label_note": (
                "Labels are derived from Layer 0 OHLCV forward_return_1d shards joined to "
                "Layer 1 preprocessed news by trading-bucket date and ticker."
            ),
            "dataset_stats": dataset_result.stats.to_dict(),
            "missing_news_dates": list(missing_news_dates),
            "train_rows": int(len(split.train)),
            "eval_rows": int(len(split.eval)),
        }
        _write_json(root / paths["dataset_report_path"], dataset_report_payload)

        metrics_payload = {
            "run_id": config.run_id,
            "approved_for_production": False,
            "production_promotion_note": (
                "This offline artifact does not change production inference. "
                "Production FinBERT remains pinned in config/finbert_sentiment.json "
                "until a human approves the offline artifact and a separate change updates "
                "the serving configuration."
            ),
            "date_window": {"from_date": config.from_date, "to_date": config.to_date},
            "news_run_id": config.news_run_id,
            "tickers": list(config.tickers),
            "base_model": {
                "name": runtime.base_model_name,
                "revision": runtime.base_model_revision,
            },
            "baseline_metrics": baseline_metrics.to_dict(),
            "fine_tuned_metrics": (
                training_artifact.eval_metrics.to_dict()
                if training_artifact is not None
                else None
            ),
            "training_artifact": (
                training_artifact.to_dict() if training_artifact is not None else None
            ),
        }
        _write_json(root / paths["metrics_path"], metrics_payload)

        artifact_manifest = ArtifactManifestRecord(
            artifact_id=config.run_id,
            model_version=(
                training_artifact.model_version
                if training_artifact is not None
                else f"finbert-offline-eval-{config.run_id}"
            ),
            created_at=datetime.now(UTC),
            stage=ARTIFACT_MANIFEST_STAGE,
            metrics_path=paths["metrics_path"],
            diagnostics_path=paths["dataset_report_path"],
            bundle_path=paths["bundle_path"],
            schema_version=SCHEMA_VERSION,
            approved=False,
        )
        _write_text(
            root / paths["artifact_manifest_path"],
            artifact_manifest.model_dump_json(indent=2),
        )

        metadata.update(
            {
                "dataset_rows": dataset_result.stats.rows_built,
                "train_rows": int(len(split.train)),
                "eval_rows": int(len(split.eval)),
                "baseline_accuracy": baseline_metrics.accuracy,
                "fine_tuned_accuracy": (
                    training_artifact.eval_metrics.accuracy
                    if training_artifact is not None
                    else None
                ),
                "missing_news_dates": list(missing_news_dates),
            }
        )
        _write_pipeline_manifest(
            root=root,
            run_id=config.run_id,
            status=RunStatus.COMPLETED,
            started_at=started_at,
            metadata=metadata,
        )
        logger.info("Offline FinBERT run complete: {}", paths["artifact_manifest_path"])
        return FinBERTFineTuneResult(
            run_id=config.run_id,
            dataset_rows=dataset_result.stats.rows_built,
            train_rows=int(len(split.train)),
            eval_rows=int(len(split.eval)),
            baseline_metrics_path=paths["metrics_path"],
            dataset_report_path=paths["dataset_report_path"],
            pipeline_manifest_path=paths["pipeline_manifest_path"],
            artifact_manifest_path=paths["artifact_manifest_path"],
            bundle_path=paths["bundle_path"],
            fine_tuned=config.fine_tune,
            missing_news_dates=missing_news_dates,
        )
    except Exception as exc:
        metadata["error"] = {"type": type(exc).__name__, "message": str(exc)}
        _write_pipeline_manifest(
            root=root,
            run_id=config.run_id,
            status=RunStatus.FAILED,
            started_at=started_at,
            metadata=metadata,
        )
        logger.exception("Offline FinBERT run failed")
        raise


class TransformersSentimentScorer:
    """Transformers-backed FinBERT scorer used for offline evaluation."""

    def __init__(self, runtime_config: FinBERTFineTuneRuntimeConfig) -> None:
        """Load the baseline FinBERT pipeline lazily."""
        transformers = importlib.import_module("transformers")
        self._batch_size = runtime_config.eval_batch_size
        self._pipeline = transformers.pipeline(
            "text-classification",
            model=runtime_config.base_model_name,
            tokenizer=runtime_config.base_model_name,
            revision=runtime_config.base_model_revision,
            top_k=None,
            device=_transformers_device(runtime_config),
        )

    def score(self, texts: Sequence[str]) -> Sequence[SentimentScore]:
        """Return FinBERT class probabilities for each input text."""
        outputs = self._pipeline(
            list(texts),
            truncation=True,
            batch_size=self._batch_size,
        )
        if outputs and isinstance(outputs[0], dict):
            outputs = [outputs]
        return [_score_from_model_output(output) for output in outputs]


class TransformersFineTuneTrainer:
    """Optional PyTorch/Transformers training backend."""

    def __init__(self, scorer: SentimentScorer) -> None:
        """Retain the scorer for post-train evaluation when needed."""
        self._baseline_scorer = scorer

    def train(
        self,
        *,
        run_id: str,
        train_dataset: pd.DataFrame,
        eval_dataset: pd.DataFrame,
        output_dir: Path,
        runtime_config: FinBERTFineTuneRuntimeConfig,
    ) -> FineTuneTrainingArtifact:
        """Fine-tune a three-class classifier from the baseline FinBERT weights."""
        torch = importlib.import_module("torch")
        transformers = importlib.import_module("transformers")
        tokenizer = transformers.AutoTokenizer.from_pretrained(
            runtime_config.base_model_name,
            revision=runtime_config.base_model_revision,
        )
        model = transformers.AutoModelForSequenceClassification.from_pretrained(
            runtime_config.base_model_name,
            revision=runtime_config.base_model_revision,
            num_labels=len(FINBERT_LABEL_ORDER),
            id2label={index: label.upper() for index, label in enumerate(FINBERT_LABEL_ORDER)},
            label2id={label.upper(): index for index, label in enumerate(FINBERT_LABEL_ORDER)},
        )
        device = torch.device("cuda" if _use_cuda(runtime_config, torch) else "cpu")
        model.to(device)

        train_loader = _build_torch_dataloader(
            dataset=train_dataset,
            tokenizer=tokenizer,
            batch_size=runtime_config.train_batch_size,
            max_length=runtime_config.max_length,
            shuffle=True,
            torch_module=torch,
        )
        eval_loader = _build_torch_dataloader(
            dataset=eval_dataset,
            tokenizer=tokenizer,
            batch_size=runtime_config.eval_batch_size,
            max_length=runtime_config.max_length,
            shuffle=False,
            torch_module=torch,
        )

        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=runtime_config.learning_rate,
            weight_decay=runtime_config.weight_decay,
        )
        final_loss: float | None = None
        for _epoch in range(runtime_config.epochs):
            model.train()
            total_loss = 0.0
            steps = 0
            for batch in train_loader:
                batch_on_device = {
                    key: value.to(device) for key, value in batch.items()
                }
                optimizer.zero_grad()
                outputs = model(**batch_on_device)
                loss = outputs.loss
                loss.backward()
                optimizer.step()
                total_loss += float(loss.item())
                steps += 1
            final_loss = total_loss / steps if steps > 0 else None

        output_dir.mkdir(parents=True, exist_ok=True)
        model.save_pretrained(output_dir)
        tokenizer.save_pretrained(output_dir)

        predicted_ids, true_ids = _predict_from_loader(
            model=model,
            eval_loader=eval_loader,
            torch_module=torch,
            device=device,
        )
        metrics = _classification_metrics_from_ids(
            true_ids=true_ids,
            predicted_ids=predicted_ids,
        )
        return FineTuneTrainingArtifact(
            model_version=f"finbert-finetuned-{run_id}",
            bundle_subdir=output_dir.name,
            eval_metrics=metrics,
            training_loss=final_loss,
            epochs_completed=runtime_config.epochs,
        )


def load_finbert_finetuning_runtime_config(
    path: Path = FINETUNE_CONFIG_PATH,
) -> FinBERTFineTuneRuntimeConfig:
    """Load offline FinBERT runtime settings from repository config."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    gpu_type = payload.get("gpu_type")
    normalized_gpu = None if gpu_type in (None, "") else str(gpu_type)
    return FinBERTFineTuneRuntimeConfig(
        app_name=str(payload["app_name"]),
        r2_secret_name=str(payload["r2_secret_name"]),
        timeout_seconds=int(payload["timeout_seconds"]),
        python_version=str(payload.get("python_version", "3.11")),
        requirements_path=str(payload.get("requirements_path", "requirements/modal.txt")),
        base_model_name=str(payload["base_model_name"]),
        base_model_revision=str(payload["base_model_revision"]),
        train_batch_size=int(payload["train_batch_size"]),
        eval_batch_size=int(payload["eval_batch_size"]),
        learning_rate=float(payload["learning_rate"]),
        weight_decay=float(payload.get("weight_decay", 0.0)),
        epochs=int(payload["epochs"]),
        max_length=int(payload.get("max_length", 256)),
        eval_fraction=float(payload.get("eval_fraction", 0.2)),
        neutral_band_return=float(payload.get("neutral_band_return", 0.0)),
        gpu_type=normalized_gpu,
    )


def _load_news_records(
    *,
    writer: ObjectStore,
    news_run_id: str,
    from_date: str,
    to_date: str,
    tickers: tuple[str, ...],
) -> tuple[list[NewsSentimentRecord], tuple[str, ...]]:
    """Load preprocessed news rows for a historical date range."""
    pd = _require_pandas()
    all_records: list[NewsSentimentRecord] = []
    missing_dates: list[str] = []
    ticker_filter = {ticker.upper() for ticker in tickers}

    for date_text in _date_range(from_date, to_date):
        key = layer1_news_preprocessing_path(date_text, news_run_id)
        try:
            frame = pd.read_parquet(io.BytesIO(writer.get_object(key)))
        except FileNotFoundError:
            missing_dates.append(date_text)
            continue
        records = news_sentiment_frame_to_records(frame)
        if ticker_filter:
            records = [record for record in records if record.ticker.upper() in ticker_filter]
        all_records.extend(records)
    return all_records, tuple(missing_dates)


def _load_label_records(
    *,
    writer: ObjectStore,
    news_records: Sequence[NewsSentimentRecord],
) -> dict[tuple[str, str], FeatureRecord]:
    """Load one label shard per `(date, ticker)` present in the news sample."""
    records: dict[tuple[str, str], FeatureRecord] = {}
    keys = sorted({(record.date, record.ticker) for record in news_records})
    for date_text, ticker in keys:
        try:
            records[(date_text, ticker)] = read_label_record(date_text, ticker, writer=writer)
        except FileNotFoundError:
            continue
    return records


def _offline_output_paths(*, run_id: str) -> dict[str, str]:
    """Return repo-relative output paths for one offline run."""
    _validate_run_id(run_id)
    bundle_path = Path("artifacts") / "bundles" / "finbert_finetuning" / run_id
    return {
        "bundle_path": bundle_path.as_posix(),
        "metrics_path": (
            Path("artifacts")
            / "reports"
            / "diagnostics"
            / f"finbert_finetuning_{run_id}.json"
        ).as_posix(),
        "dataset_report_path": (
            Path("artifacts")
            / "reports"
            / "diagnostics"
            / f"finbert_finetuning_{run_id}_dataset.json"
        ).as_posix(),
        "pipeline_manifest_path": (
            Path("artifacts")
            / "manifests"
            / FINBERT_FINETUNING_STAGE
            / f"{run_id}.json"
        ).as_posix(),
        "artifact_manifest_path": (
            Path("artifacts")
            / "manifests"
            / ARTIFACT_MANIFEST_STAGE
            / f"{run_id}.json"
        ).as_posix(),
    }


def _write_pipeline_manifest(
    *,
    root: Path,
    run_id: str,
    status: RunStatus,
    started_at: datetime,
    metadata: dict[str, object],
) -> None:
    """Persist a local pipeline manifest for the offline run."""
    output_paths = _offline_output_paths(run_id=run_id)
    manifest_path = root / output_paths["pipeline_manifest_path"]
    manifest = PipelineManifestRecord(
        run_id=run_id,
        stage=FINBERT_FINETUNING_STAGE,
        status=status,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        metadata=metadata,
        output_path=output_paths["bundle_path"],
    )
    _write_text(manifest_path, manifest.model_dump_json(indent=2))


def _write_text(path: Path, payload: str) -> None:
    """Write a UTF-8 text file, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_json(path: Path, payload: dict[str, object]) -> None:
    """Write pretty JSON to disk."""
    _write_text(path, json.dumps(payload, indent=2, sort_keys=True))


def _score_from_model_output(output: object) -> SentimentScore:
    """Normalize one transformers pipeline output into SentimentScore."""
    if not isinstance(output, Sequence):
        raise ValueError("FinBERT output must be a sequence of label scores")

    scores: dict[str, float] = {}
    for item in output:
        if not isinstance(item, dict):
            raise ValueError("FinBERT label scores must be mappings")
        label = str(item.get("label", "")).strip().lower()
        score = float(item.get("score", 0.0))
        scores[label] = score

    missing = sorted(set(FINBERT_LABEL_ORDER) - set(scores))
    if missing:
        raise ValueError(f"FinBERT output missing labels: {missing}")
    return SentimentScore(
        positive=scores["positive"],
        negative=scores["negative"],
        neutral=scores["neutral"],
    )


def _classification_metrics_from_ids(
    *,
    true_ids: Sequence[int],
    predicted_ids: Sequence[int],
) -> ClassificationMetrics:
    """Delegate to the shared metrics helper without circular imports."""
    from app.lab.training.finbert_finetuning import classification_metrics

    return classification_metrics(true_ids, predicted_ids)


def _build_torch_dataloader(
    *,
    dataset: pd.DataFrame,
    tokenizer: object,
    batch_size: int,
    max_length: int,
    shuffle: bool,
    torch_module: object,
) -> object:
    """Tokenize one dataset and return a PyTorch DataLoader."""
    encoded = tokenizer(
        dataset["text"].astype(str).tolist(),
        truncation=True,
        padding=True,
        max_length=max_length,
        return_tensors="pt",
    )
    labels = torch_module.tensor(dataset["label_id"].astype(int).tolist(), dtype=torch_module.long)

    class _EncodedDataset(torch_module.utils.data.Dataset):
        def __len__(self) -> int:
            return len(labels)

        def __getitem__(self, index: int) -> dict[str, object]:
            item = {key: value[index] for key, value in encoded.items()}
            item["labels"] = labels[index]
            return item

    return torch_module.utils.data.DataLoader(_EncodedDataset(), batch_size=batch_size, shuffle=shuffle)


def _predict_from_loader(
    *,
    model: object,
    eval_loader: object,
    torch_module: object,
    device: object,
) -> tuple[list[int], list[int]]:
    """Run model inference over one encoded evaluation loader."""
    model.eval()
    predicted_ids: list[int] = []
    true_ids: list[int] = []
    with torch_module.no_grad():
        for batch in eval_loader:
            labels = batch["labels"].tolist()
            inputs = {
                key: value.to(device)
                for key, value in batch.items()
                if key != "labels"
            }
            outputs = model(**inputs)
            logits = outputs.logits
            predictions = torch_module.argmax(logits, dim=-1).tolist()
            predicted_ids.extend(int(value) for value in predictions)
            true_ids.extend(int(value) for value in labels)
    return predicted_ids, true_ids


def _use_cuda(runtime_config: FinBERTFineTuneRuntimeConfig, torch_module: object) -> bool:
    """Return True when the run should use the configured T4 GPU."""
    return bool(runtime_config.gpu_type == "T4" and torch_module.cuda.is_available())


def _transformers_device(runtime_config: FinBERTFineTuneRuntimeConfig) -> int:
    """Return the transformers pipeline device id for the current runtime."""
    try:
        torch = importlib.import_module("torch")
    except ModuleNotFoundError:
        return -1
    return 0 if _use_cuda(runtime_config, torch) else -1


def _date_range(from_date: str, to_date: str) -> list[str]:
    """Return every ISO date in the inclusive window."""
    start = _parse_iso_date(from_date, field_name="from_date")
    end = _parse_iso_date(to_date, field_name="to_date")
    values: list[str] = []
    current = start
    while current <= end:
        values.append(current.isoformat())
        current += timedelta(days=1)
    return values


def _validate_run_id(run_id: str) -> None:
    """Reject run ids that would escape the local artifact tree."""
    stripped = run_id.strip()
    if not stripped:
        raise ValueError("run_id cannot be empty")
    if any(token in stripped for token in ("/", "\\", "..")):
        raise ValueError("run_id must be a safe path component")


def _parse_iso_date(value: str, *, field_name: str) -> Date:
    """Parse a required YYYY-MM-DD string."""
    try:
        return Date.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD") from exc


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the offline FinBERT runner."""
    parser = argparse.ArgumentParser(
        description="Run offline FinBERT evaluation and optional fine-tuning."
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--from-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--news-run-id", required=True)
    parser.add_argument(
        "--tickers",
        default="",
        help="Optional comma-separated tickers, or @path/to/tickers.json.",
    )
    parser.add_argument(
        "--fine-tune",
        action="store_true",
        help="Fine-tune FinBERT offline after baseline evaluation.",
    )
    return parser.parse_args(argv)


def _resolve_tickers(value: str) -> tuple[str, ...]:
    """Resolve optional ticker filters from inline CSV or a JSON array file."""
    stripped = value.strip()
    if not stripped:
        return ()
    if stripped.startswith("@"):
        payload = json.loads(Path(stripped[1:]).read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("Ticker JSON file must contain an array of strings")
        return _validate_tickers(payload)
    return _validate_tickers([token for token in stripped.split(",") if token.strip()])


def _validate_tickers(values: Iterable[object]) -> tuple[str, ...]:
    """Normalize optional ticker filters into uppercase strings."""
    cleaned: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise ValueError("ticker entries must be strings")
        ticker = value.strip().upper()
        if not ticker:
            raise ValueError("ticker entries cannot be empty")
        cleaned.append(ticker)
    return tuple(cleaned)


def _config_from_args(args: argparse.Namespace) -> FinBERTFineTuneConfig:
    """Build validated run config from CLI arguments."""
    return FinBERTFineTuneConfig(
        run_id=args.run_id.strip(),
        from_date=args.from_date.strip(),
        to_date=args.to_date.strip(),
        news_run_id=args.news_run_id.strip(),
        fine_tune=bool(args.fine_tune),
        tickers=_resolve_tickers(args.tickers),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run offline FinBERT work from the local command line."""
    result = run_finbert_finetuning(_config_from_args(_parse_args(argv)))
    logger.info("Offline FinBERT manifest written to {}", result.artifact_manifest_path)
    return 0


def modal_main(
    run_id: str,
    from_date: str,
    to_date: str,
    news_run_id: str,
    fine_tune: bool = False,
    tickers: str = "",
) -> None:
    """Submit the offline FinBERT runner to Modal from the local CLI."""
    globals()["modal_run_finbert_finetuning"].remote(
        run_id=run_id,
        from_date=from_date,
        to_date=to_date,
        news_run_id=news_run_id,
        fine_tune=fine_tune,
        tickers=tickers,
    )


def _modal_run_finbert_finetuning_entry(
    run_id: str,
    from_date: str,
    to_date: str,
    news_run_id: str,
    fine_tune: bool = False,
    tickers: str = "",
) -> dict[str, object]:
    """Run offline FinBERT evaluation/fine-tuning on Modal."""
    result = run_finbert_finetuning(
        FinBERTFineTuneConfig(
            run_id=run_id,
            from_date=from_date,
            to_date=to_date,
            news_run_id=news_run_id,
            fine_tune=fine_tune,
            tickers=_resolve_tickers(tickers),
        ),
        runtime_config=load_finbert_finetuning_runtime_config(),
    )
    return {
        "run_id": result.run_id,
        "dataset_rows": result.dataset_rows,
        "train_rows": result.train_rows,
        "eval_rows": result.eval_rows,
        "baseline_metrics_path": result.baseline_metrics_path,
        "dataset_report_path": result.dataset_report_path,
        "pipeline_manifest_path": result.pipeline_manifest_path,
        "artifact_manifest_path": result.artifact_manifest_path,
        "bundle_path": result.bundle_path,
        "fine_tuned": result.fine_tuned,
        "missing_news_dates": list(result.missing_news_dates),
    }


def _define_modal_app() -> object | None:
    """Create the Modal app when the modal package is installed."""
    try:
        modal = importlib.import_module("modal")
    except ModuleNotFoundError:
        return None

    runtime = load_finbert_finetuning_runtime_config()
    image = _build_modal_image(modal, runtime)
    app = modal.App(runtime.app_name)
    secrets = build_modal_secrets(
        modal,
        named_secret_names=(runtime.r2_secret_name,),
        env_file=SIMFIN_MODAL_ENV_FILE,
        env_keys=SIMFIN_MODAL_ENV_KEYS,
    )

    function_options: dict[str, object] = {
        "image": image,
        "secrets": secrets,
        "timeout": runtime.timeout_seconds,
    }
    if runtime.gpu_type == "T4":
        function_options["gpu"] = "T4"

    modal_run_finbert_finetuning = app.function(
        **function_options,
    )(_modal_run_finbert_finetuning_entry)

    app.local_entrypoint()(modal_main)
    globals()["modal_run_finbert_finetuning"] = modal_run_finbert_finetuning
    return app


def _build_modal_image(
    modal_module: object,
    runtime: FinBERTFineTuneRuntimeConfig,
) -> object:
    """Build the Modal image while preserving local `-r base.txt` includes."""
    requirements_path = Path(runtime.requirements_path)
    requirements_dir = requirements_path.parent
    remote_requirements_path = f"{MODAL_REPO_ROOT}/{requirements_path.as_posix()}"
    return (
        modal_module.Image.debian_slim(python_version=runtime.python_version)
        .add_local_dir(_REPO_ROOT / "app", f"{MODAL_REPO_ROOT}/app", copy=True)
        .add_local_dir(_REPO_ROOT / "core", f"{MODAL_REPO_ROOT}/core", copy=True)
        .add_local_dir(_REPO_ROOT / "services", f"{MODAL_REPO_ROOT}/services", copy=True)
        .add_local_dir(_REPO_ROOT / "config", f"{MODAL_REPO_ROOT}/config", copy=True)
        .add_local_dir(
            _REPO_ROOT / requirements_dir,
            f"{MODAL_REPO_ROOT}/{requirements_dir.as_posix()}",
            copy=True,
        )
        .env(
            {
                "AI_STOCK_TRADER_REPO_ROOT": MODAL_REPO_ROOT,
                "PYTHONPATH": MODAL_REPO_ROOT,
            }
        )
        .workdir(MODAL_REPO_ROOT)
        .run_commands(f"python -m pip install -r {remote_requirements_path}")
    )


def _require_pandas() -> types.ModuleType:
    """Import pandas lazily with a clear error when absent."""
    try:
        return importlib.import_module("pandas")
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError("pandas is required for offline FinBERT evaluation.") from exc


app = _define_modal_app()


if __name__ == "__main__":
    raise SystemExit(main())

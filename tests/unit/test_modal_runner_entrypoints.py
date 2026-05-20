from __future__ import annotations

import importlib
from dataclasses import dataclass, field

import pytest

from app.lab.data_pipelines import (
    backfill_layer1,
    run_daily_layer1,
    run_finbert_sentiment,
    run_hmm_regime_detection,
    run_news_preprocessing,
    run_text_topics,
)
from app.lab.training import run_finbert_finetuning

_WORKSPACE_IMAGE_MODULES = {
    run_news_preprocessing,
    run_text_topics,
    run_finbert_sentiment,
    run_hmm_regime_detection,
    run_finbert_finetuning,
}


@dataclass
class _FakeImage:
    """Minimal Modal image stub used for app-construction tests."""

    python_version: str
    requirements_path: str | None = None
    local_dirs: list[tuple[str, str, bool]] = field(default_factory=list)
    env_vars: dict[str, str] = field(default_factory=dict)
    workdir_path: str | None = None
    commands: list[str] = field(default_factory=list)

    @classmethod
    def debian_slim(cls, *, python_version: str) -> _FakeImage:
        """Return a fake image configured for the requested Python version."""
        return cls(python_version=python_version)

    def pip_install_from_requirements(self, path: str) -> _FakeImage:
        """Record the requirements file used to build the fake image."""
        self.requirements_path = path
        return self

    def add_local_dir(self, local_path, remote_path: str, *, copy: bool) -> _FakeImage:
        """Record one mounted local directory."""
        self.local_dirs.append((str(local_path), remote_path, copy))
        return self

    def env(self, payload: dict[str, str]) -> _FakeImage:
        """Record image environment variables."""
        self.env_vars.update(payload)
        return self

    def workdir(self, path: str) -> _FakeImage:
        """Record the image working directory."""
        self.workdir_path = path
        return self

    def run_commands(self, *commands: str) -> _FakeImage:
        """Record image build commands."""
        self.commands.extend(commands)
        return self


@dataclass(frozen=True)
class _FakeSecretRef:
    """Modal secret reference stub."""

    name: str


class _FakeSecret:
    """Factory for fake Modal secret references."""

    @staticmethod
    def from_name(name: str) -> _FakeSecretRef:
        """Return a fake Modal secret reference."""
        return _FakeSecretRef(name=name)


@dataclass
class _FakeRegisteredFunction:
    """Decorator result returned by `app.function(...)`."""

    name: str
    options: dict[str, object]
    remote_calls: list[dict[str, object]] = field(default_factory=list)

    def remote(self, **kwargs: object) -> None:
        """Record one remote invocation."""
        self.remote_calls.append(kwargs)


@dataclass
class _FakeFunctionCall:
    """Minimal Modal function-call handle used for `.spawn()` tests."""

    payload: dict[str, object]
    get_calls: int = 0

    def get(self, timeout: float | None = None) -> dict[str, object]:
        """Return the staged payload and record the await."""
        del timeout
        self.get_calls += 1
        return dict(self.payload)


class _FakeApp:
    """Minimal Modal app stub that records functions and entrypoints."""

    def __init__(self, name: str) -> None:
        """Initialize an empty fake app."""
        self.name = name
        self.functions: dict[str, _FakeRegisteredFunction] = {}
        self.local_entrypoints: list[str] = []

    def function(self, **kwargs: object):
        """Return a decorator that records function wiring."""

        def decorator(fn):
            registered = _FakeRegisteredFunction(name=fn.__name__, options=kwargs)
            self.functions[fn.__name__] = registered
            return registered

        return decorator

    def local_entrypoint(self):
        """Return a decorator that records local entrypoints."""

        def decorator(fn):
            self.local_entrypoints.append(fn.__name__)
            return fn

        return decorator


class _FakeModal:
    """Minimal subset of the Modal SDK used by the runners."""

    Image = _FakeImage
    Secret = _FakeSecret
    App = _FakeApp


def _install_fake_modal(monkeypatch: pytest.MonkeyPatch) -> _FakeModal:
    """Patch `importlib.import_module` so runner builders see a fake Modal SDK."""
    fake_modal = _FakeModal()
    real_import_module = importlib.import_module

    def _fake_import_module(name: str, package: str | None = None) -> object:
        if name == "modal":
            return fake_modal
        return real_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", _fake_import_module)
    return fake_modal


@pytest.mark.parametrize(
    (
        "module",
        "builder_name",
        "config_loader_name",
        "app_name_attr",
        "function_name",
        "modal_args",
        "expected_remote_kwargs",
    ),
    [
        (
            run_news_preprocessing,
            "_define_modal_app",
            "load_modal_runtime_config",
            "app_name",
            "modal_run_news_preprocessing",
            ("smoke-news", "2024-01-02", 5),
            {
                "run_id": "smoke-news",
                "as_of_date": "2024-01-02",
                "min_sentence_chars": 5,
            },
        ),
        (
            run_text_topics,
            "_define_modal_app",
            "load_text_model_runtime_config",
            "app_name",
            "modal_run_text_topics",
            (
                "smoke-topics",
                "2024-01-02",
                "features/layer1/news_sentiment/2024-01-02/smoke.parquet",
            ),
            {
                "run_id": "smoke-topics",
                "as_of_date": "2024-01-02",
                "preprocessed_news_key": "features/layer1/news_sentiment/2024-01-02/smoke.parquet",
            },
        ),
        (
            run_finbert_sentiment,
            "_define_modal_app",
            "load_finbert_runtime_config",
            "app_name",
            "modal_run_finbert_sentiment",
            (
                "smoke-finbert",
                "2024-01-02",
                "features/layer1/news_sentiment/2024-01-02/smoke.parquet",
            ),
            {
                "run_id": "smoke-finbert",
                "as_of_date": "2024-01-02",
                "preprocessed_news_key": "features/layer1/news_sentiment/2024-01-02/smoke.parquet",
            },
        ),
        (
            run_hmm_regime_detection,
            "_define_modal_app",
            "load_modal_runtime_config",
            "hmm_regime_app_name",
            "modal_run_hmm_regime_detection",
            (
                "smoke-hmm",
                "2024-01-31",
                "2024-02-01,2024-02-02",
                "2024-01-02",
                " spy ",
                10,
                5,
            ),
            {
                "run_id": "smoke-hmm",
                "train_start_date": "2024-01-02",
                "train_end_date": "2024-01-31",
                "inference_dates": "2024-02-01,2024-02-02",
                "benchmark_ticker": "SPY",
                "max_iterations": 10,
                "min_training_rows": 5,
            },
        ),
        (
            run_finbert_finetuning,
            "_define_modal_app",
            "load_finbert_finetuning_runtime_config",
            "app_name",
            "modal_run_finbert_finetuning",
            (
                "smoke-finetune",
                "2024-01-02",
                "2024-01-05",
                "news-run",
                True,
                "spy, aapl",
            ),
            {
                "run_id": "smoke-finetune",
                "from_date": "2024-01-02",
                "to_date": "2024-01-05",
                "news_run_id": "news-run",
                "fine_tune": True,
                "tickers": "spy, aapl",
            },
        ),
        (
            backfill_layer1,
            "_define_modal_app",
            "load_modal_runtime_config",
            "app_name",
            "modal_run_backfill_layer1",
            ("smoke-backfill", "spy, aapl", "qqq"),
            {
                "run_id": "smoke-backfill",
                "tickers": ["SPY", "AAPL"],
                "benchmark_ticker": "QQQ",
            },
        ),
    ],
)
def test_modal_runner_entrypoints_build_apps_and_dispatch_remote_calls(
    monkeypatch: pytest.MonkeyPatch,
    module,
    builder_name: str,
    config_loader_name: str,
    app_name_attr: str,
    function_name: str,
    modal_args: tuple[object, ...],
    expected_remote_kwargs: dict[str, object],
) -> None:
    """Each Layer 1 / 1.5 runner exposes a smoke-testable Modal app and entrypoint."""
    _install_fake_modal(monkeypatch)

    builder = getattr(module, builder_name)
    config_loader = getattr(module, config_loader_name)
    app = builder()
    runtime = config_loader()
    registered = app.functions.get(function_name)
    if registered is None:
        assert module in _WORKSPACE_IMAGE_MODULES
        assert len(app.functions) == 1
        registered = next(iter(app.functions.values()))
    image = registered.options["image"]
    expected_python = getattr(runtime, "python_version", "3.11")
    expected_requirements = getattr(runtime, "requirements_path", "requirements/modal.txt")

    assert app.name == getattr(runtime, app_name_attr)
    assert app.local_entrypoints == ["modal_main"]
    assert image.python_version == expected_python
    if module in _WORKSPACE_IMAGE_MODULES:
        modal_repo_root = getattr(module, "MODAL_REPO_ROOT")
        assert image.requirements_path is None
        assert image.workdir_path == modal_repo_root
        assert image.env_vars["AI_STOCK_TRADER_REPO_ROOT"] == modal_repo_root
        assert image.env_vars["PYTHONPATH"] == modal_repo_root
        assert image.commands == [
            f"python -m pip install -r {modal_repo_root}/{expected_requirements}"
        ]
    else:
        assert image.requirements_path == expected_requirements
    assert registered.options["timeout"] == runtime.timeout_seconds
    if module in _WORKSPACE_IMAGE_MODULES:
        assert "serialized" not in registered.options
    else:
        assert registered.options["serialized"] is True
    assert registered.options["secrets"][0].name == runtime.r2_secret_name
    if getattr(runtime, "gpu_type", None):
        assert registered.options["gpu"] == runtime.gpu_type

    getattr(module, "modal_main")(*modal_args)

    assert registered.remote_calls == [expected_remote_kwargs]


def test_daily_layer1_modal_app_builds_workspace_image(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The daily orchestrator app packages the repo workspace into the Modal image."""
    _install_fake_modal(monkeypatch)

    app = run_daily_layer1._define_modal_app()
    runtime = run_daily_layer1.load_modal_runtime_config()
    registered = app.functions["_modal_run_daily_layer1_entry"]
    batched_registered = app.functions["_modal_run_batched_layer1_entry"]
    image = registered.options["image"]

    assert app.name == runtime.app_name
    assert app.local_entrypoints == ["modal_main"]
    assert image.python_version == runtime.python_version
    assert image.workdir_path == run_daily_layer1.MODAL_REPO_ROOT
    assert image.env_vars["AI_STOCK_TRADER_REPO_ROOT"] == run_daily_layer1.MODAL_REPO_ROOT
    assert image.env_vars["PYTHONPATH"] == run_daily_layer1.MODAL_REPO_ROOT
    assert image.commands == [
        (
            "python -m pip install -r "
            f"{run_daily_layer1.MODAL_REPO_ROOT}/requirements/modal.txt"
        )
    ]
    assert registered.options["timeout"] == runtime.timeout_seconds
    assert registered.options["secrets"][0].name == runtime.r2_secret_name
    assert batched_registered.options["image"] is image
    assert batched_registered.options["timeout"] == runtime.batch_timeout_seconds
    assert batched_registered.options["secrets"][0].name == runtime.r2_secret_name
    assert batched_registered.options["gpu"] == runtime.batch_gpu_type


def test_daily_layer1_modal_main_orchestrates_stage_apps_before_final_assembly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The daily local entrypoint delegates heavy work to stage-specific Modal apps."""

    class _FakeStageRemote:
        def __init__(self, payload: dict[str, object]) -> None:
            self.payload = payload
            self.remote_calls: list[dict[str, object]] = []

        def remote(self, **kwargs: object) -> dict[str, object]:
            self.remote_calls.append(kwargs)
            return dict(self.payload)

    final_remote = _FakeRegisteredFunction(name="modal_run_daily_layer1", options={})
    news_remote = _FakeStageRemote(
        {"output_key": "features/layer1/news_sentiment/2024-01-02/smoke.parquet"}
    )
    topics_remote = _FakeStageRemote(
        {"topic_feature_key": "features/layer1/topic_features/2024-01-02/smoke.parquet"}
    )
    finbert_remote = _FakeStageRemote(
        {"sentiment_feature_key": "features/layer1/sentiment_features/2024-01-02/smoke.parquet"}
    )
    regime_remote = _FakeStageRemote(
        {"output_key": "features/layer1_5/regime/smoke-daily-2024-01-02.parquet"}
    )

    monkeypatch.setattr(run_daily_layer1, "_modal_run_daily_layer1", final_remote)
    monkeypatch.setattr(run_daily_layer1.news_module, "modal_run_news_preprocessing", news_remote)
    monkeypatch.setattr(run_daily_layer1.text_topics_module, "modal_run_text_topics", topics_remote)
    monkeypatch.setattr(
        run_daily_layer1.finbert_module,
        "modal_run_finbert_sentiment",
        finbert_remote,
    )
    monkeypatch.setattr(
        run_daily_layer1.regime_module,
        "modal_run_hmm_regime_detection",
        regime_remote,
    )
    warnings: list[str] = []
    monkeypatch.setattr(
        run_daily_layer1.logger,
        "warning",
        lambda message: warnings.append(message),
    )

    run_daily_layer1.modal_main(
        "smoke-daily",
        "2024-01-02",
        "layer0-daily-2024-01-02",
        " spy ",
        True,
        min_sentence_chars=5,
        hmm_train_start_date="2023-10-01",
        hmm_max_iterations=77,
        hmm_min_training_rows=11,
    )

    assert warnings == [
        (
            "allow_layer0_manifest_date_range=True is intended for historical readiness runs "
            "only and must not be used on the Pi daily path"
        )
    ]
    assert news_remote.remote_calls == [
        {
            "run_id": "smoke-daily-2024-01-02",
            "as_of_date": "2024-01-02",
            "min_sentence_chars": 5,
        }
    ]
    assert topics_remote.remote_calls == [
        {
            "run_id": "smoke-daily-2024-01-02",
            "as_of_date": "2024-01-02",
            "preprocessed_news_key": "features/layer1/news_sentiment/2024-01-02/smoke.parquet",
        }
    ]
    assert finbert_remote.remote_calls == [
        {
            "run_id": "smoke-daily-2024-01-02",
            "as_of_date": "2024-01-02",
            "preprocessed_news_key": "features/layer1/news_sentiment/2024-01-02/smoke.parquet",
        }
    ]
    assert regime_remote.remote_calls == [
        {
            "run_id": "smoke-daily-2024-01-02",
            "train_start_date": "2023-10-01",
            "train_end_date": "2024-01-01",
            "inference_dates": "2024-01-02",
            "benchmark_ticker": "SPY",
            "max_iterations": 77,
            "min_training_rows": 11,
        }
    ]
    assert final_remote.remote_calls == [
        {
            "run_id": "smoke-daily",
            "as_of_date": "2024-01-02",
            "layer0_run_id": "layer0-daily-2024-01-02",
            "benchmark_ticker": "SPY",
            "allow_layer0_manifest_date_range": True,
            "min_sentence_chars": 5,
            "hmm_train_start_date": "2023-10-01",
            "hmm_max_iterations": 77,
            "hmm_min_training_rows": 11,
            "preprocessed_news_key": "features/layer1/news_sentiment/2024-01-02/smoke.parquet",
            "topic_feature_key": "features/layer1/topic_features/2024-01-02/smoke.parquet",
            "sentiment_feature_key": "features/layer1/sentiment_features/2024-01-02/smoke.parquet",
            "regime_output_key": "features/layer1_5/regime/smoke-daily-2024-01-02.parquet",
        }
    ]


def test_daily_layer1_modal_range_main_dispatches_batched_remote_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The range entrypoint submits one batched Modal job for a multi-date window."""
    final_remote = _FakeRegisteredFunction(name="modal_run_batched_layer1", options={})
    logged_messages: list[tuple[str, bool]] = []

    monkeypatch.setattr(run_daily_layer1, "_modal_run_batched_layer1", final_remote)
    monkeypatch.setattr(
        run_daily_layer1.logger,
        "info",
        lambda message, manifest_key, ready_for_layer2: logged_messages.append(
            (manifest_key, ready_for_layer2)
        )
        if message == "Layer 1 batched Modal run complete manifest={} ready_for_layer2={}"
        else None,
    )
    final_remote.remote = lambda **kwargs: (
        final_remote.remote_calls.append(kwargs)
        or {
            "manifest_key": "artifacts/manifests/layer1/smoke-range.json",
            "ready_for_layer2": True,
        }
    )

    run_daily_layer1.modal_range_main(
        run_id="smoke-range",
        from_date="2024-01-03",
        to_date="2024-01-05",
        layer0_run_id="layer0-range",
        benchmark_ticker=" spy ",
        allow_layer0_manifest_date_range=True,
        min_sentence_chars=4,
        hmm_train_start_date="2023-10-01",
        hmm_max_iterations=61,
        hmm_min_training_rows=12,
    )

    assert final_remote.remote_calls == [
        {
            "run_id": "smoke-range",
            "from_date": "2024-01-03",
            "to_date": "2024-01-05",
            "layer0_run_id": "layer0-range",
            "benchmark_ticker": "SPY",
            "allow_layer0_manifest_date_range": True,
            "min_sentence_chars": 4,
            "hmm_train_start_date": "2023-10-01",
            "hmm_max_iterations": 61,
            "hmm_min_training_rows": 12,
        }
    ]
    assert logged_messages == [
        ("artifacts/manifests/layer1/smoke-range.json", True)
    ]

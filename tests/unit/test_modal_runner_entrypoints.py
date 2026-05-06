from __future__ import annotations

import importlib
from dataclasses import dataclass, field

import pytest

from app.lab.data_pipelines import (
    backfill_layer1,
    run_finbert_sentiment,
    run_hmm_regime_detection,
    run_news_preprocessing,
    run_text_topics,
)


@dataclass
class _FakeImage:
    """Minimal Modal image stub used for app-construction tests."""

    python_version: str
    requirements_path: str | None = None

    @classmethod
    def debian_slim(cls, *, python_version: str) -> _FakeImage:
        """Return a fake image configured for the requested Python version."""
        return cls(python_version=python_version)

    def pip_install_from_requirements(self, path: str) -> _FakeImage:
        """Record the requirements file used to build the fake image."""
        self.requirements_path = path
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
                "SPY",
                10,
                5,
            ),
            {
                "run_id": "smoke-hmm",
                "train_start_date": "2024-01-02",
                "train_end_date": "2024-01-31",
                "inference_dates": ["2024-02-01", "2024-02-02"],
                "benchmark_ticker": "SPY",
                "max_iterations": 10,
                "min_training_rows": 5,
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
    registered = app.functions[function_name]
    image = registered.options["image"]
    expected_python = getattr(runtime, "python_version", "3.11")
    expected_requirements = getattr(runtime, "requirements_path", "requirements/modal.txt")

    assert app.name == getattr(runtime, app_name_attr)
    assert app.local_entrypoints == ["modal_main"]
    assert image.python_version == expected_python
    assert image.requirements_path == expected_requirements
    assert registered.options["timeout"] == runtime.timeout_seconds
    assert registered.options["serialized"] is True
    assert registered.options["secrets"][0].name == runtime.r2_secret_name

    getattr(module, "modal_main")(*modal_args)

    assert registered.remote_calls == [expected_remote_kwargs]

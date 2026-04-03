from __future__ import annotations

import shutil
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT_DIR / "artifacts" / "deployments" / "huggingface_space_lab"

DEPLOYMENT_FILES: dict[str, str] = {
    "cloud_training/__init__.py": "cloud_training/__init__.py",
    "cloud_training/requirements.txt": "cloud_training/requirements-training.txt",
    "cloud_training/data_pipelines/alpaca_news.py": "cloud_training/data_pipelines/alpaca_news.py",
    "cloud_training/data_pipelines/build_predictive_dataset.py": "cloud_training/data_pipelines/build_predictive_dataset.py",
    "cloud_training/data_pipelines/finbert_sentiment.py": "cloud_training/data_pipelines/finbert_sentiment.py",
    "cloud_training/data_pipelines/predictive_feature_core.py": "cloud_training/data_pipelines/predictive_feature_core.py",
    "cloud_training/model_architecture/__init__.py": "cloud_training/model_architecture/__init__.py",
    "cloud_training/model_architecture/hybrid_model.py": "cloud_training/model_architecture/hybrid_model.py",
    "cloud_training/backtesting/__init__.py": "cloud_training/backtesting/__init__.py",
    "cloud_training/backtesting/validation_metrics.py": "cloud_training/backtesting/validation_metrics.py",
    "cloud_training/hf_jobs.py": "cloud_training/hf_jobs.py",
    "cloud_training/model_repo.py": "cloud_training/model_repo.py",
    "cloud_training/training/hf_job_runner.py": "cloud_training/training/hf_job_runner.py",
    "cloud_training/training/train_predictive_model.py": "cloud_training/training/train_predictive_model.py",
    "cloud_training/training/export_servable_artifact.py": "cloud_training/training/export_servable_artifact.py",
    "cloud_training/training/run_issue12_cloud_pipeline.py": "cloud_training/training/run_issue12_cloud_pipeline.py",
    "cloud_training/training/run_issue21_cloud_pipeline.py": "cloud_training/training/run_issue21_cloud_pipeline.py",
    "cloud_training/training/synthetic_issue12_dataset.py": "cloud_training/training/synthetic_issue12_dataset.py",
    "cloud_training/publish_hf_bundle.py": "cloud_training/publish_hf_bundle.py",
    "runtime/__init__.py": "runtime/__init__.py",
    "runtime/common/__init__.py": "runtime/common/__init__.py",
    "runtime/common/common.py": "runtime/common/common.py",
}

ROOT_APP = "from cloud_training.hf_space_app import demo, ensure_runtime_dirs\nimport os\n\n\nif __name__ == '__main__':\n    ensure_runtime_dirs()\n    demo.launch(server_name='0.0.0.0', server_port=int(os.getenv('PORT', '7860')))\n" 

README = '''---
title: AI Stock Trader Lab
emoji: 📈
colorFrom: blue
colorTo: indigo
sdk: gradio
sdk_version: 5.24.0
python_version: 3.11
app_file: app.py
pinned: false
---

# AI Stock Trader Lab

This Space is the cloud-side Milestone 2 training lab.

## Runtime role
- submit predictive-dataset / train / export flows as durable jobs
- run FinBERT-heavy feature generation in the cloud via the existing repo entrypoints
- train the first predictive model through job-style execution
- export a servable bundle for the Cloud Oracle
- publish the finished bundle into a separate HF model/artifact repo

## Important boundary
This Space is **not** the production inference endpoint.
That remains the lightweight Hugging Face Oracle package under `cloud_inference/`.

## Expected secrets / variables
- `ALPACA_API_KEY`
- `ALPACA_API_SECRET`
- `ALPACA_DATA_URL` (optional; defaults to Alpaca market-data URL)
- `HF_TOKEN` or Hugging Face Space token with write access
- `HF_ARTIFACT_REPO_ID` = target model repo for exported bundles

## Typical flow
1. Build the Space package locally:
   - `.venv/bin/python cloud_training/build_hf_space.py`
2. Sync `artifacts/deployments/huggingface_space_lab/` to the Space repo.
3. For real Issue #12 runs, stage and upload the market snapshot handoff:
   - `.venv/bin/python cloud_training/prepare_issue12_snapshot_handoff.py --overwrite`
   - `.venv/bin/python cloud_training/upload_issue12_snapshot_handoff.py --repo-id FunkMonk87/AI-Stock-Trader-Lab`
4. In the Space UI, submit an Issue #12 pipeline job and inspect the durable job status/logs.
5. Publish or upload the exported `artifacts/bundles/*.bundle.json` into the model/artifact repo.
'''

ROOT_REQUIREMENTS_HEADER = '''# Hugging Face Space runtime for cloud-side training / feature generation.\n# The Oracle endpoint stays separate and lightweight.\n\n'''
ROOT_REQUIREMENTS_FOOTER = '\ngradio==5.24.0\nhuggingface_hub==0.34.4\n'
RUNTIME_TXT = 'python-3.11\n'


def build_requirements() -> str:
    training_requirements = (ROOT_DIR / "cloud_training" / "requirements.txt").read_text().strip()
    return ROOT_REQUIREMENTS_HEADER + training_requirements + ROOT_REQUIREMENTS_FOOTER


def build_hf_space(output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for src_rel, dst_rel in DEPLOYMENT_FILES.items():
        src = ROOT_DIR / src_rel
        dst = output_dir / dst_rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    app_module_dst = output_dir / "cloud_training" / "hf_space_app.py"
    app_module_dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT_DIR / "cloud_training" / "hf_space_app.py", app_module_dst)

    for rel_dir in [
        "artifacts/bundles",
        "data/processed/models",
        "data/processed/training",
        "reports/pipeline",
    ]:
        target = output_dir / rel_dir
        target.mkdir(parents=True, exist_ok=True)
        (target / ".gitkeep").write_text("")

    (output_dir / "app.py").write_text(ROOT_APP)
    (output_dir / "requirements.txt").write_text(build_requirements())
    (output_dir / "runtime.txt").write_text(RUNTIME_TXT)
    (output_dir / "README.md").write_text(README)
    return output_dir


if __name__ == "__main__":
    built = build_hf_space()
    print(built)

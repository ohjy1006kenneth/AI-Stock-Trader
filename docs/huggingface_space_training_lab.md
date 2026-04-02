# Hugging Face Space Training Lab

Issue #18 defines the **cloud-side lab/runtime path** for AI-heavy feature generation and training.

This is intentionally separate from the production Hugging Face Oracle inference package.

## Canonical split

### 1. Hugging Face Space = training / feature-generation control plane
Use the Space for:
- submitting FinBERT-heavy news feature generation and training jobs
- dataset construction via the existing repo entrypoints
- first predictive-model training via durable job-style execution
- servable bundle export
- manual or operator-driven experiment runs for Milestone 2

The Space is the right place for heavier Python dependencies such as:
- `transformers`
- `torch`
- Gradio UI/runtime helpers

### 2. Hugging Face Inference Endpoint = production Oracle
Use the existing Oracle package for:
- loading the latest validated bundle
- serving the Pi request/response contract
- lightweight artifact-backed inference only

Do **not** turn the production Oracle into the training environment.

## Repo-native Space package

The repo now defines a first-class Space package builder:

```bash
.venv/bin/python cloud_training/build_hf_space.py
```

Output:

```text
artifacts/deployments/huggingface_space_lab/
```

That package contains:
- root `app.py`
- root `requirements.txt`
- root `README.md` with Space metadata
- `cloud_training/hf_space_app.py`
- dataset/training/export scripts needed by Milestone 2
- minimal `data/`, `reports/`, and `artifacts/` runtime folders

## Space runtime shape

The generated Space package is a **Gradio Space**.

Runtime entrypoint:
- `app.py`

Primary UI actions:
1. submit the explicit Issue #12 dataset -> train -> export pipeline entrypoint as a background job
2. submit the publish-latest-bundle flow as a background job
3. inspect durable job status/result JSON and logs
4. inspect latest datasets / artifacts / bundles

Durable job contract path:
- `reports/pipeline/jobs/<job-id>/status.json`
- `reports/pipeline/jobs/<job-id>/run.log`
- `reports/pipeline/jobs/index.json`

The status JSON is the control-plane contract. It persists:
- lifecycle timestamps (`requested_at`, `started_at`, `finished_at`)
- `status` (`queued`, `running`, `succeeded`, `failed`)
- the exact command and cwd
- the durable log path
- parsed JSON result payload when the underlying entrypoint emits one
- return code / error details for failures

Dependencies are intentionally layered like this:
- start from `cloud_training/requirements.txt`
- add `gradio`
- add `huggingface_hub`

This keeps training dependencies in the Space while keeping the Oracle package lean.

## Sync path: repo -> Space

Build the package locally:

```bash
.venv/bin/python cloud_training/build_hf_space.py
```

Upload it to the Space repo:

```bash
.venv/bin/python cloud_training/sync_hf_space.py \
  --repo-id FunkMonk87/AI-Stock-Trader-Lab
```

This makes the Space deployment path repo-native and reproducible.

## Output path: Space -> HF model/artifact repo

The Space should not be the long-term source of truth for trained artifacts.

Canonical artifact flow:

```text
Space training run
-> data/processed/models/*.artifact.json + *.metrics.json
-> artifacts/bundles/*.bundle.json
-> publish bundle to a dedicated HF model/artifact repo
-> Oracle deployment consumes promoted bundle from repo/build output
```

End-to-end Issue #12 command in a cloud runtime:

```bash
.venv/bin/python -m cloud_training.training.run_issue12_cloud_pipeline \
  --build-dataset \
  --epochs 250 \
  --export-bundle
```

Before that real-data command can succeed in a fresh Space/cloud runtime, the operator must provision:
- `data/runtime/market/price_snapshot.json`
- `data/runtime/market/fundamental_snapshot.json`
- `ALPACA_API_KEY`
- `ALPACA_API_SECRET`

Those market snapshot inputs are not automatically created by the Space package itself.

Repo-native handoff path:

```bash
.venv/bin/python cloud_training/prepare_issue12_snapshot_handoff.py --overwrite
.venv/bin/python cloud_training/upload_issue12_snapshot_handoff.py \
  --repo-id FunkMonk87/AI-Stock-Trader-Lab
```

That copies the staged market snapshot files into the Space repo at the exact runtime paths used by the pipeline.

For local/plumbing smoke only:

```bash
.venv/bin/python -m pip install -r requirements/issue12_cloud.txt
.venv/bin/python -m cloud_training.training.run_issue12_cloud_pipeline \
  --smoke-synthetic-dataset \
  --epochs 25 \
  --export-bundle
```

Publishing command:

```bash
.venv/bin/python cloud_training/publish_hf_bundle.py \
  --repo-id <org-or-user>/<artifact-repo>
```

Promotion / approval command:

```bash
.venv/bin/python cloud_training/publish_hf_bundle.py \
  --repo-id <org-or-user>/<artifact-repo> \
  --approve-for-oracle
```

Canonical model repo contract:
- `bundles/<bundle-name>.bundle.json`
- `manifests/bundles/<bundle-name>.manifest.json`
- `manifest.json`
- `channels/approved/manifest.json` when a bundle is explicitly approved for Oracle use
- `endpoints/oracle/ready.json` when an approved bundle is declared deployment-ready

Default behavior:
- publishes the latest `artifacts/bundles/*.bundle.json`
- writes the canonical per-bundle + root manifests into the target model repo
- with `--approve-for-oracle`, also writes the approved-channel and endpoint-ready manifests that downstream deployment builders can consume directly

## Promotion boundary

Issue #18 only defines the environment/path, not automated promotion policy.

Human or PM/orchestrator review should still decide:
- which trained bundle is promotable
- which bundle should be pinned into the Oracle deployment
- whether later automation should mirror or tag validated bundles

## Required human configuration

Two IDs still require an explicit human choice:
- the final Hugging Face **Space repo ID** to target for sync
- the final Hugging Face **model/artifact repo ID** to receive exported bundles

The current known Space target is:
- `FunkMonk87/AI-Stock-Trader-Lab`

The artifact/model repo target is still a required decision if not already created.

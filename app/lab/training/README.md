# training

Training entry points, exporters, and job runners live here.

This folder should contain the scripts that actually execute the training pipeline.

- `finbert_finetuning.py` builds point-in-time-safe offline datasets by joining
  Layer 1 preprocessed news with Layer 1 forward-return label shards.
- `run_finbert_finetuning.py` evaluates the pinned baseline FinBERT model and can
  optionally fine-tune it offline on Modal or in a lab environment.

Offline FinBERT safety rules:
- Inputs come only from archived Layer 1 news plus archived Layer 1 label shards.
- Outputs are written under local `artifacts/` paths, not to production serving config.
- The emitted artifact manifest always sets `approved: false`.
- Production inference continues to use `config/finbert_sentiment.json` until a human
  approves the offline artifact and a separate change updates the production model pin.

Typical local or Modal command:

```bash
HOME=/home/juyoungoh ./.venv/bin/python app/lab/training/run_finbert_finetuning.py \
    --run-id finbert-offline-2026-05-14 \
    --from-date 2026-03-01 \
    --to-date 2026-04-10 \
    --news-run-id layer1-readiness-2026-04-10-v7 \
    --fine-tune
```

Offline output layout:
- `artifacts/bundles/finbert_finetuning/{run_id}/`
- `artifacts/manifests/lab_finbert_finetuning/{run_id}.json`
- `artifacts/manifests/lab_finbert_finetuning_artifact/{run_id}.json`
- `artifacts/reports/diagnostics/finbert_finetuning_{run_id}.json`

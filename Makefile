.PHONY: install test test-r2-live layer1-daily lint typecheck clean docker-pi

# ── Local development ──────────────────────────────────────────────────────────

PYTHON := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
PIP := $(PYTHON) -m pip
PYTEST := $(PYTHON) -m pytest

install:
	$(PIP) install -r requirements/dev.txt

install-modal:
	$(PIP) install -r requirements/modal.txt

test:
	$(PYTEST) tests/unit/ -v --tb=short

test-r2-live:
	RUN_R2_INTEGRATION=1 $(PYTEST) tests/integration/test_r2_live.py -v --tb=short

test-cov:
	$(PYTEST) tests/unit/ -v --tb=short --cov=core --cov=services --cov-report=term-missing

layer1-daily:
	@test -n "$(RUN_ID)" || (echo "RUN_ID is required"; exit 1)
	@test -n "$(FROM_DATE)" || (echo "FROM_DATE is required"; exit 1)
	$(PYTHON) app/lab/data_pipelines/run_daily_layer1.py \
		--run-id "$(RUN_ID)" \
		--from-date "$(FROM_DATE)" \
		$(if $(TO_DATE),--to-date "$(TO_DATE)",) \
		$(if $(LAYER0_RUN_ID),--layer0-run-id "$(LAYER0_RUN_ID)",) \
		$(if $(TICKERS),--tickers $(TICKERS),) \
		$(if $(VALIDATION_OUTPUT_DIR),--validation-output-dir "$(VALIDATION_OUTPUT_DIR)",)

validate-universe:
	$(PYTHON) app/lab/data_pipelines/validate_universe_membership.py --max-violation-rate 0.01

lint:
	ruff check .

lint-fix:
	ruff check --fix .

typecheck:
	mypy core/ services/ app/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .ruff_cache

# ── Docker (Pi image) ──────────────────────────────────────────────────────────

docker-pi:
	docker buildx build \
		--platform linux/arm64 \
		-f docker/pi/Dockerfile \
		-t ai-stock-trader-pi:latest \
		.

docker-pi-run:
	docker run --rm \
		--env-file config/alpaca.env \
		ai-stock-trader-pi:latest

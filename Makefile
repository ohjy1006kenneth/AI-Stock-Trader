.PHONY: install test lint typecheck clean docker-pi

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

test-cov:
	$(PYTEST) tests/unit/ -v --tb=short --cov=core --cov=services --cov-report=term-missing

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

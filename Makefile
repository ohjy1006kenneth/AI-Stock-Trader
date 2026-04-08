.PHONY: install test test-r2-live lint typecheck clean docker-pi

# ── Local development ──────────────────────────────────────────────────────────

install:
	pip install -r requirements/dev.txt

install-modal:
	pip install -r requirements/modal.txt

test:
	pytest tests/unit/ -v --tb=short

test-r2-live:
	RUN_R2_INTEGRATION=1 pytest tests/integration/test_r2_live.py -v --tb=short

test-cov:
	pytest tests/unit/ -v --tb=short --cov=core --cov=services --cov-report=term-missing

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

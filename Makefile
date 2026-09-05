.PHONY: help install-test-deps lint format check test test-storage test-storage-core test-ingest test-fdau test-flt test-settings test-coverage coverage cov build build-clean clean

# Forward extra CLI args to pytest, e.g.:
# make test -- -q
# make test-storage -- -k compaction
MAKE_TARGETS := help install-test-deps lint format check test test-storage test-storage-core test-ingest test-fdau test-flt test-settings test-coverage coverage cov build build-clean clean
PYTEST_ARGS = $(filter-out $(MAKE_TARGETS),$(MAKECMDGOALS))
COVERAGE_MIN ?= 70
UV ?= uv
UV_RUN := $(UV) run

help: ## Show this help message
	@$(UV_RUN) python -c "import re; from pathlib import Path; lines=Path('Makefile').read_text().splitlines(); [print(f'  {m.group(1):<20} {m.group(2)}') for line in lines if (m:=re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line))]"

install-test-deps: ## Install project and test dependencies
	$(UV) sync --all-groups

lint: ## Run ruff checks with fixes
	$(UV_RUN) ruff check . --fix

format: ## Format Python files with ruff
	$(UV_RUN) ruff format .

check: lint format test ## Run lint, format, and tests

test: ## Run all tests
	$(UV_RUN) pytest $(PYTEST_ARGS)

test-storage: ## Run storage tests
	$(UV_RUN) pytest tests/storage $(PYTEST_ARGS)

test-storage-core: ## Run StorageCore tests
	$(UV_RUN) pytest tests/storage/test_storage_core.py $(PYTEST_ARGS)

test-ingest: ## Run ingest tests
	$(UV_RUN) pytest tests/test_ingest.py $(PYTEST_ARGS)

test-fdau: ## Run FDAU tests
	$(UV_RUN) pytest tests/test_fdau.py $(PYTEST_ARGS)

test-flt: ## Run FLT parser tests
	$(UV_RUN) pytest tests/test_flt.py $(PYTEST_ARGS)

test-settings: ## Run settings tests
	$(UV_RUN) pytest tests/test_settings.py $(PYTEST_ARGS)

test-coverage: ## Run tests with coverage report and threshold (artifacts under tmp/coverage/)
	@$(UV_RUN) python -c "import pathlib; pathlib.Path('tmp/coverage').mkdir(parents=True, exist_ok=True)"
	$(UV_RUN) pytest --cov=src --cov-report=html --cov-report=term --cov-fail-under=$(COVERAGE_MIN) $(PYTEST_ARGS)

coverage: test-coverage ## Alias for test-coverage

cov: test-coverage ## Alias for test-coverage

build: ## Build docs/LATEX/main.tex
	$(UV_RUN) python docs/LATEX/build/build.py

build-clean: ## Clean docs/LATEX build artifacts
	$(UV_RUN) python docs/LATEX/build/build.py clean

clean: ## Clean up generated files
	$(UV_RUN) python -c "import pathlib, shutil; r=pathlib.Path('.'); [p.unlink() for p in r.rglob('*.pyc')]; [shutil.rmtree(p, ignore_errors=True) for p in r.rglob('__pycache__')]; [shutil.rmtree(p, ignore_errors=True) for p in r.rglob('*.egg-info')]; [shutil.rmtree(p, ignore_errors=True) for p in [pathlib.Path('tmp/coverage'), pathlib.Path('tmp/.ruff_cache'), pathlib.Path('htmlcov'), pathlib.Path('.pytest_cache'), pathlib.Path('.ruff_cache')]]; pathlib.Path('.coverage').unlink(missing_ok=True); pathlib.Path('tmp/coverage/.coverage').unlink(missing_ok=True)"

# Swallow extra positional args passed after '--' so make does not fail.
%:
	@:

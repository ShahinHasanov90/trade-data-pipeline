.PHONY: help install test lint format clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -r requirements.txt

test: ## Run test suite
	pytest tests/ -v --tb=short

lint: ## Run linters
	flake8 src/ tests/ --max-line-length 120
	mypy src/ --ignore-missing-imports

format: ## Format code
	black src/ tests/ --line-length 120

clean: ## Clean artifacts
	find . -type d -name __pycache__ -exec rm -rf {} +
	rm -rf .pytest_cache .mypy_cache dist build *.egg-info

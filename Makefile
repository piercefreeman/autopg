.PHONY: lint lint-ruff lint-pyright

lint: lint-fix lint-pyright

lint-ci: lint-ruff lint-pyright

lint-ruff:
	uv run ruff check .

lint-pyright:
	uv run pyright .

# Run all linting in fix mode where possible
lint-fix:
	uv run ruff format .
	uv run ruff check . --fix 

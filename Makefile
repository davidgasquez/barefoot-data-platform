.DEFAULT_GOAL := run

lint:
	uv run ruff check
	uv run ty check

test:
	uv run bdp test

check: lint
	uv run bdp check
	uv run bdp test

run:
	uv run bdp materialize

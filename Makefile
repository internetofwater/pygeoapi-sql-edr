include .env

# install dependencies
# this project uses uv to manage dependencies
deps:
	uv sync --all-groups --locked --all-packages

database:
	docker compose up --build -d database
	docker compose up --build -d mysql

# run pygeoapi dev
dev:
	UV_ENV_FILE=.env uv run pygeoapi openapi generate pygeoapi.config.yml --output-file pygeoapi.openapi.yml
	UV_ENV_FILE=.env PYGEOAPI_CONFIG=pygeoapi.config.yml PYGEOAPI_OPENAPI=pygeoapi.openapi.yml uv run pygeoapi serve

clean:
	rm -rf .venv/
	rm -rf .pytest_cache/

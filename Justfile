# django-object-streams Justfile

set ignore-comments := true
set dotenv-load := true
set dotenv-filename := ".env.local"

bootstrap: # install the local development environment
  cd {{justfile_directory()}} && uv sync --all-groups

[positional-arguments]
test *args:
  cd {{justfile_directory()}} && uv run --no-sync pytest "$@"

[positional-arguments]
coverage *args:
  cd {{justfile_directory()}} && uv run --no-sync pytest --cov "$@"

check:
  just check-ruff
  just check-format

check-ruff:
  cd {{justfile_directory()}} && uv run --group dev --no-sync ruff check .

check-format:
  cd {{justfile_directory()}} && uv run --group dev --no-sync ruff format --check .

fix:
  just fix-ruff
  just fix-format

fix-ruff:
  cd {{justfile_directory()}} && uv run --group dev --no-sync ruff check --fix .

fix-format:
  cd {{justfile_directory()}} && uv run --group dev --no-sync ruff format .

build:
  cd {{justfile_directory()}} && uv run --group dev --no-sync python -m build

manage *args:
  cd {{justfile_directory()}} && uv run --no-sync django-admin {{args}} --settings=tests.settings

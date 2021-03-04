#!/usr/bin/env bash

set -e

source tools/lib/lib.sh

MYPY_CONFIG=${MYPY_CONFIG:-$(pwd)/tools/python/.mypy.ini}
FLAKE8_CONFIG=${FLAKE8_CONFIG:-$(pwd)/tools/python/.flake8}
ISORT_CONFIG=${ISORT_CONFIG:-$(pwd)/tools/python/.isort.cfg}

cmd_setup() {
  # ensure we use the proper version of python
  pyenv install -s

  # install poetry
  pip install poetry

  # install deps
  poetry install
}

cmd_build() {
  local module=$1; shift || error "missing module."

  echo poetry run mypy -m "$module" --config-file "$MYPY_CONFIG"
}

cmd_format() {
  poetry run isort . --settings-file "$ISORT_CONFIG" --line-length 140
  poetry run black . --line-length 140
  poetry run flake8 . --config "$FLAKE8_CONFIG"
}

cmd_test() {
  poetry run pytest
}

USAGE="
Usage: $(basename "$0") <cmd>
Available commands:
  setup
  build
  format
  test
"

main() {
  assert_root

  check_binary pyenv


  local project=$1; shift || error "Missing project path" "$USAGE"
  local cmd=$1; shift || error "Missing command" "$USAGE"

  (
    cd "$project"
    cmd_"$cmd" "$@"
  )
}

main "$@"

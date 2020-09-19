#!/usr/bin/env bash

set -e

. tools/lib/lib.sh

DOCKERFILE=tools/git-crypt/Dockerfile
IMAGE=$(_docker_get_current_image $DOCKERFILE)

function run_git-crypt() {
  docker run --rm -it \
    -v ~/.gnupg:/root/.gnupg \
    -v "$(pwd)":/code \
    -w /code \
    $IMAGE "$@"
}

function main() {
  assert_root

  if [ "$1" == "build" ]; then
    _docker_build $DOCKERFILE
  elif [ "$1" == "publish" ]; then
    _docker_publish $DOCKERFILE
  else
    run_git-crypt "$@"
  fi
}

main "$@"

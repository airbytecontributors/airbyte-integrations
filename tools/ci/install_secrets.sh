#!/usr/bin/env bash

set -e

. tools/lib/lib.sh

WORKDIR=$(mktemp -d)

function install_gpg_key() {
  echo "$GPG_KEY" > $WORKDIR/private.key
  echo "$GPG_PASSPHRASE" | gpg --batch --yes --passphrase-fd 0 --import $WORKDIR/private.key
  gpg --list-secret-keys --keyid-format LONG
}

install_gpg_key

[ "$(cat secrets/encryption_probe)" != "decrypted" ] || error "Secret shouldn't be visible. Something very wrong is happening."

./tools/ci/git-crypt/runner.sh unlock

[ "$(cat secrets/encryption_probe)" == "decrypted" ] || error "Secret should now be visible."


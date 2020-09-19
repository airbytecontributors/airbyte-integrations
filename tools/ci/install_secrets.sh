#!/usr/bin/env bash

set -e

. tools/lib/lib.sh

WORKDIR=$(mktemp -d)
GIT_CRYPT_RELEASE=git-crypt-0.6.0
GIT_CRYPT_BIN=$WORKDIR/$GIT_CRYPT_RELEASE/git-crypt

function install_git-crypt() {
  local tarball=$GIT_CRYPT_RELEASE.tar.gz

  cp tools/ci/$tarball.asc $WORKDIR

  cd $WORKDIR
  wget https://www.agwa.name/projects/git-crypt/downloads/$tarball
  gpg --keyserver hkp://pool.sks-keyservers.net --recv-key 0xEF5D84C1838F2EB6D8968C0410378EFC2080080C
  gpg --verify $tarball.asc $tarball

  tar zxf $tarball
  cd $GIT_CRYPT_RELEASE
  make
}

function install_gpg_key() {
  echo "$GPG_KEY" > $WORKDIR/private.key
  echo "$GPG_PASSPHRASE" | gpg --batch --yes --passphrase-fd 0 --import $WORKDIR/private.key
  gpg --list-secret-keys --keyid-format LONG
}

( install_git-crypt ) # we are cd-ing during install, let's make sure we don't loose the current path
install_gpg_key

[ "$(cat secrets/encryption_probe)" != "decrypted" ] || error "Secret shouldn't be visible. Something very wrong is happening."

$GIT_CRYPT_BIN unlock

[ "$(cat secrets/encryption_probe)" == "decrypted" ] || error "Secret should now be visible."


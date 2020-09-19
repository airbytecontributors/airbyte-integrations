#!/usr/bin/env bash

set -ex

WORKDIR=$(mktemp -d)
GIT_CRYPT_RELEASE=git-crypt-0.6.0

GIT_CRYPT_BIN=

function install_git-crypt() {
  local tarball=$GIT_CRYPT_RELEASE.tar.gz
  cd $WORKDIR
  wget https://www.agwa.name/projects/git-crypt/downloads/$tarball
  tar zxf $tarball
  cd $GIT_CRYPT_RELEASE
  make
  GIT_CRYPT_BIN=$WORKDIR/$GIT_CRYPT_RELEASE/git-crypt
}

#gpg --import ${TARBALL}.asc
#gpg --verify ${TARBALL}.asc $TARBALL

install_git-crypt

$GIT_CRYPT_BIN

#echo "$GPG_KEY" > private.key
#echo "$GPG_PASSPHRASE" | gpg --batch --yes --passphrase-fd 0 --import private.key
#rm private.key
#gpg --list-secret-keys --keyid-format LONG

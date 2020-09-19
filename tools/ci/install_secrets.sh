#!/usr/bin/env bash

set -ex

WORKDIR=$(mktemp -d)
APP=git-crypt-0.6.0
SOURCES=https://www.agwa.name/projects/git-crypt/downloads/$APP.tar.gz
TARBALL=$(basename $SOURCES)

cp tools/ci/${TARBALL}.asc "$WORKDIR"
cd $WORKDIR
wget $SOURCES
#gpg --import ${TARBALL}.asc
#gpg --verify ${TARBALL}.asc $TARBALL
tar  zxf $TARBALL
cd $APP
echo $PATH
make && make install PREFIX=/usr/local/bin

git-crypt

#echo "$GPG_KEY" > private.key
#echo "$GPG_PASSPHRASE" | gpg --batch --yes --passphrase-fd 0 --import private.key
#rm private.key
#gpg --list-secret-keys --keyid-format LONG

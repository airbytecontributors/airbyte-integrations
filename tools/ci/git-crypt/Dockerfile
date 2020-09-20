FROM alpine:3.12.0 AS build

ARG GIT_CRYPT_RELEASE=git-crypt-0.6.0

RUN apk --no-cache add ca-certificates curl git bash g++ gnupg make openssh openssl openssl-dev

RUN curl -L https://www.agwa.name/projects/git-crypt/downloads/$GIT_CRYPT_RELEASE.tar.gz | tar zxv -C /var/tmp

RUN cd /var/tmp/$GIT_CRYPT_RELEASE && make && make install PREFIX=/usr/local && rm -rf /var/tmp/$GIT_CRYPT_RELEASE

ENTRYPOINT ["git-crypt"]

LABEL io.airbyte.name=airbyte/ci_git-crypt
LABEL io.airbyte.version=1.0.0

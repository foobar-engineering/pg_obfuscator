# Warning! Build this image from repo root dir
# Build command: docker build --no-cache -t pg_obfuscator .
FROM yandex/clickhouse-client:20.3.18 AS clickhouse-obfuscator
FROM ruby:2.7-alpine
ENV APP_PATH="/opt/pg_obfuscator" \
    BUNDLER_VERSION="2.2.9" \
    GEM_VERSION="3.2.7" \
    GLIBC_VERSION="2.32-r0"

COPY --from=clickhouse-obfuscator /usr/bin/clickhouse-obfuscator /usr/bin/clickhouse-obfuscator
COPY ./ ${APP_PATH}/

RUN cd ${APP_PATH} && \
    apk add --update --no-cache build-base postgresql-dev postgresql-client gnupg ca-certificates && \
    bundle install --jobs `grep -c ^processor /proc/cpuinfo`

RUN wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub && \
    wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/${GLIBC_VERSION}/glibc-${GLIBC_VERSION}.apk && \
    apk add glibc-${GLIBC_VERSION}.apk && \
    rm glibc-${GLIBC_VERSION}.apk

WORKDIR ${APP_PATH}

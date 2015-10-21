# syntax=docker/dockerfile:1
#
# RushDB server image.
#
#   docker build -t rushdb .                      # build the binaries from source
#   docker buildx build --build-arg BINARIES=prebuilt ...  # release: package verified
#                                                  # binaries from bin/<arch>/ in the context
#
# Run (a token is required: the server refuses the default token off loopback):
#
#   docker run -d -p 127.0.0.1:3000:3000 -v rushdb-data:/data \
#     -e RUSHDB_TOKEN=change-me rushdb/rushdb

ARG BINARIES=source

# The data directory, owned by the runtime's non-root user (uid 65532).
FROM --platform=$BUILDPLATFORM busybox:1.37 AS data
RUN mkdir -p /data && chown 65532:65532 /data

FROM rust:1.98.1-bookworm AS source
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p rushdb-server --bin rushdb \
    && mkdir /out \
    && cp target/release/rushdb /out/

FROM scratch AS prebuilt
ARG TARGETARCH
COPY bin/${TARGETARCH}/rushdb /out/

FROM ${BINARIES} AS binaries

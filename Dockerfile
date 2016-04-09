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

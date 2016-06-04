#!/usr/bin/env bash
# Run install.sh the way users do (`curl ... | sh`) inside clean Linux
# containers against a locally served release directory, then start the
# installed database and write a record through it.
#
#   scripts/test-install.sh RELEASE_DIR [IMAGE...]
#
# RELEASE_DIR holds rushdb-<target>.tar.gz and its .sha256 for this machine's
# architecture. Images default to supported glibc distributions plus two that
# must be refused with a clear message (musl, and a glibc older than 2.28).
set -euo pipefail

RELEASE_DIR="$(cd "${1:?usage: test-install.sh RELEASE_DIR [IMAGE...]}" && pwd)"
shift
IMAGES=("$@")
if [ ${#IMAGES[@]} -eq 0 ]; then
  IMAGES=(debian:12 ubuntu:20.04 rockylinux:8 amazonlinux:2023 alpine:3 centos:7)
fi
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

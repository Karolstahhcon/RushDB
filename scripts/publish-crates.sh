#!/usr/bin/env sh
# Publish the crates that make up the standalone RushDB distribution.
#
# Use --dry-run during archive qualification. The package verifier resolves
# interdependent crates from the clean consumer's available registry; a first
# release therefore requires synchronized versioning/publication of the full
# internal crate set before consumer verification can pass. --publish
# intentionally requires a separate workflow_dispatch run with the
# CARGO_REGISTRY_TOKEN secret.
set -eu

mode="${1:---dry-run}"
case "$mode" in

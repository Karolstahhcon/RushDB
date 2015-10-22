# Quality gate for RushDB. Run `make gate` before every pull request.
#
# The workspace lint levels live in the root `Cargo.toml` under
# `[workspace.lints]` (`missing_docs` + `clippy::pedantic` are denied) and are
# opted into per crate via `[lints] workspace = true`. `make lint` therefore
# enforces them without extra flags; the `-D warnings` here additionally denies
# default `clippy::all` warnings.

CARGO ?= cargo

.PHONY: gate fmt fmt-check lint test test-doc clean

## Format, lint, and test the whole workspace.
gate: fmt-check lint test

## Apply rustfmt.
fmt:
	$(CARGO) fmt --all

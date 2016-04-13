#!/usr/bin/env python3
"""Convert the ann-benchmarks SIFT1M HDF5 to Rust-loadable fixtures.

Outputs (in the same directory as the input hdf5):
  base.fvecs       : 1M x 128-dim float32 base vectors
  query.fvecs      : 10k x 128-dim float32 query vectors
  neighbors.ivecs  : ground-truth: 10k rows x 100 int32 nearest-neighbor ids

This is the classic large unfiltered recall gate (no payload metadata; the
benchmark uses its own synthetic categories for the filtered experiment).
"""

import sys
import struct
import h5py


def write_fvecs(rows, path: str) -> None:
    with open(path, "wb") as f:
        for r in rows:
            f.write(struct.pack("<I", r.shape[0]))
            f.write(r.astype("<f4").tobytes())


def write_ivecs(rows, path: str) -> None:

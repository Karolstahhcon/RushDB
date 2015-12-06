#!/usr/bin/env python3
"""Convert the Qdrant DBPedia-100K parquet shards to a Rust-loadable fixture.

Outputs:
  vectors.fvecs : uint32 dim + float32 rows (1536-dim OpenAI embeddings)
  payload.csv   : one row per vector: _id,title,category-ish (title text)

The filter predicate for calibration uses the source record identity and a
synthetic category derived from the title (see plan doc), so the benchmark can
build correlated/uncorrelated filters over a real embedding corpus.
"""

import sys
import struct
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

VEC_COL = "text-embedding-3-large-1536-embedding"
N_DIM = 1536

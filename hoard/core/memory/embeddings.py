from __future__ import annotations

from array import array
from typing import Iterable


def _as_float_list(vector: Iterable[float]) -> list[float]:
    return [float(x) for x in vector]


def encode_embedding(vector: Iterable[float], expected_dim: int) -> bytes:
    try:
        import numpy as np

        arr = np.asarray(list(vector), dtype=np.float32)
        if arr.shape[0] != expected_dim:
            raise ValueError(f"Expected {expected_dim} dims, got {arr.shape[0]}")
        norm = float(np.linalg.norm(arr))
        if norm > 0:
            arr = arr / norm
        return arr.astype("<f4").tobytes()
    except Exception:
        vals = _as_float_list(vector)
        if len(vals) != expected_dim:
            raise ValueError(f"Expected {expected_dim} dims, got {len(vals)}")
        norm_sq = sum(v * v for v in vals)
        norm = norm_sq ** 0.5
        if norm > 0:
            vals = [v / norm for v in vals]
        arr = array("f", vals)
        return arr.tobytes()


def decode_embedding(blob: bytes, expected_dim: int) -> list[float]:
    try:
        import numpy as np

        arr = np.frombuffer(blob, dtype="<f4")
        if arr.shape[0] != expected_dim:
            raise ValueError(f"Expected {expected_dim} dims, got {arr.shape[0]}")
        return arr.astype(float).tolist()
    except Exception:
        arr = array("f")
        arr.frombytes(blob)
        if len(arr) != expected_dim:
            raise ValueError(f"Expected {expected_dim} dims, got {len(arr)}")
        return [float(x) for x in arr]


def validate_embedding_blob(blob: bytes, expected_dim: int) -> bool:
    if len(blob) != expected_dim * 4:
        return False
    try:
        vector = decode_embedding(blob, expected_dim)
    except Exception:
        return False
    norm_sq = sum(v * v for v in vector)
    norm = norm_sq ** 0.5
    return 0.99 < norm < 1.01

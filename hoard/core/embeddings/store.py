from __future__ import annotations

from array import array
import sqlite3
from typing import Iterable, List, Optional, Tuple

from hoard.core.embeddings.model import EmbeddingModel


def serialize_vector(vector: List[float]) -> bytes:
    arr = array("f", vector)
    return arr.tobytes()


def deserialize_vector(blob: bytes) -> array:
    arr = array("f")
    arr.frombytes(blob)
    return arr


def upsert_embedding(conn, chunk_id: str, model: str, vector: List[float], dims: int) -> None:
    conn.execute(
        """
        INSERT INTO embeddings (chunk_id, model, vector, dims)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(chunk_id) DO UPDATE SET
            model = excluded.model,
            vector = excluded.vector,
            dims = excluded.dims,
            created_at = CURRENT_TIMESTAMP
        """,
        (chunk_id, model, serialize_vector(vector), dims),
    )


def build_embeddings(
    conn,
    model: EmbeddingModel,
    batch_size: int = 32,
    source: Optional[str] = None,
) -> int:
    total = 0
    for batch in _iter_missing_chunks(conn, model.model_name, batch_size, source):
        chunk_ids = [row["id"] for row in batch]
        contents = [row["content"] for row in batch]
        vectors = model.encode(contents, batch_size=batch_size)

        for chunk_id, vector in zip(chunk_ids, vectors):
            upsert_embedding(conn, chunk_id, model.model_name, vector, model.dims)
            total += 1

        conn.commit()
        _mark_ann_stale(conn, model.model_name)

    return total


def _iter_missing_chunks(
    conn,
    model_name: str,
    batch_size: int,
    source: Optional[str],
) -> Iterable[List[dict]]:
    while True:
        params: List[str] = [model_name]
        source_filter = ""
        if source:
            source_filter = "AND entities.source = ?"
            params.append(source)

        rows = conn.execute(
            f"""
            SELECT chunks.id, chunks.content
            FROM chunks
            JOIN entities ON entities.id = chunks.entity_id
            LEFT JOIN embeddings
                ON embeddings.chunk_id = chunks.id
               AND embeddings.model = ?
            WHERE embeddings.chunk_id IS NULL
              AND entities.tombstoned_at IS NULL
              {source_filter}
            LIMIT ?
            """,
            (*params, batch_size),
        ).fetchall()

        if not rows:
            break

        yield rows


def _mark_ann_stale(conn, model_name: str) -> None:
    try:
        count_row = conn.execute(
            "SELECT COUNT(*) AS total FROM embeddings WHERE model = ?",
            (model_name,),
        ).fetchone()
        vectors_count = int(count_row["total"]) if count_row else 0
        conn.execute(
            """
            INSERT INTO ann_index_meta (id, backend, model_name, vectors_count, updated_at, state)
            VALUES (1, 'hnsw', ?, ?, datetime('now'), 'stale')
            ON CONFLICT(id) DO UPDATE SET
                model_name = excluded.model_name,
                vectors_count = excluded.vectors_count,
                updated_at = excluded.updated_at,
                state = 'stale'
            """,
            (model_name, vectors_count),
        )
        conn.commit()
    except sqlite3.OperationalError:
        # Table is available only after migration 008.
        return

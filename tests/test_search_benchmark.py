from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("pytest_benchmark")

from hoard.core.db.connection import connect, initialize_db
from hoard.core.search.service import search_entities


def test_search_benchmark(benchmark, tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)

    entities = []
    chunks = []
    total_entities = 500
    chunks_per_entity = 4

    for idx in range(total_entities):
        entity_id = f"entity-{idx}"
        entities.append(
            (
                entity_id,
                "local_files",
                f"/path/{idx}.md",
                "document",
                f"Doc {idx}",
            )
        )
        for cidx in range(chunks_per_entity):
            chunk_id = f"{entity_id}:{cidx}"
            content = f"Hoard benchmark content {idx} {cidx}"
            chunks.append(
                (
                    chunk_id,
                    entity_id,
                    cidx,
                    content,
                    f"hash-{idx}-{cidx}",
                )
            )

    conn.executemany(
        """
        INSERT INTO entities (id, source, source_id, entity_type, title)
        VALUES (?, ?, ?, ?, ?)
        """,
        entities,
    )
    conn.executemany(
        """
        INSERT INTO chunks (id, entity_id, chunk_index, content, content_hash)
        VALUES (?, ?, ?, ?, ?)
        """,
        chunks,
    )
    conn.commit()

    config = {"vectors": {"enabled": False}, "search": {}}

    def _run():
        search_entities(conn, query="Hoard benchmark", config=config, limit=5)

    benchmark(_run)
    conn.close()

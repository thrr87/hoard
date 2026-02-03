from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from hoard.connectors.notion_export.connector import NotionExportConnector
from hoard.sdk.types import EntityInput


@dataclass
class ExportStats:
    export_path: Path
    total_entities: int
    extracted_ids: int
    missing_ids: int
    duplicate_ids: int
    database_entities: int
    database_with_csv: int
    schema_fields: int
    total_chunks: int
    avg_chunks_per_entity: float
    avg_tokens_per_chunk: float


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit Notion export connector output.")
    parser.add_argument("--export", required=True, help="Path to Notion export ZIP or folder")
    parser.add_argument("--compare", help="Optional second export to compare IDs")
    parser.add_argument("--sample", type=int, default=5, help="Sample size for database titles")
    args = parser.parse_args()

    stats_a, ids_a, db_titles_a, schema_a = collect_stats(Path(args.export))
    print_stats("Export A", stats_a, db_titles_a, args.sample, schema_a)

    if args.compare:
        stats_b, ids_b, db_titles_b, schema_b = collect_stats(Path(args.compare))
        print_stats("Export B", stats_b, db_titles_b, args.sample, schema_b)
        compare_ids(ids_a, ids_b)


def collect_stats(export_path: Path) -> Tuple[ExportStats, List[str], List[str], List[str]]:
    connector = NotionExportConnector()
    config = {
        "export_path": str(export_path),
        "include_databases": True,
        "include_csv_databases": True,
        "chunk_max_tokens": 400,
        "chunk_overlap_tokens": 50,
    }

    ids: List[str] = []
    db_titles: List[str] = []
    db_with_csv = 0
    schema_fields = 0
    schema_examples: List[str] = []
    total_chunks = 0
    token_counts: List[int] = []

    for entity, chunks in connector.scan(config):
        ids.append(entity.source_id)
        total_chunks += len(chunks)
        token_counts.extend(len(chunk.content.split()) for chunk in chunks)

        meta = (entity.metadata or {}).get("notion", {})
        if meta.get("is_database"):
            db_titles.append(entity.title)
            if meta.get("has_csv"):
                db_with_csv += 1
            schema = meta.get("schema") or []
            schema_fields += len(schema)
            if schema and not schema_examples:
                schema_examples = [
                    f"{col.get('name')}: {col.get('type')}" for col in schema[:5]
                ]

    id_counter = Counter(ids)
    extracted_ids = len([value for value in ids if _looks_like_id(value)])
    missing_ids = len(ids) - extracted_ids
    duplicate_ids = len([value for value, count in id_counter.items() if count > 1])

    avg_chunks = total_chunks / len(ids) if ids else 0.0
    avg_tokens = sum(token_counts) / len(token_counts) if token_counts else 0.0

    stats = ExportStats(
        export_path=export_path,
        total_entities=len(ids),
        extracted_ids=extracted_ids,
        missing_ids=missing_ids,
        duplicate_ids=duplicate_ids,
        database_entities=len(db_titles),
        database_with_csv=db_with_csv,
        schema_fields=schema_fields,
        total_chunks=total_chunks,
        avg_chunks_per_entity=avg_chunks,
        avg_tokens_per_chunk=avg_tokens,
    )
    return stats, ids, db_titles, schema_examples


def compare_ids(ids_a: List[str], ids_b: List[str]) -> None:
    set_a = set(ids_a)
    set_b = set(ids_b)
    common = set_a & set_b
    only_a = set_a - set_b
    only_b = set_b - set_a

    print("\nID Stability Comparison")
    print(f"Common IDs: {len(common)}")
    print(f"Only in A: {len(only_a)}")
    print(f"Only in B: {len(only_b)}")


def print_stats(label: str, stats: ExportStats, db_titles: List[str], sample: int, schema_examples: List[str]) -> None:
    print(f"\n{label} ({stats.export_path})")
    print(f"Entities: {stats.total_entities}")
    print(f"Extracted IDs: {stats.extracted_ids}")
    print(f"Missing IDs: {stats.missing_ids}")
    print(f"Duplicate IDs: {stats.duplicate_ids}")
    print(f"Database entities: {stats.database_entities}")
    print(f"Database entities with CSV: {stats.database_with_csv}")
    if stats.schema_fields:
        print(f"Schema fields (total): {stats.schema_fields}")
    print(f"Chunks: {stats.total_chunks}")
    print(f"Avg chunks/entity: {stats.avg_chunks_per_entity:.2f}")
    print(f"Avg tokens/chunk: {stats.avg_tokens_per_chunk:.2f}")

    if db_titles:
        print("Sample database titles:")
        for title in db_titles[:sample]:
            print(f"- {title}")
    if schema_examples:
        print("Sample schema fields:")
        for entry in schema_examples:
            print(f"- {entry}")


def _looks_like_id(value: str) -> bool:
    return len(value) == 32 and all(c in "0123456789abcdef" for c in value.lower())


if __name__ == "__main__":
    main()

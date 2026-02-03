from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterator, List, Tuple

from hoard.sdk.base import ConnectorV1
from hoard.sdk.chunking import chunk_plain_text
from hoard.sdk.hash import compute_content_hash
from hoard.sdk.types import ChunkInput, DiscoverResult, EntityInput


class LocalFilesConnector(ConnectorV1):
    @property
    def name(self) -> str:
        return "local_files"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def source_name(self) -> str:
        return "local_files"

    def discover(self, config: dict) -> DiscoverResult:
        paths = _normalize_paths(config.get("paths", []))
        if not paths:
            return DiscoverResult(success=False, message="No paths configured")

        missing = [str(p) for p in paths if not p.exists()]
        if missing:
            return DiscoverResult(
                success=False,
                message=f"Missing paths: {', '.join(missing)}",
            )

        files = list(_iter_files(paths, config))
        return DiscoverResult(
            success=True,
            message=f"Found {len(files)} files",
            entity_count_estimate=len(files),
        )

    def scan(self, config: dict) -> Iterator[Tuple[EntityInput, List[ChunkInput]]]:
        paths = _normalize_paths(config.get("paths", []))
        include_exts = _normalize_extensions(config.get("include_extensions"))
        max_tokens = int(config.get("chunk_max_tokens", 400))
        overlap_tokens = int(config.get("chunk_overlap_tokens", 50))

        for file_path in _iter_files(paths, config):
            if include_exts and file_path.suffix.lower() not in include_exts:
                continue

            try:
                resolved_path = file_path.resolve()
                content = resolved_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue

            stat = resolved_path.stat()
            created_at = datetime.utcfromtimestamp(stat.st_ctime)
            updated_at = datetime.utcfromtimestamp(stat.st_mtime)

            entity = EntityInput(
                source=self.source_name,
                source_id=str(resolved_path),
                entity_type="document",
                title=resolved_path.name,
                uri=resolved_path.as_uri(),
                tags=[resolved_path.suffix.lower().lstrip(".")] if resolved_path.suffix else None,
                content_hash=compute_content_hash(content),
                mime_type=None,
                created_at=created_at,
                updated_at=updated_at,
                connector_name=self.name,
                connector_version=self.version,
            )

            chunks = [
                ChunkInput(
                    content=span.text,
                    char_offset_start=span.start,
                    char_offset_end=span.end,
                )
                for span in chunk_plain_text(
                    content,
                    max_tokens=max_tokens,
                    overlap_tokens=overlap_tokens,
                )
            ]

            yield entity, chunks


def _normalize_paths(paths: List[str]) -> List[Path]:
    normalized: List[Path] = []
    for path in paths:
        expanded = Path(path).expanduser()
        normalized.append(expanded)
    return normalized


def _normalize_extensions(exts: List[str] | None) -> set[str]:
    if not exts:
        return set()
    return {ext.lower() for ext in exts}


def _iter_files(paths: List[Path], config: dict) -> Iterator[Path]:
    include_exts = _normalize_extensions(config.get("include_extensions"))
    for base in paths:
        if base.is_file():
            if not include_exts or base.suffix.lower() in include_exts:
                yield base
            continue

        if not base.exists():
            continue

        for path in base.rglob("*"):
            if path.is_file():
                if include_exts and path.suffix.lower() not in include_exts:
                    continue
                yield path

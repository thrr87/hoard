from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterator, List, Tuple

from hoard.sdk.base import ConnectorV1
from hoard.sdk.chunking import chunk_plain_text
from hoard.sdk.hash import compute_content_hash
from hoard.sdk.types import ChunkInput, DiscoverResult, EntityInput


class InboxConnector(ConnectorV1):
    @property
    def name(self) -> str:
        return "inbox"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def source_name(self) -> str:
        return "inbox"

    def discover(self, config: dict) -> DiscoverResult:
        inbox_path = _resolve_inbox_path(config)
        if not inbox_path:
            return DiscoverResult(success=False, message="Inbox path not configured")
        if not inbox_path.exists():
            return DiscoverResult(success=False, message=f"Inbox path not found: {inbox_path}")

        files = list(_iter_files(inbox_path, config))
        return DiscoverResult(
            success=True,
            message=f"Found {len(files)} inbox files",
            entity_count_estimate=len(files),
        )

    def scan(self, config: dict) -> Iterator[Tuple[EntityInput, List[ChunkInput]]]:
        inbox_path = _resolve_inbox_path(config)
        if not inbox_path or not inbox_path.exists():
            return

        max_tokens = int(config.get("chunk_max_tokens", 400))
        overlap_tokens = int(config.get("chunk_overlap_tokens", 50))

        for file_path in _iter_files(inbox_path, config):
            try:
                resolved_path = file_path.resolve()
                content = resolved_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue

            stat = resolved_path.stat()
            created_at = datetime.utcfromtimestamp(stat.st_ctime)
            updated_at = datetime.utcfromtimestamp(stat.st_mtime)

            tags = ["inbox", "agent"]
            if resolved_path.suffix:
                tags.append(resolved_path.suffix.lower().lstrip("."))

            entity = EntityInput(
                source=self.source_name,
                source_id=str(resolved_path),
                entity_type="document",
                title=resolved_path.name,
                uri=resolved_path.as_uri(),
                tags=tags,
                content_hash=compute_content_hash(content),
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


def _resolve_inbox_path(config: dict) -> Path | None:
    raw = config.get("path") or config.get("inbox_path")
    if not raw:
        return None
    return Path(raw).expanduser()


def _normalize_extensions(exts: List[str] | None) -> set[str]:
    if not exts:
        return set()
    return {ext.lower() for ext in exts}


def _iter_files(base: Path, config: dict) -> Iterator[Path]:
    include_exts = _normalize_extensions(config.get("include_extensions"))

    if base.is_file():
        if not include_exts or base.suffix.lower() in include_exts:
            yield base
        return

    for path in base.rglob("*"):
        if not path.is_file():
            continue
        if include_exts and path.suffix.lower() not in include_exts:
            continue
        yield path

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterator, List, Tuple

from hoard.sdk.base import ConnectorV1
from hoard.sdk.chunking import chunk_plain_text
from hoard.sdk.hash import compute_content_hash
from hoard.sdk.types import ChunkInput, DiscoverResult, EntityInput


class ObsidianConnector(ConnectorV1):
    @property
    def name(self) -> str:
        return "obsidian"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def source_name(self) -> str:
        return "obsidian"

    def discover(self, config: dict) -> DiscoverResult:
        vault_path = Path(config.get("vault_path", "")).expanduser()
        if not vault_path.exists():
            return DiscoverResult(success=False, message=f"Vault not found: {vault_path}")

        files = list(_iter_markdown(vault_path))
        return DiscoverResult(
            success=True,
            message=f"Found {len(files)} notes",
            entity_count_estimate=len(files),
        )

    def scan(self, config: dict) -> Iterator[Tuple[EntityInput, List[ChunkInput]]]:
        vault_path = Path(config.get("vault_path", "")).expanduser()
        max_tokens = int(config.get("chunk_max_tokens", 400))
        overlap_tokens = int(config.get("chunk_overlap_tokens", 50))

        vault_name = vault_path.name

        for file_path in _iter_markdown(vault_path):
            try:
                resolved_path = file_path.resolve()
                content = resolved_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue

            rel_path = resolved_path.relative_to(vault_path).as_posix()
            source_id = f"{vault_name}:{rel_path}"

            stat = resolved_path.stat()
            created_at = datetime.utcfromtimestamp(stat.st_ctime)
            updated_at = datetime.utcfromtimestamp(stat.st_mtime)

            entity = EntityInput(
                source=self.source_name,
                source_id=source_id,
                entity_type="note",
                title=resolved_path.stem,
                uri=resolved_path.as_uri(),
                tags=["obsidian"],
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


def _iter_markdown(vault_path: Path) -> Iterator[Path]:
    if vault_path.is_file():
        if vault_path.suffix.lower() == ".md":
            yield vault_path
        return

    for path in vault_path.rglob("*"):
        if not path.is_file():
            continue
        if ".obsidian" in path.parts:
            continue
        if path.suffix.lower() != ".md":
            continue
        yield path

from __future__ import annotations

import re
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from io import StringIO
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

from hoard.sdk.base import ConnectorV1
from hoard.sdk.chunking import chunk_plain_text
from hoard.sdk.hash import compute_content_hash
from hoard.sdk.types import ChunkInput, DiscoverResult, EntityInput

import csv
from collections import Counter


@dataclass
class ExportContext:
    root: Path
    temp_dir: tempfile.TemporaryDirectory | None = None


class NotionExportConnector(ConnectorV1):
    def __init__(self) -> None:
        self._temp_dir: tempfile.TemporaryDirectory | None = None

    @property
    def name(self) -> str:
        return "notion_export"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def source_name(self) -> str:
        return "notion_export"

    def discover(self, config: dict) -> DiscoverResult:
        export_path = _resolve_export_path(config)
        if not export_path.exists():
            return DiscoverResult(success=False, message=f"Export not found: {export_path}")

        context = _prepare_export(export_path)
        try:
            html_files = list(_iter_export_files(context.root, include_csv=True))
        finally:
            _cleanup_context(context)

        return DiscoverResult(
            success=True,
            message=f"Found {len(html_files)} HTML/MD files",
            entity_count_estimate=len(html_files),
        )

    def scan(self, config: dict) -> Iterator[Tuple[EntityInput, List[ChunkInput]]]:
        export_path = _resolve_export_path(config)
        include_databases = bool(config.get("include_databases", True))
        include_csv = bool(config.get("include_csv_databases", True))
        schema_sample_rows = int(config.get("schema_sample_rows", 200))
        max_schema_tags = int(config.get("max_schema_tags", 200))
        max_tokens = int(config.get("chunk_max_tokens", 400))
        overlap_tokens = int(config.get("chunk_overlap_tokens", 50))

        context = _prepare_export(export_path)
        self._temp_dir = context.temp_dir

        try:
            for file_path in _iter_export_files(context.root, include_csv=include_csv):
                try:
                    resolved_path = file_path.resolve()
                    content = resolved_path.read_text(encoding="utf-8", errors="replace")
                except Exception:
                    continue

                notion_id = _extract_notion_id(resolved_path.name)
                title = _extract_title(resolved_path, content, notion_id)
                text = _extract_text(resolved_path, content)
                schema = None
                if resolved_path.suffix.lower() == ".csv":
                    schema = _infer_csv_schema(content, sample_rows=schema_sample_rows)

                has_csv = _has_csv_sibling(resolved_path)
                is_database = has_csv or _looks_like_database(content) or resolved_path.suffix.lower() == ".csv"

                if is_database and not include_databases:
                    continue

                rel_path = resolved_path.relative_to(context.root).as_posix() if context.root in resolved_path.parents or resolved_path == context.root else resolved_path.name
                source_id = notion_id or rel_path

                stat = resolved_path.stat()
                created_at = datetime.utcfromtimestamp(stat.st_ctime)
                updated_at = datetime.utcfromtimestamp(stat.st_mtime)

                tags = ["notion"]
                if is_database:
                    tags.append("database")
                if resolved_path.suffix.lower() == ".csv":
                    tags.append("csv")
                    if schema:
                        field_tags = _schema_tags(schema, max_schema_tags=max_schema_tags)
                        tags.extend(field_tags)

                entity = EntityInput(
                    source=self.source_name,
                    source_id=source_id,
                    entity_type="database" if is_database else "page",
                    title=title,
                    uri=resolved_path.as_uri(),
                    tags=tags,
                    metadata={
                        "notion": {
                            "id": notion_id,
                            "is_database": is_database,
                            "has_csv": has_csv or resolved_path.suffix.lower() == ".csv",
                            "path": rel_path,
                            "schema": schema,
                        }
                    },
                    content_hash=compute_content_hash(text),
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
                        text,
                        max_tokens=max_tokens,
                        overlap_tokens=overlap_tokens,
                    )
                ]

                yield entity, chunks
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        if self._temp_dir:
            self._temp_dir.cleanup()
            self._temp_dir = None


def _resolve_export_path(config: dict) -> Path:
    raw_path = config.get("export_path", "")
    return Path(raw_path).expanduser()


def _prepare_export(path: Path) -> ExportContext:
    if path.is_file() and path.suffix.lower() == ".zip":
        temp_dir = tempfile.TemporaryDirectory(prefix="hoard-notion-")
        with zipfile.ZipFile(path, "r") as zf:
            zf.extractall(temp_dir.name)
        return ExportContext(root=Path(temp_dir.name), temp_dir=temp_dir)
    return ExportContext(root=path)


def _cleanup_context(context: ExportContext) -> None:
    if context.temp_dir:
        context.temp_dir.cleanup()


def _iter_export_files(root: Path, include_csv: bool) -> Iterator[Path]:
    if root.is_file():
        if root.suffix.lower() in {".html", ".md"} or (include_csv and root.suffix.lower() == ".csv"):
            yield root
        return

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.name.lower() in {"index.html", "index.md"}:
            continue
        if path.suffix.lower() in {".html", ".md"} or (include_csv and path.suffix.lower() == ".csv"):
            yield path


def _extract_text(path: Path, content: str) -> str:
    if path.suffix.lower() in {".md", ".csv"}:
        return content
    parser = _HTMLTextExtractor()
    parser.feed(content)
    return parser.text()


def _extract_title(path: Path, content: str, notion_id: Optional[str]) -> str:
    title_match = re.search(r"<title>(.*?)</title>", content, re.IGNORECASE | re.DOTALL)
    if title_match:
        return unescape(title_match.group(1)).strip()

    stem = path.stem
    if notion_id:
        stem = _strip_id_suffix(stem, notion_id)
    return stem.strip() or path.name


def _strip_id_suffix(name: str, notion_id: str) -> str:
    normalized = notion_id.lower()
    dashed = _insert_dashes(normalized)
    pattern = re.compile(rf"[\s_-]*({re.escape(normalized)}|{re.escape(dashed)})$", re.IGNORECASE)
    return pattern.sub("", name).strip()


def _insert_dashes(notion_id: str) -> str:
    if len(notion_id) != 32:
        return notion_id
    return f"{notion_id[:8]}-{notion_id[8:12]}-{notion_id[12:16]}-{notion_id[16:20]}-{notion_id[20:]}"


def _extract_notion_id(name: str) -> Optional[str]:
    uuid_match = re.search(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", name)
    if uuid_match:
        return uuid_match.group(0).replace("-", "").lower()

    hex_match = re.search(r"[0-9a-fA-F]{32}", name)
    if hex_match:
        return hex_match.group(0).lower()
    return None


def _has_csv_sibling(path: Path) -> bool:
    if path.suffix.lower() == ".csv":
        return True
    csv_path = path.with_suffix(".csv")
    return csv_path.exists()


def _looks_like_database(content: str) -> bool:
    lowered = content.lower()
    if "collection" in lowered and "table" in lowered:
        return True
    if "data-collection-id" in lowered:
        return True
    return False


def _infer_csv_schema(content: str, sample_rows: int = 200) -> List[dict]:
    reader = csv.reader(StringIO(content))
    try:
        header = next(reader)
    except StopIteration:
        return []

    columns = [
        _clean_column_name(value) or f"column_{idx + 1}"
        for idx, value in enumerate(header)
    ]
    samples: List[List[str]] = [[] for _ in columns]
    type_counts: List[dict] = [Counter() for _ in columns]

    row_count = 0
    for row in reader:
        if sample_rows and row_count >= sample_rows:
            break
        row_count += 1
        for idx, value in enumerate(row[: len(columns)]):
            value = value.strip()
            if value:
                if len(samples[idx]) < 3 and value not in samples[idx]:
                    samples[idx].append(value)
                value_type = _classify_value(value)
                type_counts[idx][value_type] = type_counts[idx].get(value_type, 0) + 1

    schema: List[dict] = []
    for idx, name in enumerate(columns):
        inferred_type = _resolve_type(type_counts[idx])
        schema.append({"name": name, "type": inferred_type, "examples": samples[idx]})
    return schema


def _classify_value(value: str) -> str:
    lower = value.lower()
    if lower in {"true", "false", "yes", "no", "y", "n"}:
        return "boolean"

    numeric = value.replace(",", "")
    if re.fullmatch(r"[+-]?\d+", numeric):
        return "integer"
    if re.fullmatch(r"[+-]?(\d+\.\d*|\d*\.\d+)", numeric):
        return "float"

    iso_value = value.replace("Z", "+00:00")
    try:
        if "t" in lower or ":" in value:
            datetime.fromisoformat(iso_value)
            return "datetime"
        datetime.fromisoformat(iso_value)
        return "date"
    except ValueError:
        return "string"


def _resolve_type(type_counts: dict) -> str:
    if not type_counts:
        return "empty"
    if type_counts.get("string"):
        return "string"
    if type_counts.get("datetime"):
        return "datetime"
    if type_counts.get("date"):
        return "date"
    if type_counts.get("float"):
        return "float"
    if type_counts.get("integer"):
        return "integer"
    if type_counts.get("boolean"):
        return "boolean"
    return "string"


def _schema_tags(schema: List[dict], max_schema_tags: int) -> List[str]:
    tags: List[str] = []
    limit = max_schema_tags if max_schema_tags > 0 else len(schema)
    for field in schema[:limit]:
        name = field.get("name") or ""
        tag = _normalize_field_tag(name)
        if tag:
            tags.append(tag)
    return tags


def _normalize_field_tag(name: str) -> str:
    cleaned = _clean_column_name(name)
    cleaned = re.sub(r"[^a-zA-Z0-9_\s-]", "", cleaned).strip().lower()
    cleaned = re.sub(r"[\s-]+", "_", cleaned)
    if not cleaned:
        return ""
    return f"field:{cleaned}"


def _clean_column_name(name: str) -> str:
    return name.replace("\ufeff", "").strip()


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._chunks: List[str] = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style"}:
            self._skip = True

    def handle_endtag(self, tag):
        if tag in {"script", "style"}:
            self._skip = False
        if tag in {"p", "br", "div", "li"}:
            self._chunks.append("\n")

    def handle_data(self, data):
        if self._skip:
            return
        text = data.strip()
        if text:
            self._chunks.append(text)

    def text(self) -> str:
        return " ".join(self._chunks).replace("\n ", "\n").strip()

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


@dataclass
class ConnectorPack:
    pack_path: Path | None
    pack_markdown: str | None
    config_markdown: str | None


@dataclass
class ConnectorManifest:
    path: Path
    data: Dict[str, Any]
    pack: ConnectorPack | None = None

    @property
    def name(self) -> str | None:
        return self.data.get("name")

    @property
    def version(self) -> str | None:
        return self.data.get("version")

    @property
    def entry_point(self) -> str | None:
        return self.data.get("entry_point")

    def to_dict(self) -> Dict[str, Any]:
        payload = {"path": str(self.path), "manifest": self.data}
        if self.pack:
            payload["pack"] = {
                "path": str(self.pack.pack_path) if self.pack.pack_path else None,
                "pack_markdown": self.pack.pack_markdown,
                "config_markdown": self.pack.config_markdown,
            }
        return payload


def load_manifest(path: Path) -> ConnectorManifest:
    manifest_path = _resolve_manifest_path(path)
    data = yaml.safe_load(manifest_path.read_text()) or {}
    pack = load_pack(manifest_path.parent)
    return ConnectorManifest(path=manifest_path, data=data, pack=pack)


def load_pack(directory: Path) -> ConnectorPack | None:
    pack_path = directory / "PACK.md"
    config_path = directory / "CONFIG.md"

    if not pack_path.exists() and not config_path.exists():
        return None

    pack_markdown = pack_path.read_text() if pack_path.exists() else None
    config_markdown = config_path.read_text() if config_path.exists() else None
    return ConnectorPack(pack_path=pack_path if pack_path.exists() else None,
                         pack_markdown=pack_markdown,
                         config_markdown=config_markdown)


def _resolve_manifest_path(path: Path) -> Path:
    if path.is_dir():
        for name in ("manifest.yaml", "manifest.yml"):
            candidate = path / name
            if candidate.exists():
                return candidate
        raise FileNotFoundError("manifest.yaml not found in directory")

    if path.name in {"manifest.yaml", "manifest.yml"}:
        return path

    raise FileNotFoundError("manifest.yaml not found")

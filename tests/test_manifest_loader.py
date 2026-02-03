from __future__ import annotations

from pathlib import Path

import yaml

from hoard.core.connectors.manifest import load_manifest


def test_manifest_loader_with_pack(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.yaml"
    pack_path = tmp_path / "PACK.md"
    config_path = tmp_path / "CONFIG.md"

    manifest_data = {
        "name": "demo",
        "version": "1.0.0",
        "entry_point": "connector:Demo",
    }

    manifest_path.write_text(yaml.safe_dump(manifest_data))
    pack_path.write_text("# Pack")
    config_path.write_text("# Config")

    manifest = load_manifest(tmp_path)
    assert manifest.name == "demo"
    assert manifest.pack is not None
    assert manifest.pack.pack_markdown == "# Pack"
    assert manifest.pack.config_markdown == "# Config"

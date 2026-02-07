from __future__ import annotations

import json
import re
from pathlib import Path

import yaml
from click.testing import CliRunner

from hoard.cli.main import cli
from hoard.core.config import save_config
from hoard.core.db.connection import connect, initialize_db
from hoard.core.memory.store import memory_put


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def test_cli_add_inbox_updates_config(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    inbox_dir = tmp_path / "inbox"
    inbox_dir.mkdir()

    runner = CliRunner()
    result = runner.invoke(cli, ["add", "--inbox", str(inbox_dir)])
    assert result.exit_code == 0

    config_path = home_dir / ".hoard" / "config.yaml"
    data = yaml.safe_load(config_path.read_text())
    assert data["connectors"]["inbox"]["enabled"] is True
    assert data["connectors"]["inbox"]["path"] == str(inbox_dir)


def test_cli_search_json_includes_memory(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "note.md").write_text("Hoard document content")

    db_path = tmp_path / "hoard.db"
    config_path = tmp_path / "config.yaml"
    config = {
        "storage": {"db_path": str(db_path)},
        "connectors": {
            "local_files": {
                "enabled": True,
                "paths": [str(data_dir)],
                "include_extensions": [".md"],
                "chunk_max_tokens": 50,
                "chunk_overlap_tokens": 0,
            }
        },
        "vectors": {"enabled": False},
        "search": {},
    }
    save_config(config, config_path)

    conn = connect(db_path)
    initialize_db(conn)
    memory_put(conn, key="context", content="Hoard memory entry")
    conn.commit()
    conn.close()

    runner = CliRunner()
    sync_result = runner.invoke(cli, ["sync", "--config", str(config_path)])
    assert sync_result.exit_code == 0

    result = runner.invoke(cli, ["search", "Hoard", "--config", str(config_path), "--json"])
    assert result.exit_code == 0

    payload = json.loads(_strip_ansi(result.output))
    result_types = {entry.get("result_type") for entry in payload.get("results", [])}
    assert "entity" in result_types
    assert "memory" in result_types


def test_cli_memory_prune(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    config_path = tmp_path / "config.yaml"
    config = {"storage": {"db_path": str(db_path)}}
    save_config(config, config_path)

    conn = connect(db_path)
    initialize_db(conn)
    memory_put(conn, key="expired", content="old", expires_at="2000-01-01T00:00:00")
    conn.commit()
    conn.close()

    runner = CliRunner()
    result = runner.invoke(cli, ["memory", "prune", "--config", str(config_path)])
    assert result.exit_code == 0
    assert "Pruned 1" in result.output

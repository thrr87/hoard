from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from hoard.cli import main as cli_main
from hoard.cli.main import cli


def test_setup_remote_with_token_writes_local_configs(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    monkeypatch.setattr(
        cli_main,
        "_remote_health",
        lambda base_url: {"status": "ok", "db_ready": True, "migrations_pending": False},
    )
    monkeypatch.setattr(cli_main, "_validate_remote_token", lambda mcp_url, token, label: None)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "setup",
            "remote",
            "--url",
            "https://example.com",
            "--token",
            "hoard_sk_test",
            "--claude",
            "--codex",
        ],
    )
    assert result.exit_code == 0
    assert "Recommended: --token" in result.output

    claude_config = home_dir / ".claude.json"
    codex_config = home_dir / ".codex" / "config.toml"
    assert claude_config.exists()
    assert codex_config.exists()

    claude_data = json.loads(claude_config.read_text())
    assert claude_data["mcpServers"]["hoard"]["url"] == "https://example.com/mcp"
    assert 'url = "https://example.com/mcp"' in codex_config.read_text()


def test_setup_remote_admin_token_dry_run_does_not_write_files(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    monkeypatch.setattr(
        cli_main,
        "_remote_health",
        lambda base_url: {"status": "ok", "db_ready": True, "migrations_pending": False},
    )
    monkeypatch.setattr(cli_main, "_validate_remote_token", lambda mcp_url, token, label: None)
    monkeypatch.setattr(cli_main, "_provision_remote_token", lambda mcp_url, admin_token, target: f"tok-{target}")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "setup",
            "remote",
            "--url",
            "https://example.com/mcp",
            "--admin-token",
            "admin",
            "--codex",
            "--openclaw",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "Provisioned token for codex." in result.output
    assert "Dry run: no files were changed." in result.output
    assert not (home_dir / ".codex" / "config.toml").exists()
    assert not (home_dir / ".openclaw" / "openclaw.json").exists()


def test_setup_remote_requires_credentials(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "setup",
            "remote",
            "--url",
            "https://example.com",
            "--codex",
        ],
    )
    assert result.exit_code != 0
    assert "Provide --token (recommended) or --admin-token (advanced)." in result.output


def test_openclaw_config_uses_base_url(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    monkeypatch.setattr(
        cli_main,
        "_remote_health",
        lambda base_url: {"status": "ok", "db_ready": True, "migrations_pending": False},
    )
    monkeypatch.setattr(cli_main, "_validate_remote_token", lambda mcp_url, token, label: None)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "setup",
            "remote",
            "--url",
            "https://example.com/mcp",
            "--token",
            "hoard_sk_test",
            "--openclaw",
        ],
    )
    assert result.exit_code == 0

    openclaw_config = home_dir / ".openclaw" / "openclaw.json"
    data = json.loads(openclaw_config.read_text())
    assert data["skills"]["entries"]["hoard"]["env"]["HOARD_URL"] == "https://example.com"

    script_path = home_dir / ".openclaw" / "skills" / "hoard" / "scripts" / "hoard_client.py"
    assert "_normalize_base_url(HOARD_URL)" in script_path.read_text()

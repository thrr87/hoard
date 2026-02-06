from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from hoard.cli import main as cli_main
from hoard.cli.main import cli
from hoard.core.config import load_config, save_config


def test_cli_init_quick_sets_local_files(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "note.md").write_text("Quick init note")

    monkeypatch.setattr(
        cli_main,
        "detect_document_folders",
        lambda exts: [(data_dir, 1)],
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--quick"])
    assert result.exit_code == 0

    config_path = home_dir / ".hoard" / "config.yaml"
    config = load_config(config_path)
    assert config["connectors"]["local_files"]["enabled"] is True
    assert str(data_dir) in config["connectors"]["local_files"]["paths"]


def test_cli_setup_writes_codex_and_claude_configs(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    monkeypatch.setattr(cli_main, "_ensure_server_running", lambda host, port: None)
    monkeypatch.setattr(cli_main, "_ensure_token", lambda config, name: "hoard_sk_test")

    runner = CliRunner()
    result = runner.invoke(cli, ["setup", "--codex", "--claude"])
    assert result.exit_code == 0
    assert "Setup Summary" in result.output
    assert "Next steps" in result.output

    codex_config = home_dir / ".codex" / "config.toml"
    assert codex_config.exists()
    assert "mcp_servers.hoard" in codex_config.read_text()

    claude_config = home_dir / ".claude.json"
    assert claude_config.exists()
    data = json.loads(claude_config.read_text())
    assert "hoard" in data.get("mcpServers", {})


def test_cli_setup_generates_server_secret(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    monkeypatch.setattr(cli_main, "_ensure_server_running", lambda host, port: None)
    monkeypatch.setattr(cli_main, "_ensure_token", lambda config, name: "hoard_sk_test")

    runner = CliRunner()
    result = runner.invoke(cli, ["setup", "--codex"])
    assert result.exit_code == 0
    assert "Generated server secret" in result.output
    assert (home_dir / ".hoard" / "server.key").exists()


def test_cli_setup_fails_when_server_unhealthy(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    monkeypatch.setattr(cli_main, "_serve_daemon", lambda host, port, no_migrate=False, allow_remote=False: None)
    monkeypatch.setattr(cli_main, "_is_server_healthy", lambda host, port: False)

    runner = CliRunner()
    result = runner.invoke(cli, ["setup", "--codex"])
    assert result.exit_code != 0
    assert "Hoard server failed to start." in result.output
    assert ".hoard/hoard.log" in result.output


def test_cli_setup_verify_output(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))
    monkeypatch.setenv("HOARD_SERVER_SECRET", "test-secret")

    config_path = home_dir / ".hoard" / "config.yaml"
    save_config({"security": {"tokens": [{"name": "default", "token": "t", "scopes": ["search"]}]}}, config_path)

    monkeypatch.setattr(cli_main, "_is_server_healthy", lambda host, port: True)
    monkeypatch.setattr(cli_main, "_check_tools_list", lambda host, port, token: True)
    monkeypatch.setattr(cli_main, "_check_write_smoke", lambda host, port, token: (True, ""))

    runner = CliRunner()
    result = runner.invoke(cli, ["setup", "--verify"])
    assert result.exit_code == 0
    assert "Tier 1" in result.output
    assert "Tier 2" in result.output
    assert "Write tools operational" in result.output


def test_cli_serve_blocks_remote_bind_without_opt_in(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    runner = CliRunner()
    result = runner.invoke(cli, ["serve", "--host", "0.0.0.0", "--daemon"])
    assert result.exit_code != 0
    assert "Refusing non-loopback bind" in result.output


def test_cli_serve_allows_remote_bind_with_flag(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    called: dict[str, object] = {}

    def _fake_daemon(host: str, port: int, no_migrate: bool = False, allow_remote: bool = False) -> None:
        called["host"] = host
        called["port"] = port
        called["allow_remote"] = allow_remote

    monkeypatch.setattr(cli_main, "_serve_daemon", _fake_daemon)

    runner = CliRunner()
    result = runner.invoke(cli, ["serve", "--host", "0.0.0.0", "--daemon", "--allow-remote"])
    assert result.exit_code == 0
    assert called["host"] == "0.0.0.0"
    assert called["allow_remote"] is True


def test_cli_serve_allows_remote_bind_via_config(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    config_path = home_dir / ".hoard" / "config.yaml"
    save_config({"server": {"allow_remote": True}}, config_path)

    called = {"count": 0}

    def _fake_daemon(host: str, port: int, no_migrate: bool = False, allow_remote: bool = False) -> None:
        called["count"] += 1
        called["allow_remote"] = allow_remote

    monkeypatch.setattr(cli_main, "_serve_daemon", _fake_daemon)

    runner = CliRunner()
    result = runner.invoke(cli, ["serve", "--host", "0.0.0.0", "--daemon"])
    assert result.exit_code == 0
    assert called["count"] == 1
    assert called["allow_remote"] is True


def test_cli_mcp_serve_blocks_remote_bind_without_opt_in(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    runner = CliRunner()
    result = runner.invoke(cli, ["mcp", "serve", "--host", "0.0.0.0", "--no-migrate"])
    assert result.exit_code != 0
    assert "Refusing non-loopback bind" in result.output

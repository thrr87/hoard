from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from hoard.cli import main as cli_main
from hoard.cli.main import cli


def test_cli_instructions_dry_run_writes_nothing(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / ".git").mkdir()
    monkeypatch.chdir(project_dir)

    runner = CliRunner()
    result = runner.invoke(cli, ["instructions", "--codex", "--dry-run"])

    assert result.exit_code == 0
    assert "Dry run complete" in result.output
    assert not (project_dir / "AGENTS.md").exists()


def test_cli_instructions_reject_confirmation(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / ".git").mkdir()
    monkeypatch.chdir(project_dir)

    runner = CliRunner()
    result = runner.invoke(cli, ["instructions", "--codex"], input="n\n")

    assert result.exit_code == 0
    assert "Instruction update canceled." in result.output
    assert not (project_dir / "AGENTS.md").exists()


def test_cli_instructions_yes_is_idempotent(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / ".git").mkdir()
    monkeypatch.chdir(project_dir)

    runner = CliRunner()
    first = runner.invoke(cli, ["instructions", "--codex", "--yes"])
    assert first.exit_code == 0

    agents_path = project_dir / "AGENTS.md"
    assert agents_path.exists()
    first_content = agents_path.read_text()
    assert "<!-- HOARD:START -->" in first_content
    assert "<!-- HOARD:END -->" in first_content

    second = runner.invoke(cli, ["instructions", "--codex", "--yes"])
    assert second.exit_code == 0
    assert "already up to date" in second.output
    assert agents_path.read_text() == first_content


def test_cli_instructions_requires_project_root(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    plain_dir = tmp_path / "plain"
    plain_dir.mkdir()
    monkeypatch.chdir(plain_dir)

    runner = CliRunner()
    result = runner.invoke(cli, ["instructions", "--codex", "--yes"])

    assert result.exit_code != 0
    assert "Could not determine project root" in result.output
    assert "--root" in result.output


def test_setup_applies_instructions_when_interactive(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / ".git").mkdir()
    monkeypatch.chdir(project_dir)

    monkeypatch.setattr(cli_main, "_ensure_server_running", lambda host, port: None)
    monkeypatch.setattr(cli_main, "_is_interactive_session", lambda: True)

    runner = CliRunner()
    result = runner.invoke(cli, ["setup", "--codex"], input="y\n")

    assert result.exit_code == 0
    assert (project_dir / "AGENTS.md").exists()
    assert "<!-- HOARD:START -->" in (project_dir / "AGENTS.md").read_text()


def test_setup_skips_instructions_with_flag(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / ".git").mkdir()
    monkeypatch.chdir(project_dir)

    monkeypatch.setattr(cli_main, "_ensure_server_running", lambda host, port: None)
    monkeypatch.setattr(cli_main, "_is_interactive_session", lambda: True)

    runner = CliRunner()
    result = runner.invoke(cli, ["setup", "--codex", "--no-instructions"])

    assert result.exit_code == 0
    assert not (project_dir / "AGENTS.md").exists()
    assert "Skipping instruction injection (--no-instructions)." in result.output


def test_setup_skips_instructions_when_non_interactive(tmp_path: Path, monkeypatch) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / ".git").mkdir()
    monkeypatch.chdir(project_dir)

    monkeypatch.setattr(cli_main, "_ensure_server_running", lambda host, port: None)
    monkeypatch.setattr(cli_main, "_is_interactive_session", lambda: False)

    runner = CliRunner()
    result = runner.invoke(cli, ["setup", "--codex"])

    assert result.exit_code == 0
    assert not (project_dir / "AGENTS.md").exists()
    assert "Skipping instruction injection in non-interactive mode." in result.output


def test_openclaw_client_script_contains_new_and_existing_commands() -> None:
    script = cli_main._openclaw_client_script()
    assert 'add_parser("search"' in script
    assert 'add_parser("get"' in script
    assert 'add_parser("memory_get"' in script
    assert 'add_parser("memory_put"' in script
    assert 'add_parser("sync"' in script
    assert 'add_parser("inbox_put"' in script

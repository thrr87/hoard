from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from hoard.cli.main import cli
from hoard.core.config import save_config
from hoard.core.db.connection import connect, initialize_db
from hoard.core.memory.store import memory_get, memory_put


def test_cli_db_backup_restore_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    config_path = tmp_path / "config.yaml"
    backup_path = tmp_path / "backup.db"
    save_config({"storage": {"db_path": str(db_path)}}, config_path)

    conn = connect(db_path)
    initialize_db(conn)
    memory_put(conn, key="before_backup", content="alpha")
    conn.commit()
    conn.close()

    runner = CliRunner()
    backup_result = runner.invoke(cli, ["db", "backup", str(backup_path), "--config", str(config_path)])
    assert backup_result.exit_code == 0
    assert backup_path.exists()
    assert Path(f"{backup_path}.sha256").exists()

    conn = connect(db_path)
    memory_put(conn, key="after_backup", content="beta")
    conn.commit()
    conn.close()

    restore_result = runner.invoke(
        cli,
        ["db", "restore", str(backup_path), "--force", "--config", str(config_path)],
    )
    assert restore_result.exit_code == 0

    conn = connect(db_path)
    assert memory_get(conn, "before_backup") is not None
    assert memory_get(conn, "after_backup") is None
    conn.close()


def test_cli_db_restore_rejects_checksum_mismatch(tmp_path: Path) -> None:
    db_path = tmp_path / "hoard.db"
    config_path = tmp_path / "config.yaml"
    backup_path = tmp_path / "backup.db"
    save_config({"storage": {"db_path": str(db_path)}}, config_path)

    conn = connect(db_path)
    initialize_db(conn)
    memory_put(conn, key="x", content="y")
    conn.commit()
    conn.close()

    runner = CliRunner()
    backup_result = runner.invoke(cli, ["db", "backup", str(backup_path), "--config", str(config_path)])
    assert backup_result.exit_code == 0

    with backup_path.open("ab") as handle:
        handle.write(b"tamper")

    restore_result = runner.invoke(
        cli,
        ["db", "restore", str(backup_path), "--force", "--config", str(config_path)],
    )
    assert restore_result.exit_code != 0
    assert "Checksum mismatch" in restore_result.output

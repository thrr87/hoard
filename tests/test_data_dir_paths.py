from __future__ import annotations

from pathlib import Path

from hoard.cli import main as cli_main
from hoard.core.config import (
    ensure_config_file,
    get_default_config_path,
    load_config,
    resolve_paths,
    save_config,
)
from hoard.core.sync import service as sync_service


def test_default_paths_follow_hoard_data_dir(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "hoard-data"
    monkeypatch.setenv("HOARD_DATA_DIR", str(data_dir))

    config_path = get_default_config_path()
    assert config_path == data_dir / "config.yaml"

    ensure_config_file(None)
    config = load_config(None)
    paths = resolve_paths(config, None)

    assert paths.config_path == data_dir / "config.yaml"
    assert paths.db_path == data_dir / "hoard.db"
    assert Path(config["write"]["server_secret_file"]) == data_dir / "server.key"
    assert Path(config["artifacts"]["blob_path"]) == data_dir / "artifacts"


def test_explicit_path_settings_are_respected(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "hoard-data"
    monkeypatch.setenv("HOARD_DATA_DIR", str(data_dir))

    config_path = data_dir / "config.yaml"
    explicit_db = tmp_path / "explicit.db"
    explicit_secret = tmp_path / "secret.key"
    explicit_artifacts = tmp_path / "artifacts"
    save_config(
        {
            "storage": {"db_path": str(explicit_db)},
            "write": {"server_secret_file": str(explicit_secret)},
            "artifacts": {"blob_path": str(explicit_artifacts)},
        },
        config_path,
    )

    config = load_config(config_path)
    assert Path(config["storage"]["db_path"]) == explicit_db
    assert Path(config["write"]["server_secret_file"]) == explicit_secret
    assert Path(config["artifacts"]["blob_path"]) == explicit_artifacts


def test_operational_paths_follow_data_dir(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "ops-data"
    monkeypatch.setenv("HOARD_DATA_DIR", str(data_dir))

    pid_path, log_path = cli_main._daemon_paths()
    lock_path = sync_service._lock_path()

    assert pid_path.parent == data_dir
    assert log_path.parent == data_dir
    assert lock_path.parent == data_dir

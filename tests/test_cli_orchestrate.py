from __future__ import annotations

from pathlib import Path

import yaml
from click.testing import CliRunner

from hoard.cli.main import cli


def test_cli_orchestrate_init_writes_config(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    blob_path = tmp_path / "artifacts"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["orchestrate", "init", "--config", str(config_path)],
        input=f"{blob_path}\n14\n",
    )
    assert result.exit_code == 0

    data = yaml.safe_load(config_path.read_text())
    assert data["orchestrator"]["registration_token"].startswith("hoard_reg_")
    assert data["artifacts"]["blob_path"] == str(blob_path)
    assert data["artifacts"]["retention_days"] == 14

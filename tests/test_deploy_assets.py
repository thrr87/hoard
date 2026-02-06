from __future__ import annotations

from pathlib import Path

import yaml


def test_deploy_files_exist() -> None:
    assert Path("Dockerfile").exists()
    assert Path(".dockerignore").exists()
    assert Path("deploy/docker-compose.yml").exists()
    assert Path("deploy/Caddyfile").exists()
    assert Path("deploy/.env.example").exists()


def test_compose_includes_data_dir_and_healthcheck() -> None:
    compose = yaml.safe_load(Path("deploy/docker-compose.yml").read_text())
    hoard = compose["services"]["hoard"]
    assert hoard["environment"]["HOARD_DATA_DIR"] == "/data"
    healthcheck = hoard["healthcheck"]["test"]
    health_text = " ".join(healthcheck)
    assert "/health" in health_text


def test_caddyfile_routes_expected_paths() -> None:
    content = Path("deploy/Caddyfile").read_text()
    assert "/mcp" in content
    assert "/events" in content
    assert "/health" in content

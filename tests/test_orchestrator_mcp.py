from __future__ import annotations

import json
import socket
import time
import urllib.request
from pathlib import Path

from hoard.core.config import save_config


def _call_mcp(url: str, token: str, method: str, params: dict) -> tuple[int, dict]:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, json.loads(resp.read())


def _server_url(base: str) -> str:
    return base.replace("/mcp", "")


def _config(db_path: Path, artifacts_path: Path) -> dict:
    return {
        "storage": {"db_path": str(db_path)},
        "vectors": {"enabled": False},
        "orchestrator": {"registration_token": "hoard_reg_test"},
        "artifacts": {"blob_path": str(artifacts_path), "inline_max_bytes": 16},
    }


def test_orchestrator_mcp_task_flow(tmp_path: Path, mcp_server, monkeypatch) -> None:
    db_path = tmp_path / "hoard.db"
    config_path = tmp_path / "config.yaml"
    save_config(_config(db_path, tmp_path / "artifacts"), config_path)

    monkeypatch.setenv("HOARD_SERVER_SECRET", "hoard_admin_test")

    url = mcp_server(config_path)

    _, register_resp = _call_mcp(
        url,
        "hoard_reg_test",
        "tools/call",
        {
            "name": "agent.register",
            "arguments": {
                "name": "worker-alpha",
                "agent_type": "worker",
                "scopes": ["task.claim", "task.execute", "artifact.write", "artifact.read", "event.read"],
                "capabilities": ["cap.basic"],
            },
        },
    )
    register_content = json.loads(register_resp["result"]["content"][0]["text"])
    agent_token = register_content["token"]

    _, task_resp = _call_mcp(
        url,
        "hoard_admin_test",
        "tools/call",
        {"name": "task.create", "arguments": {"name": "Demo task", "requires_capability": "cap.basic"}},
    )
    task_content = json.loads(task_resp["result"]["content"][0]["text"])
    task_id = task_content["id"]

    _, poll_resp = _call_mcp(url, agent_token, "tools/call", {"name": "task.poll", "arguments": {"limit": 5}})
    poll_content = json.loads(poll_resp["result"]["content"][0]["text"])
    tasks = poll_content["tasks"]
    assert tasks and tasks[0]["id"] == task_id

    _, claim_resp = _call_mcp(url, agent_token, "tools/call", {"name": "task.claim", "arguments": {"task_id": task_id}})
    claim_content = json.loads(claim_resp["result"]["content"][0]["text"])
    assert claim_content["task"]["status"] == "claimed"

    _, start_resp = _call_mcp(url, agent_token, "tools/call", {"name": "task.start", "arguments": {"task_id": task_id}})
    start_content = json.loads(start_resp["result"]["content"][0]["text"])
    assert start_content["success"] is True

    _, complete_resp = _call_mcp(
        url,
        agent_token,
        "tools/call",
        {"name": "task.complete", "arguments": {"task_id": task_id, "output_summary": "done"}},
    )
    complete_content = json.loads(complete_resp["result"]["content"][0]["text"])
    assert complete_content["success"] is True

    _, artifact_resp = _call_mcp(
        url,
        agent_token,
        "tools/call",
        {
            "name": "artifact.put",
            "arguments": {
                "task_id": task_id,
                "name": "result.txt",
                "artifact_type": "text",
                "content": "hello",
            },
        },
    )
    artifact_content = json.loads(artifact_resp["result"]["content"][0]["text"])
    artifact_id = artifact_content["artifact"]["artifact_id"]

    _, artifact_get_resp = _call_mcp(
        url,
        agent_token,
        "tools/call",
        {"name": "artifact.get", "arguments": {"artifact_id": artifact_id, "include_content": True}},
    )
    artifact_get_content = json.loads(artifact_get_resp["result"]["content"][0]["text"])
    assert artifact_get_content["artifact"]["content"] == "hello"

    _, event_resp = _call_mcp(url, agent_token, "tools/call", {"name": "event.poll", "arguments": {"limit": 50}})
    event_content = json.loads(event_resp["result"]["content"][0]["text"])
    event_types = [event["event_type"] for event in event_content["events"]]
    assert "task.completed" in event_types


def test_orchestrator_mcp_event_stream(tmp_path: Path, mcp_server, monkeypatch) -> None:
    db_path = tmp_path / "hoard.db"
    config_path = tmp_path / "config.yaml"
    save_config(_config(db_path, tmp_path / "artifacts"), config_path)

    monkeypatch.setenv("HOARD_SERVER_SECRET", "hoard_admin_test")

    url = mcp_server(config_path)

    _call_mcp(
        url,
        "hoard_admin_test",
        "tools/call",
        {"name": "event.publish", "arguments": {"event_type": "event.stream.test", "payload": {"ok": True}}},
    )

    time.sleep(0.05)

    sse_url = f"{_server_url(url)}/events?since=1970-01-01T00:00:00"
    req = urllib.request.Request(sse_url, headers={"Authorization": "Bearer hoard_admin_test"})

    found = False
    with urllib.request.urlopen(req, timeout=3) as resp:
        for _ in range(20):
            try:
                line = resp.readline().decode("utf-8")
            except socket.timeout:
                break
            if "event: event.stream.test" in line:
                found = True
                break

    assert found

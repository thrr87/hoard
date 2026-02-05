from __future__ import annotations

import base64
import time
from pathlib import Path

import pytest

from hoard.core.db.connection import connect, initialize_db
from hoard.core.orchestrator.agents import (
    deregister_agent,
    heartbeat_agent,
    list_agents,
    register_agent,
    update_agent_capabilities,
)
from hoard.core.orchestrator.artifacts import artifact_get, artifact_list, artifact_put
from hoard.core.orchestrator.cost import cost_budget_status, cost_summary, report_cost
from hoard.core.orchestrator.events import poll_events, publish_event
from hoard.core.orchestrator.tasks import (
    claim_task,
    complete_task,
    create_task,
    fail_task,
    get_task,
    list_tasks,
    poll_tasks,
    start_task,
)
from hoard.core.orchestrator.workflows import (
    WorkflowError,
    create_workflow,
    start_workflow,
    workflow_status,
)


def _config(tmp_path: Path) -> dict:
    return {
        "write": {"server_secret_env": "HOARD_SERVER_SECRET"},
        "orchestrator": {"default_scopes": ["task.claim", "task.execute", "event.read"]},
        "artifacts": {
            "blob_path": str(tmp_path / "artifacts"),
            "inline_max_bytes": 10,
            "retention_days": 30,
        },
        "cost": {
            "budgets": {
                "per_agent": {"default": 5.0},
                "per_workflow": {"default": 7.0},
                "global": {"daily": 10.0},
            }
        },
    }


def _connect(tmp_path: Path):
    db_path = tmp_path / "hoard.db"
    conn = connect(db_path)
    initialize_db(conn)
    return conn


def _set_secret(monkeypatch, value: str = "test-secret") -> None:
    monkeypatch.setenv("HOARD_SERVER_SECRET", value)


def _register_agent(conn, tmp_path: Path, monkeypatch, name: str = "agent-a", capabilities: list[str] | None = None) -> str:
    _set_secret(monkeypatch)
    config = _config(tmp_path)
    agent = register_agent(
        conn,
        config=config,
        name=name,
        agent_type="worker",
        capabilities=capabilities,
    )
    return agent["agent_id"]


def test_agent_register_and_lifecycle(tmp_path: Path, monkeypatch) -> None:
    _set_secret(monkeypatch)
    conn = _connect(tmp_path)
    config = _config(tmp_path)

    agent = register_agent(conn, config=config, name="alpha", agent_type="worker")
    assert agent["agent_id"]
    assert "task.claim" in agent["scopes"]

    listed = list_agents(conn)
    assert any(entry["name"] == "alpha" for entry in listed)

    assert heartbeat_agent(conn, agent_id=agent["agent_id"], status="active")
    listed = list_agents(conn)
    row = next(entry for entry in listed if entry["agent_id"] == agent["agent_id"])
    assert row["last_heartbeat_at"] is not None

    assert update_agent_capabilities(conn, agent_id=agent["agent_id"], capabilities=["cap.one", "cap.two"])
    listed = list_agents(conn)
    row = next(entry for entry in listed if entry["agent_id"] == agent["agent_id"])
    caps = {cap["capability"] for cap in row["capabilities"]}
    assert {"cap.one", "cap.two"}.issubset(caps)

    assert deregister_agent(conn, agent_id=agent["agent_id"])
    listed = list_agents(conn)
    row = next(entry for entry in listed if entry["agent_id"] == agent["agent_id"])
    assert row["status"] == "deregistered"

    token_row = conn.execute(
        "SELECT COUNT(*) AS total FROM agent_tokens WHERE agent_id = ?",
        (agent["agent_id"],),
    ).fetchone()
    assert token_row["total"] == 0
    conn.close()


def test_task_lifecycle_and_retry(tmp_path: Path, monkeypatch) -> None:
    conn = _connect(tmp_path)
    agent_id = _register_agent(conn, tmp_path, monkeypatch, capabilities=["cap.summarize"])

    task = create_task(
        conn,
        name="Summarize",
        description="Test task",
        requires_capability="cap.summarize",
        max_attempts=2,
    )
    assert task["status"] == "queued"

    assert poll_tasks(conn, agent_id=agent_id, capabilities=set(), limit=5) == []

    tasks = poll_tasks(conn, agent_id=agent_id, capabilities={"cap.summarize"}, limit=5)
    assert tasks and tasks[0]["id"] == task["id"]

    claimed = claim_task(conn, task_id=task["id"], agent_id=agent_id)
    assert claimed and claimed["status"] == "claimed"
    assert claim_task(conn, task_id=task["id"], agent_id="agent-b") is None

    assert start_task(conn, task_id=task["id"], agent_id=agent_id)

    assert fail_task(conn, task_id=task["id"], agent_id=agent_id, error_message="boom")
    refreshed = get_task(conn, task["id"])
    assert refreshed["status"] == "queued"

    assert claim_task(conn, task_id=task["id"], agent_id=agent_id)
    assert fail_task(conn, task_id=task["id"], agent_id=agent_id, error_message="boom2")
    refreshed = get_task(conn, task["id"])
    assert refreshed["status"] == "failed"

    events = poll_events(conn, limit=10)
    event_types = [event["event_type"] for event in events]
    assert "task.failed" in event_types
    conn.close()


def test_task_dependencies_promote(tmp_path: Path, monkeypatch) -> None:
    conn = _connect(tmp_path)
    agent_id = _register_agent(conn, tmp_path, monkeypatch)

    parent = create_task(conn, name="parent")
    child = create_task(
        conn,
        name="child",
        depends_on=[{"task_id": parent["id"], "dependency_type": "completion"}],
    )
    assert child["status"] == "pending"

    assert claim_task(conn, task_id=parent["id"], agent_id=agent_id)
    assert start_task(conn, task_id=parent["id"], agent_id=agent_id)
    assert complete_task(conn, task_id=parent["id"], agent_id=agent_id)

    tasks = poll_tasks(conn, agent_id=agent_id, capabilities=set(), limit=5)
    ids = {task["id"] for task in tasks}
    assert child["id"] in ids
    conn.close()


def test_task_complete_emits_event(tmp_path: Path, monkeypatch) -> None:
    conn = _connect(tmp_path)
    agent_id = _register_agent(conn, tmp_path, monkeypatch)
    task = create_task(conn, name="finish")
    assert claim_task(conn, task_id=task["id"], agent_id=agent_id)
    assert start_task(conn, task_id=task["id"], agent_id=agent_id)
    assert complete_task(conn, task_id=task["id"], agent_id=agent_id, output_summary="done")

    events = poll_events(conn, limit=10)
    event_types = [event["event_type"] for event in events]
    assert "task.completed" in event_types
    conn.close()


def test_artifacts_inline_and_blob(tmp_path: Path) -> None:
    conn = _connect(tmp_path)
    config = _config(tmp_path)
    task = create_task(conn, name="artifact-task")

    inline = artifact_put(
        conn,
        config=config,
        task_id=task["id"],
        name="inline.txt",
        artifact_type="text",
        content="small",
    )
    assert inline["content_blob_path"] is None

    blob_content = "x" * 32
    blob = artifact_put(
        conn,
        config=config,
        task_id=task["id"],
        name="blob.bin",
        artifact_type="binary",
        content=blob_content,
    )
    assert blob["content_blob_path"]
    assert Path(blob["content_blob_path"]).exists()

    inline_fetched = artifact_get(conn, artifact_id=inline["artifact_id"], include_content=True)
    assert inline_fetched["content"] == "small"

    blob_fetched = artifact_get(conn, artifact_id=blob["artifact_id"], include_content=True)
    decoded = base64.b64decode(blob_fetched["content_base64"]).decode("utf-8")
    assert decoded == blob_content

    listed = artifact_list(conn, task_id=task["id"])
    assert {entry["artifact_id"] for entry in listed} == {inline["artifact_id"], blob["artifact_id"]}
    conn.close()


def test_events_poll_ordering(tmp_path: Path) -> None:
    conn = _connect(tmp_path)

    publish_event(conn, event_type="event.one", payload={"a": 1})
    time.sleep(0.01)
    publish_event(conn, event_type="event.two", payload={"b": 2})

    events = poll_events(conn, limit=10)
    assert len(events) >= 2
    assert events[0]["event_type"] == "event.one"
    assert events[1]["event_type"] == "event.two"

    since = events[0]["published_at"]
    next_events = poll_events(conn, since=since, limit=10)
    assert next_events and next_events[0]["event_type"] == "event.two"
    conn.close()


def test_cost_summary_and_budgets(tmp_path: Path) -> None:
    conn = _connect(tmp_path)
    config = _config(tmp_path)

    report_cost(conn, agent_id="agent-a", model="model-x", provider="provider", estimated_cost_usd=1.5)
    report_cost(conn, agent_id="agent-b", model="model-y", provider="provider", estimated_cost_usd=2.0)

    summary = cost_summary(conn, period="today")
    assert summary["total_usd"] == pytest.approx(3.5)

    budget = cost_budget_status(conn, agent_id="agent-a", workflow_id="wf-1", period="today", config=config)
    assert budget["global"]["budget"] == 10.0
    assert budget["agent"]["budget"] == 5.0
    assert budget["workflow"]["budget"] == 7.0
    conn.close()


def test_workflow_dag_and_progress(tmp_path: Path, monkeypatch) -> None:
    conn = _connect(tmp_path)
    agent_id = _register_agent(conn, tmp_path, monkeypatch)

    definition = {
        "steps": [
            {"step_key": "step-1", "name": "Step 1"},
            {"step_key": "step-2", "name": "Step 2", "depends_on_steps": ["step-1"]},
        ]
    }
    workflow = create_workflow(conn, name="Demo", description=None, definition=definition)
    assert workflow["status"] == "draft"

    workflow = start_workflow(conn, workflow_id=workflow["id"], inputs={"foo": "bar"})
    status = workflow_status(conn, workflow_id=workflow["id"])
    steps = {step["step_key"]: step for step in status["steps"]}
    assert steps["step-1"]["status"] == "running"
    assert steps["step-2"]["status"] == "pending"

    tasks = list_tasks(conn, workflow_id=workflow["id"])
    assert len(tasks) == 1
    first_task = tasks[0]

    assert claim_task(conn, task_id=first_task["id"], agent_id=agent_id)
    assert start_task(conn, task_id=first_task["id"], agent_id=agent_id)
    assert complete_task(conn, task_id=first_task["id"], agent_id=agent_id)

    status = workflow_status(conn, workflow_id=workflow["id"])
    steps = {step["step_key"]: step for step in status["steps"]}
    assert steps["step-1"]["status"] == "completed"
    assert steps["step-2"]["status"] == "running"

    tasks = list_tasks(conn, workflow_id=workflow["id"])
    assert len(tasks) == 2
    second_task = next(task for task in tasks if task["id"] != first_task["id"])

    assert claim_task(conn, task_id=second_task["id"], agent_id=agent_id)
    assert start_task(conn, task_id=second_task["id"], agent_id=agent_id)
    assert complete_task(conn, task_id=second_task["id"], agent_id=agent_id)

    status = workflow_status(conn, workflow_id=workflow["id"])
    assert status["status"] == "completed"
    conn.close()


def test_workflow_definition_validation(tmp_path: Path) -> None:
    conn = _connect(tmp_path)

    cycle_def = {
        "steps": [
            {"step_key": "a", "depends_on_steps": ["b"]},
            {"step_key": "b", "depends_on_steps": ["a"]},
        ]
    }
    with pytest.raises(WorkflowError):
        create_workflow(conn, name="Cycle", description=None, definition=cycle_def)

    missing_dep_def = {
        "steps": [
            {"step_key": "a", "depends_on_steps": ["missing"]},
        ]
    }
    with pytest.raises(WorkflowError):
        create_workflow(conn, name="Missing", description=None, definition=missing_dep_def)

    fallback_missing = {
        "steps": [
            {"step_key": "a", "on_failure": "fallback", "fallback_step": "missing"},
        ]
    }
    with pytest.raises(WorkflowError):
        create_workflow(conn, name="Fallback", description=None, definition=fallback_missing)

    conn.close()


def test_workflow_on_failure_skip(tmp_path: Path, monkeypatch) -> None:
    conn = _connect(tmp_path)
    agent_id = _register_agent(conn, tmp_path, monkeypatch)

    definition = {
        "steps": [
            {"step_key": "step-1", "name": "Step 1", "on_failure": "skip", "max_attempts": 1},
            {"step_key": "step-2", "name": "Step 2", "depends_on_steps": ["step-1"]},
        ]
    }
    workflow = create_workflow(conn, name="Skip Demo", description=None, definition=definition)
    start_workflow(conn, workflow_id=workflow["id"], inputs=None)

    tasks = list_tasks(conn, workflow_id=workflow["id"])
    assert len(tasks) == 1
    first_task = tasks[0]

    assert claim_task(conn, task_id=first_task["id"], agent_id=agent_id)
    assert start_task(conn, task_id=first_task["id"], agent_id=agent_id)
    assert fail_task(conn, task_id=first_task["id"], agent_id=agent_id, error_message="skip")

    status = workflow_status(conn, workflow_id=workflow["id"])
    steps = {step["step_key"]: step for step in status["steps"]}
    assert steps["step-1"]["status"] == "skipped"
    assert steps["step-2"]["status"] == "running"

    tasks = list_tasks(conn, workflow_id=workflow["id"])
    assert len(tasks) == 2
    second_task = next(task for task in tasks if task["workflow_step_id"] == "step-2")

    assert claim_task(conn, task_id=second_task["id"], agent_id=agent_id)
    assert start_task(conn, task_id=second_task["id"], agent_id=agent_id)
    assert complete_task(conn, task_id=second_task["id"], agent_id=agent_id)

    status = workflow_status(conn, workflow_id=workflow["id"])
    assert status["status"] == "completed"
    conn.close()


def test_workflow_on_failure_retry(tmp_path: Path, monkeypatch) -> None:
    conn = _connect(tmp_path)
    agent_id = _register_agent(conn, tmp_path, monkeypatch)

    definition = {
        "steps": [
            {"step_key": "step-1", "name": "Step 1", "on_failure": "retry", "max_attempts": 2},
        ]
    }
    workflow = create_workflow(conn, name="Retry Demo", description=None, definition=definition)
    start_workflow(conn, workflow_id=workflow["id"], inputs=None)

    tasks = list_tasks(conn, workflow_id=workflow["id"])
    assert len(tasks) == 1
    first_task = tasks[0]

    assert claim_task(conn, task_id=first_task["id"], agent_id=agent_id)
    assert start_task(conn, task_id=first_task["id"], agent_id=agent_id)
    assert fail_task(conn, task_id=first_task["id"], agent_id=agent_id, error_message="retry")

    tasks = list_tasks(conn, workflow_id=workflow["id"])
    assert len(tasks) == 2
    status = workflow_status(conn, workflow_id=workflow["id"])
    steps = {step["step_key"]: step for step in status["steps"]}
    assert steps["step-1"]["status"] == "running"

    second_task = next(task for task in tasks if task["id"] != first_task["id"])
    assert claim_task(conn, task_id=second_task["id"], agent_id=agent_id)
    assert start_task(conn, task_id=second_task["id"], agent_id=agent_id)
    assert fail_task(conn, task_id=second_task["id"], agent_id=agent_id, error_message="retry 2")

    status = workflow_status(conn, workflow_id=workflow["id"])
    assert status["status"] == "failed"
    conn.close()


def test_workflow_on_failure_fallback(tmp_path: Path, monkeypatch) -> None:
    conn = _connect(tmp_path)
    agent_id = _register_agent(conn, tmp_path, monkeypatch)

    definition = {
        "steps": [
            {"step_key": "implement", "name": "Implement"},
            {
                "step_key": "review",
                "name": "Review",
                "depends_on_steps": ["implement"],
                "on_failure": "fallback",
                "fallback_step": "implement",
                "max_attempts": 2,
            },
        ]
    }
    workflow = create_workflow(conn, name="Fallback Demo", description=None, definition=definition)
    start_workflow(conn, workflow_id=workflow["id"], inputs=None)

    tasks = list_tasks(conn, workflow_id=workflow["id"])
    assert len(tasks) == 1
    implement_task = tasks[0]

    assert claim_task(conn, task_id=implement_task["id"], agent_id=agent_id)
    assert start_task(conn, task_id=implement_task["id"], agent_id=agent_id)
    assert complete_task(conn, task_id=implement_task["id"], agent_id=agent_id)

    tasks = list_tasks(conn, workflow_id=workflow["id"])
    assert len(tasks) == 2
    review_task = next(task for task in tasks if task["workflow_step_id"] == "review")

    assert claim_task(conn, task_id=review_task["id"], agent_id=agent_id)
    assert start_task(conn, task_id=review_task["id"], agent_id=agent_id)
    config = _config(tmp_path)
    artifact = artifact_put(
        conn,
        config=config,
        task_id=review_task["id"],
        name="review.json",
        artifact_type="json",
        content="{\"status\": \"fail\", \"notes\": \"needs more tests\"}",
    )
    conn.execute(
        "UPDATE tasks SET output_artifact_id = ? WHERE id = ?",
        (artifact["artifact_id"], review_task["id"]),
    )
    assert fail_task(conn, task_id=review_task["id"], agent_id=agent_id, error_message="needs work")

    tasks = list_tasks(conn, workflow_id=workflow["id"])
    assert len(tasks) == 3
    status = workflow_status(conn, workflow_id=workflow["id"])
    steps = {step["step_key"]: step for step in status["steps"]}
    assert steps["implement"]["status"] == "running"
    assert steps["review"]["status"] == "pending"

    retry_task = next(task for task in tasks if task["workflow_step_id"] == "implement" and task["id"] != implement_task["id"])
    fallback_payload = retry_task.get("input_data") or {}
    fallback_ctx = fallback_payload.get("fallback") or {}
    assert fallback_ctx.get("step_key") == "review"
    assert fallback_ctx.get("error_message") == "needs work"
    assert fallback_ctx.get("task_id") == review_task["id"]
    assert fallback_ctx.get("attempt_number") == 1
    output_artifact = fallback_ctx.get("output_artifact", {})
    if output_artifact.get("content") is not None:
        assert output_artifact.get("content") == "{\"status\": \"fail\", \"notes\": \"needs more tests\"}"
    else:
        decoded = base64.b64decode(output_artifact.get("content_base64", "")).decode("utf-8")
        assert decoded == "{\"status\": \"fail\", \"notes\": \"needs more tests\"}"
    assert claim_task(conn, task_id=retry_task["id"], agent_id=agent_id)
    assert start_task(conn, task_id=retry_task["id"], agent_id=agent_id)
    assert complete_task(conn, task_id=retry_task["id"], agent_id=agent_id)

    status = workflow_status(conn, workflow_id=workflow["id"])
    steps = {step["step_key"]: step for step in status["steps"]}
    assert steps["review"]["status"] == "running"

    tasks = list_tasks(conn, workflow_id=workflow["id"])
    retry_review = next(task for task in tasks if task["workflow_step_id"] == "review" and task["id"] != review_task["id"])
    assert claim_task(conn, task_id=retry_review["id"], agent_id=agent_id)
    assert start_task(conn, task_id=retry_review["id"], agent_id=agent_id)
    assert complete_task(conn, task_id=retry_review["id"], agent_id=agent_id)

    status = workflow_status(conn, workflow_id=workflow["id"])
    assert status["status"] == "completed"
    conn.close()


def test_workflow_on_failure_fail_workflow(tmp_path: Path, monkeypatch) -> None:
    conn = _connect(tmp_path)
    agent_id = _register_agent(conn, tmp_path, monkeypatch)

    definition = {
        "steps": [
            {"step_key": "step-1", "name": "Step 1", "on_failure": "fail_workflow", "max_attempts": 1},
            {"step_key": "step-2", "name": "Step 2"},
        ]
    }
    workflow = create_workflow(conn, name="Fail Demo", description=None, definition=definition)
    start_workflow(conn, workflow_id=workflow["id"], inputs=None)

    tasks = list_tasks(conn, workflow_id=workflow["id"])
    assert len(tasks) == 2
    task = next(task for task in tasks if task["workflow_step_id"] == "step-1")

    assert claim_task(conn, task_id=task["id"], agent_id=agent_id)
    assert start_task(conn, task_id=task["id"], agent_id=agent_id)
    assert fail_task(conn, task_id=task["id"], agent_id=agent_id, error_message="boom")

    status = workflow_status(conn, workflow_id=workflow["id"])
    assert status["status"] == "failed"
    steps = {step["step_key"]: step for step in status["steps"]}
    assert steps["step-2"]["status"] == "skipped"

    task_rows = list_tasks(conn, workflow_id=workflow["id"])
    other_task = next(task for task in task_rows if task["workflow_step_id"] == "step-2")
    assert other_task["status"] == "cancelled"
    conn.close()

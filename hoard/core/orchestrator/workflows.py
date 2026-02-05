from __future__ import annotations

import base64
import json
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from hoard.core.orchestrator.events import publish_event
from hoard.core.orchestrator.utils import dumps, now_iso


class WorkflowError(Exception):
    pass


def create_workflow(
    conn,
    *,
    name: str,
    description: Optional[str],
    definition: Dict[str, Any],
    trigger_type: str = "manual",
    trigger_config: Optional[Dict[str, Any]] = None,
    tags: Optional[Iterable[str]] = None,
    created_by: str = "user",
) -> Dict[str, Any]:
    if not name:
        raise WorkflowError("name is required")
    _validate_definition(definition)

    workflow_id = f"wf-{uuid.uuid4()}"
    conn.execute(
        """
        INSERT INTO workflows
        (id, name, description, definition, status, trigger_type, trigger_config, tags, created_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, 'draft', ?, ?, ?, ?, ?, ?)
        """,
        (
            workflow_id,
            name,
            description,
            dumps(definition),
            trigger_type,
            dumps(trigger_config),
            dumps(list(tags) if tags else None),
            created_by,
            now_iso(),
            now_iso(),
        ),
    )
    publish_event(conn, event_type="workflow.created", workflow_id=workflow_id, payload={"name": name})
    return workflow_get(conn, workflow_id)


def start_workflow(conn, *, workflow_id: str, inputs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    workflow = workflow_get(conn, workflow_id)
    if workflow["status"] in {"running", "completed"}:
        return workflow

    definition = workflow["definition"]
    steps = definition.get("steps", [])
    now = now_iso()

    conn.execute(
        """
        UPDATE workflows
        SET status = 'running', started_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (now, now, workflow_id),
    )

    for step in steps:
        _insert_step(conn, workflow_id, step)

    _enqueue_ready_steps(conn, workflow_id, inputs=inputs)
    publish_event(conn, event_type="workflow.started", workflow_id=workflow_id, payload={})
    return workflow_get(conn, workflow_id)


def pause_workflow(conn, *, workflow_id: str) -> bool:
    cursor = conn.execute(
        "UPDATE workflows SET status = 'paused', updated_at = ? WHERE id = ?",
        (now_iso(), workflow_id),
    )
    if cursor.rowcount:
        publish_event(conn, event_type="workflow.paused", workflow_id=workflow_id, payload={})
    return cursor.rowcount > 0


def resume_workflow(conn, *, workflow_id: str) -> bool:
    cursor = conn.execute(
        "UPDATE workflows SET status = 'running', updated_at = ? WHERE id = ?",
        (now_iso(), workflow_id),
    )
    if cursor.rowcount:
        publish_event(conn, event_type="workflow.resumed", workflow_id=workflow_id, payload={})
        _enqueue_ready_steps(conn, workflow_id, inputs=None)
    return cursor.rowcount > 0


def cancel_workflow(conn, *, workflow_id: str) -> bool:
    cursor = conn.execute(
        "UPDATE workflows SET status = 'cancelled', updated_at = ? WHERE id = ?",
        (now_iso(), workflow_id),
    )
    if cursor.rowcount:
        publish_event(conn, event_type="workflow.cancelled", workflow_id=workflow_id, payload={})
    return cursor.rowcount > 0


def workflow_status(conn, *, workflow_id: str) -> Dict[str, Any]:
    workflow = workflow_get(conn, workflow_id)
    steps = conn.execute(
        "SELECT * FROM workflow_steps WHERE workflow_id = ? ORDER BY created_at",
        (workflow_id,),
    ).fetchall()
    workflow["steps"] = [dict(step) for step in steps]
    return workflow


def workflow_get(conn, workflow_id: str) -> Dict[str, Any]:
    row = conn.execute("SELECT * FROM workflows WHERE id = ?", (workflow_id,)).fetchone()
    if not row:
        raise WorkflowError("Workflow not found")
    return _row_to_dict(row)


def workflow_list(conn, *, status: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM workflows WHERE 1=1"
    params: list[Any] = []
    if status:
        sql += " AND status = ?"
        params.append(status)
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(row) for row in rows]


def on_task_completion(conn, *, task_id: str, status: str, config: Optional[dict] = None) -> None:
    row = conn.execute(
        "SELECT workflow_id, workflow_step_id FROM tasks WHERE id = ?",
        (task_id,),
    ).fetchone()
    if not row or not row["workflow_id"] or not row["workflow_step_id"]:
        return

    task_row = conn.execute(
        "SELECT * FROM tasks WHERE id = ?",
        (task_id,),
    ).fetchone()

    workflow_id = row["workflow_id"]
    step_key = row["workflow_step_id"]
    step_row = conn.execute(
        "SELECT * FROM workflow_steps WHERE workflow_id = ? AND step_key = ?",
        (workflow_id, step_key),
    ).fetchone()
    if not step_row:
        return

    if status == "completed":
        conn.execute(
            """
            UPDATE workflow_steps
            SET status = 'completed'
            WHERE workflow_id = ? AND step_key = ?
            """,
            (workflow_id, step_key),
        )
        publish_event(
            conn,
            event_type="workflow.step.completed",
            workflow_id=workflow_id,
            payload={"step_key": step_key},
        )
        _enqueue_ready_steps(conn, workflow_id, inputs=None)
        _finalize_workflow_if_done(conn, workflow_id)
        return

    if status in {"failed", "cancelled"}:
        failure_context = _failure_context(conn, task_row, step_key=step_key, config=config)
        on_failure = step_row["on_failure"] or "retry"
        if on_failure == "retry":
            attempts = _step_attempts(conn, workflow_id, step_key)
            max_attempts = int(step_row["max_attempts"] or 1)
            if attempts < max_attempts:
                publish_event(
                    conn,
                    event_type="workflow.step.retry",
                    workflow_id=workflow_id,
                    payload={
                        "step_key": step_key,
                        "attempt": attempts + 1,
                        "max_attempts": max_attempts,
                        "reason": failure_context.get("error_message"),
                    },
                )
                _create_task_for_step(conn, workflow_id, step_row, inputs=None)
                return
            _fail_workflow(
                conn,
                workflow_id,
                step_key=step_key,
                reason=f"max_attempts_exceeded:{max_attempts}",
            )
            return

        if on_failure == "fallback":
            attempts = _step_attempts(conn, workflow_id, step_key)
            max_attempts = int(step_row["max_attempts"] or 1)
            if attempts >= max_attempts:
                _fail_workflow(
                    conn,
                    workflow_id,
                    step_key=step_key,
                    reason=f"max_attempts_exceeded:{max_attempts}",
                )
                return
            handled = _handle_fallback_failure(conn, workflow_id, step_row, failure_context)
            if handled:
                return
            _fail_workflow(conn, workflow_id, step_key=step_key, reason="fallback_missing")
            return

        if on_failure == "skip":
            conn.execute(
                """
                UPDATE workflow_steps
                SET status = 'skipped'
                WHERE workflow_id = ? AND step_key = ?
                """,
                (workflow_id, step_key),
            )
            publish_event(
                conn,
                event_type="workflow.step.skipped",
                workflow_id=workflow_id,
                payload={"step_key": step_key},
            )
        else:
            _fail_workflow(conn, workflow_id, step_key=step_key)
            return

        _enqueue_ready_steps(conn, workflow_id, inputs=None)
        _finalize_workflow_if_done(conn, workflow_id)


def _insert_step(conn, workflow_id: str, step: Dict[str, Any]) -> None:
    step_id = f"wfs-{uuid.uuid4()}"
    fallback_step_id = step.get("fallback_step_id") or step.get("fallback_step")
    conn.execute(
        """
        INSERT OR IGNORE INTO workflow_steps
        (id, workflow_id, step_key, name, description, requires_capability, requires_proficiency,
         preferred_agent_id, input_mapping, timeout_seconds, max_attempts, on_failure, fallback_step_id,
         depends_on_steps, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """,
        (
            step_id,
            workflow_id,
            step.get("step_key"),
            step.get("name") or step.get("step_key"),
            step.get("description"),
            step.get("requires_capability"),
            step.get("requires_proficiency", "standard"),
            step.get("preferred_agent_id"),
            dumps(step.get("input_mapping")),
            int(step.get("timeout_seconds", 3600)),
            int(step.get("max_attempts", 3)),
            step.get("on_failure", "retry"),
            fallback_step_id,
            dumps(step.get("depends_on_steps") or []),
            now_iso(),
        ),
    )


def _enqueue_ready_steps(
    conn,
    workflow_id: str,
    inputs: Optional[Dict[str, Any]],
    context_by_step: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    rows = conn.execute(
        "SELECT * FROM workflow_steps WHERE workflow_id = ?",
        (workflow_id,),
    ).fetchall()
    completed = {row["step_key"] for row in rows if row["status"] in {"completed", "skipped"}}
    pending = [row for row in rows if row["status"] == "pending"]

    for row in pending:
        depends = json.loads(row["depends_on_steps"]) if row["depends_on_steps"] else []
        if all(dep in completed for dep in depends):
            conn.execute(
                "UPDATE workflow_steps SET status = 'ready' WHERE id = ?",
                (row["id"],),
            )
            extra_context = (context_by_step or {}).get(row["step_key"])
            _create_task_for_step(conn, workflow_id, row, inputs=inputs, extra_context=extra_context)


def _create_task_for_step(
    conn,
    workflow_id: str,
    step_row,
    inputs: Optional[Dict[str, Any]],
    extra_context: Optional[Dict[str, Any]] = None,
) -> None:
    from hoard.core.orchestrator.tasks import create_task

    input_mapping = json.loads(step_row["input_mapping"]) if step_row["input_mapping"] else None
    payload = {"inputs": inputs} if inputs else {}
    if input_mapping:
        payload["input_mapping"] = input_mapping
    if extra_context:
        payload["fallback"] = extra_context

    task = create_task(
        conn,
        name=step_row["name"],
        description=step_row["description"],
        requires_capability=step_row["requires_capability"],
        requires_proficiency=step_row["requires_proficiency"],
        workflow_id=workflow_id,
        workflow_step_id=step_row["step_key"],
        input_data=payload,
        timeout_seconds=int(step_row["timeout_seconds"] or 3600),
        max_attempts=1,
    )
    conn.execute(
        "UPDATE workflow_steps SET task_id = ?, status = 'running' WHERE id = ?",
        (task["id"], step_row["id"]),
    )
    publish_event(
        conn,
        event_type="workflow.step.ready",
        workflow_id=workflow_id,
        task_id=task["id"],
        payload={"step_key": step_row["step_key"]},
    )


def _finalize_workflow_if_done(conn, workflow_id: str) -> None:
    rows = conn.execute(
        "SELECT status FROM workflow_steps WHERE workflow_id = ?",
        (workflow_id,),
    ).fetchall()
    if not rows:
        return
    statuses = {row["status"] for row in rows}
    if statuses.issubset({"completed", "skipped"}):
        conn.execute(
            "UPDATE workflows SET status = 'completed', completed_at = ?, updated_at = ? WHERE id = ?",
            (now_iso(), now_iso(), workflow_id),
        )
        publish_event(conn, event_type="workflow.completed", workflow_id=workflow_id, payload={})
    elif "failed" in statuses:
        conn.execute(
            "UPDATE workflows SET status = 'failed', completed_at = ?, updated_at = ? WHERE id = ?",
            (now_iso(), now_iso(), workflow_id),
        )
        publish_event(conn, event_type="workflow.failed", workflow_id=workflow_id, payload={})


def _validate_definition(definition: Dict[str, Any]) -> None:
    steps = definition.get("steps")
    if not isinstance(steps, list) or not steps:
        raise WorkflowError("Workflow definition must include steps")
    step_keys = [step.get("step_key") for step in steps]
    if any(not key for key in step_keys):
        raise WorkflowError("Each step must have step_key")
    if len(set(step_keys)) != len(step_keys):
        raise WorkflowError("Duplicate step_key in workflow definition")

    for step in steps:
        on_failure = step.get("on_failure", "retry")
        if on_failure == "fallback":
            fallback = step.get("fallback_step_id") or step.get("fallback_step")
            if not fallback:
                raise WorkflowError("fallback_step_id is required when on_failure is fallback")
            if fallback not in step_keys:
                raise WorkflowError(f"Unknown fallback step: {fallback}")
            if fallback == step.get("step_key"):
                raise WorkflowError("fallback_step_id cannot reference itself")

    graph = {step.get("step_key"): step.get("depends_on_steps") or [] for step in steps}
    _check_cycles(graph)


def _check_cycles(graph: Dict[str, List[str]]) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def dfs(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            raise WorkflowError("Workflow definition has cycle")
        visiting.add(node)
        for dep in graph.get(node, []):
            if dep not in graph:
                raise WorkflowError(f"Unknown dependency step: {dep}")
            dfs(dep)
        visiting.remove(node)
        visited.add(node)

    for node in graph:
        dfs(node)


def _step_attempts(conn, workflow_id: str, step_key: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS total FROM tasks WHERE workflow_id = ? AND workflow_step_id = ?",
        (workflow_id, step_key),
    ).fetchone()
    return int(row["total"] or 0) if row else 0


def _handle_fallback_failure(
    conn,
    workflow_id: str,
    step_row,
    failure_context: Dict[str, Any],
) -> bool:
    fallback_key = step_row["fallback_step_id"]
    if not fallback_key:
        return False

    fallback_row = conn.execute(
        "SELECT * FROM workflow_steps WHERE workflow_id = ? AND step_key = ?",
        (workflow_id, fallback_key),
    ).fetchone()
    if not fallback_row:
        return False

    depends = json.loads(step_row["depends_on_steps"]) if step_row["depends_on_steps"] else []
    if fallback_key not in depends:
        depends.append(fallback_key)

    conn.execute(
        """
        UPDATE workflow_steps
        SET status = 'pending', task_id = NULL, depends_on_steps = ?
        WHERE id = ?
        """,
        (dumps(depends), step_row["id"]),
    )
    conn.execute(
        """
        UPDATE workflow_steps
        SET status = 'pending', task_id = NULL
        WHERE id = ?
        """,
        (fallback_row["id"],),
    )
    publish_event(
        conn,
        event_type="workflow.step.fallback",
        workflow_id=workflow_id,
        payload={
            "step_key": step_row["step_key"],
            "fallback_step_key": fallback_key,
            "reason": failure_context.get("error_message"),
        },
    )
    _enqueue_ready_steps(conn, workflow_id, inputs=None, context_by_step={fallback_key: failure_context})
    return True


def _fail_workflow(conn, workflow_id: str, *, step_key: str | None = None, reason: str | None = None) -> None:
    now = now_iso()
    if step_key:
        conn.execute(
            """
            UPDATE workflow_steps
            SET status = 'failed'
            WHERE workflow_id = ? AND step_key = ?
            """,
            (workflow_id, step_key),
        )
        publish_event(
            conn,
            event_type="workflow.step.failed",
            workflow_id=workflow_id,
            payload={"step_key": step_key, "reason": reason},
        )

    conn.execute(
        """
        UPDATE workflow_steps
        SET status = 'skipped'
        WHERE workflow_id = ? AND status IN ('pending','ready','running')
        """,
        (workflow_id,),
    )

    rows = conn.execute(
        """
        SELECT id FROM tasks
        WHERE workflow_id = ?
          AND status NOT IN ('completed','failed','cancelled')
        """,
        (workflow_id,),
    ).fetchall()
    for row in rows:
        conn.execute(
            """
            UPDATE tasks
            SET status = 'cancelled', completed_at = ?, updated_at = ?, error_message = ?
            WHERE id = ? AND status NOT IN ('completed','failed','cancelled')
            """,
            (now, now, reason or "workflow failed", row["id"]),
        )
        publish_event(
            conn,
            event_type="task.cancelled",
            task_id=row["id"],
            workflow_id=workflow_id,
            payload={"reason": reason or "workflow failed"},
        )

    conn.execute(
        "UPDATE workflows SET status = 'failed', completed_at = ?, updated_at = ? WHERE id = ?",
        (now, now, workflow_id),
    )
    publish_event(
        conn,
        event_type="workflow.failed",
        workflow_id=workflow_id,
        payload={"reason": reason} if reason else {},
    )


def _failure_context(conn, task_row, *, step_key: str, config: Optional[dict]) -> Dict[str, Any]:
    if not task_row:
        return {"step_key": step_key}
    artifact_info = _artifact_context(conn, task_row["output_artifact_id"], config=config)
    return {
        "step_key": step_key,
        "task_id": task_row["id"],
        "error_message": task_row["error_message"],
        "output_summary": task_row["output_summary"],
        "output_artifact_id": task_row["output_artifact_id"],
        "output_artifact": artifact_info,
        "attempt_number": task_row["attempt_number"],
        "model_used": task_row["model_used"],
        "tokens_input": task_row["tokens_input"],
        "tokens_output": task_row["tokens_output"],
        "estimated_cost_usd": task_row["estimated_cost_usd"],
        "completed_at": task_row["completed_at"],
    }


def _artifact_context(conn, artifact_id: str | None, *, config: Optional[dict]) -> Dict[str, Any] | None:
    if not artifact_id:
        return None
    max_bytes = _fallback_artifact_max_bytes(config)
    row = conn.execute(
        """
        SELECT id, artifact_type, name, content_text, content_blob_path, content_url,
               mime_type, size_bytes, content_hash, metadata, role, created_at
        FROM task_artifacts
        WHERE id = ?
        """,
        (artifact_id,),
    ).fetchone()
    if not row:
        return None

    metadata = json.loads(row["metadata"]) if row["metadata"] else None
    result = {
        "artifact_id": row["id"],
        "artifact_type": row["artifact_type"],
        "name": row["name"],
        "content_url": row["content_url"],
        "mime_type": row["mime_type"],
        "size_bytes": row["size_bytes"],
        "content_hash": row["content_hash"],
        "metadata": metadata,
        "role": row["role"],
        "created_at": row["created_at"],
        "content_included": False,
    }

    if row["content_text"] is not None:
        size_bytes = int(row["size_bytes"] or len(row["content_text"].encode("utf-8")))
        if size_bytes <= max_bytes:
            result["content"] = row["content_text"]
            result["content_included"] = True
        else:
            result["content_truncated"] = True
        return result

    size_bytes = int(row["size_bytes"] or 0)
    if row["content_blob_path"] and size_bytes and size_bytes <= max_bytes:
        path = Path(row["content_blob_path"])
        if path.exists():
            result["content_base64"] = base64.b64encode(path.read_bytes()).decode("utf-8")
            result["content_included"] = True
        else:
            result["content_truncated"] = True
    elif row["content_blob_path"] and size_bytes > max_bytes:
        result["content_truncated"] = True
    return result


def _fallback_artifact_max_bytes(config: Optional[dict]) -> int:
    default_bytes = 10 * 1024 * 1024
    if not config:
        return default_bytes
    orchestrator = config.get("orchestrator", {})
    value = orchestrator.get("fallback_max_bytes")
    if value is None:
        return default_bytes
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default_bytes
    return parsed if parsed > 0 else default_bytes


def _row_to_dict(row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "name": row["name"],
        "description": row["description"],
        "definition": json.loads(row["definition"]) if row["definition"] else {},
        "status": row["status"],
        "trigger_type": row["trigger_type"],
        "trigger_config": json.loads(row["trigger_config"]) if row["trigger_config"] else None,
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
        "tags": json.loads(row["tags"]) if row["tags"] else [],
        "created_by": row["created_by"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }

from __future__ import annotations

import json
import uuid
from typing import Any, Dict, Iterable, List, Optional

from hoard.core.errors import HoardError
from hoard.core.orchestrator.events import publish_event
from hoard.core.orchestrator.utils import dumps, now_iso
from hoard.core.orchestrator.workflows import on_task_completion


class TaskError(HoardError):
    pass


def create_task(
    conn,
    *,
    name: str,
    description: Optional[str] = None,
    requires_capability: Optional[str] = None,
    requires_proficiency: str = "standard",
    priority: int = 5,
    input_data: Optional[Dict[str, Any]] = None,
    input_artifact_ids: Optional[Iterable[str]] = None,
    workflow_id: Optional[str] = None,
    workflow_step_id: Optional[str] = None,
    created_by: Optional[str] = "user",
    assigned_agent_id: Optional[str] = None,
    timeout_seconds: int = 3600,
    deadline: Optional[str] = None,
    max_attempts: int = 3,
    depends_on: Optional[Iterable[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    if not name:
        raise TaskError("name is required")

    task_id = f"tsk-{uuid.uuid4()}"
    status = "queued" if not depends_on else "pending"

    conn.execute(
        """
        INSERT INTO tasks
        (id, workflow_id, workflow_step_id, created_by, name, description,
         requires_capability, requires_proficiency, priority, input_data,
         input_artifact_ids, status, assigned_agent_id, timeout_seconds,
         deadline, max_attempts, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            workflow_id,
            workflow_step_id,
            created_by,
            name,
            description,
            requires_capability,
            requires_proficiency,
            int(priority),
            dumps(input_data),
            dumps(list(input_artifact_ids) if input_artifact_ids else None),
            status,
            assigned_agent_id,
            int(timeout_seconds),
            deadline,
            int(max_attempts),
            now_iso(),
            now_iso(),
        ),
    )

    for dep in depends_on or []:
        dep_id = dep.get("task_id") if isinstance(dep, dict) else None
        dep_type = dep.get("dependency_type") if isinstance(dep, dict) else "completion"
        if dep_id:
            conn.execute(
                """
                INSERT INTO task_dependencies (task_id, depends_on_task_id, dependency_type)
                VALUES (?, ?, ?)
                """,
                (task_id, dep_id, dep_type or "completion"),
            )

    publish_event(conn, event_type="task.created", task_id=task_id, payload={"name": name})
    return get_task(conn, task_id)


def poll_tasks(
    conn,
    *,
    agent_id: str,
    capabilities: Iterable[str],
    limit: int = 5,
) -> List[Dict[str, Any]]:
    caps = {str(c) for c in capabilities}
    _promote_ready_tasks(conn)

    rows = conn.execute(
        """
        SELECT * FROM tasks
        WHERE status = 'queued'
          AND (assigned_agent_id IS NULL OR assigned_agent_id = ?)
        ORDER BY priority ASC, created_at ASC
        LIMIT ?
        """,
        (agent_id, limit),
    ).fetchall()

    results: List[Dict[str, Any]] = []
    for row in rows:
        if row["requires_capability"] and row["requires_capability"] not in caps:
            continue
        results.append(_row_to_dict(row))
    return results


def claim_task(conn, *, task_id: str, agent_id: str) -> Optional[Dict[str, Any]]:
    if not task_id or not agent_id:
        raise TaskError("task_id and agent_id are required")
    now = now_iso()
    cursor = conn.execute(
        """
        UPDATE tasks
        SET status = 'claimed', assigned_agent_id = ?, claimed_at = ?, updated_at = ?, attempt_number = attempt_number + 1
        WHERE id = ?
          AND status = 'queued'
          AND (assigned_agent_id IS NULL OR assigned_agent_id = ?)
        """,
        (agent_id, now, now, task_id, agent_id),
    )
    if cursor.rowcount == 0:
        return None
    publish_event(conn, event_type="task.claimed", task_id=task_id, agent_id=agent_id, payload={})
    return get_task(conn, task_id)


def start_task(conn, *, task_id: str, agent_id: str) -> bool:
    now = now_iso()
    cursor = conn.execute(
        """
        UPDATE tasks
        SET status = 'running', started_at = ?, updated_at = ?
        WHERE id = ? AND assigned_agent_id = ? AND status = 'claimed'
        """,
        (now, now, task_id, agent_id),
    )
    if cursor.rowcount:
        publish_event(conn, event_type="task.started", task_id=task_id, agent_id=agent_id, payload={})
    return cursor.rowcount > 0


def complete_task(
    conn,
    *,
    task_id: str,
    agent_id: str,
    output_summary: Optional[str] = None,
    output_artifact_id: Optional[str] = None,
    tokens_input: int = 0,
    tokens_output: int = 0,
    estimated_cost_usd: float = 0.0,
    model_used: Optional[str] = None,
    config: Optional[dict] = None,
) -> bool:
    now = now_iso()
    cursor = conn.execute(
        """
        UPDATE tasks
        SET status = 'completed',
            completed_at = ?,
            updated_at = ?,
            output_summary = ?,
            output_artifact_id = ?,
            tokens_input = ?,
            tokens_output = ?,
            estimated_cost_usd = ?,
            model_used = ?
        WHERE id = ? AND assigned_agent_id = ? AND status IN ('running','claimed')
        """,
        (
            now,
            now,
            output_summary,
            output_artifact_id,
            int(tokens_input or 0),
            int(tokens_output or 0),
            float(estimated_cost_usd or 0.0),
            model_used,
            task_id,
            agent_id,
        ),
    )
    if cursor.rowcount:
        publish_event(conn, event_type="task.completed", task_id=task_id, agent_id=agent_id, payload={})
        on_task_completion(conn, task_id=task_id, status="completed", config=config)
    return cursor.rowcount > 0


def fail_task(
    conn,
    *,
    task_id: str,
    agent_id: str,
    error_message: Optional[str] = None,
    config: Optional[dict] = None,
) -> bool:
    now = now_iso()
    row = conn.execute("SELECT attempt_number, max_attempts FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if not row:
        return False
    attempt_number = int(row["attempt_number"] or 0)
    max_attempts = int(row["max_attempts"] or 0)
    if max_attempts and attempt_number < max_attempts:
        cursor = conn.execute(
            """
            UPDATE tasks
            SET status = 'queued', assigned_agent_id = NULL, started_at = NULL,
                claimed_at = NULL, updated_at = ?, error_message = ?
            WHERE id = ? AND assigned_agent_id = ? AND status IN ('running','claimed')
            """,
            (now, error_message, task_id, agent_id),
        )
        if cursor.rowcount:
            publish_event(conn, event_type="task.failed", task_id=task_id, agent_id=agent_id, payload={"retry": True})
        return cursor.rowcount > 0

    cursor = conn.execute(
        """
        UPDATE tasks
        SET status = 'failed', completed_at = ?, updated_at = ?, error_message = ?
        WHERE id = ? AND assigned_agent_id = ? AND status IN ('running','claimed')
        """,
        (now, now, error_message, task_id, agent_id),
    )
    if cursor.rowcount:
        publish_event(conn, event_type="task.failed", task_id=task_id, agent_id=agent_id, payload={"retry": False})
        on_task_completion(conn, task_id=task_id, status="failed", config=config)
    return cursor.rowcount > 0


def cancel_task(conn, *, task_id: str, reason: Optional[str] = None, config: Optional[dict] = None) -> bool:
    now = now_iso()
    cursor = conn.execute(
        """
        UPDATE tasks
        SET status = 'cancelled', completed_at = ?, updated_at = ?, error_message = ?
        WHERE id = ? AND status NOT IN ('completed','failed','cancelled')
        """,
        (now, now, reason, task_id),
    )
    if cursor.rowcount:
        publish_event(conn, event_type="task.cancelled", task_id=task_id, payload={"reason": reason})
        on_task_completion(conn, task_id=task_id, status="cancelled", config=config)
    return cursor.rowcount > 0


def delegate_task(conn, *, parent_task_id: str, name: str, description: Optional[str] = None) -> Dict[str, Any]:
    return create_task(
        conn,
        name=name,
        description=description,
        input_data={"parent_task_id": parent_task_id},
        created_by="task.delegate",
    )


def get_task(conn, task_id: str) -> Dict[str, Any]:
    row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if not row:
        raise TaskError("Task not found")
    task = _row_to_dict(row)
    deps = conn.execute(
        "SELECT depends_on_task_id, dependency_type FROM task_dependencies WHERE task_id = ?",
        (task_id,),
    ).fetchall()
    task["dependencies"] = [dict(dep) for dep in deps]
    return task


def list_tasks(
    conn,
    *,
    status: Optional[str] = None,
    agent_id: Optional[str] = None,
    workflow_id: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM tasks WHERE 1=1"
    params: list[Any] = []
    if status:
        sql += " AND status = ?"
        params.append(status)
    if agent_id:
        sql += " AND assigned_agent_id = ?"
        params.append(agent_id)
    if workflow_id:
        sql += " AND workflow_id = ?"
        params.append(workflow_id)
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(row) for row in rows]


def _row_to_dict(row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "workflow_id": row["workflow_id"],
        "workflow_step_id": row["workflow_step_id"],
        "created_by": row["created_by"],
        "name": row["name"],
        "description": row["description"],
        "requires_capability": row["requires_capability"],
        "requires_proficiency": row["requires_proficiency"],
        "priority": row["priority"],
        "input_data": json.loads(row["input_data"]) if row["input_data"] else None,
        "input_artifact_ids": json.loads(row["input_artifact_ids"]) if row["input_artifact_ids"] else [],
        "status": row["status"],
        "assigned_agent_id": row["assigned_agent_id"],
        "claimed_at": row["claimed_at"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
        "timeout_seconds": row["timeout_seconds"],
        "deadline": row["deadline"],
        "output_summary": row["output_summary"],
        "output_artifact_id": row["output_artifact_id"],
        "error_message": row["error_message"],
        "attempt_number": row["attempt_number"],
        "max_attempts": row["max_attempts"],
        "retry_delay_seconds": row["retry_delay_seconds"],
        "tokens_input": row["tokens_input"],
        "tokens_output": row["tokens_output"],
        "estimated_cost_usd": row["estimated_cost_usd"],
        "model_used": row["model_used"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _promote_ready_tasks(conn) -> None:
    rows = conn.execute(
        """
        SELECT t.id
        FROM tasks t
        WHERE t.status = 'pending'
          AND NOT EXISTS (
            SELECT 1 FROM task_dependencies d
            JOIN tasks dep ON dep.id = d.depends_on_task_id
            WHERE d.task_id = t.id
              AND (
                (d.dependency_type IN ('completion','success_only') AND dep.status != 'completed')
                OR (d.dependency_type = 'any_terminal' AND dep.status NOT IN ('completed','failed','cancelled'))
              )
          )
        """
    ).fetchall()
    for row in rows:
        conn.execute(
            "UPDATE tasks SET status = 'queued', updated_at = ? WHERE id = ?",
            (now_iso(), row["id"]),
        )

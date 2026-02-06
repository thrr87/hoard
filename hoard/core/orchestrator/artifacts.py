from __future__ import annotations

import base64
import hashlib
import json
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from hoard.core.config import default_data_path
from hoard.core.orchestrator.utils import dumps, now_iso


class ArtifactError(Exception):
    pass


def artifact_put(
    conn,
    *,
    config: dict,
    task_id: str,
    name: str,
    artifact_type: str,
    content: Optional[str] = None,
    content_base64: Optional[str] = None,
    content_url: Optional[str] = None,
    mime_type: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    role: str = "output",
) -> Dict[str, Any]:
    if not task_id:
        raise ArtifactError("task_id is required")
    if not name:
        raise ArtifactError("name is required")
    if not artifact_type:
        raise ArtifactError("artifact_type is required")

    blob_root_value = config.get("artifacts", {}).get("blob_path")
    blob_root = Path(blob_root_value).expanduser() if blob_root_value else default_data_path("artifacts")
    inline_max = int(config.get("artifacts", {}).get("inline_max_bytes", 262_144))

    content_bytes: bytes | None = None
    if content is not None:
        content_bytes = content.encode("utf-8")
    elif content_base64 is not None:
        content_bytes = base64.b64decode(content_base64)

    content_hash = hashlib.sha256(content_bytes or b"").hexdigest()[:32]
    size_bytes = len(content_bytes or b"")

    content_text = None
    content_blob_path = None
    if artifact_type in {"text", "json", "code"} and content is not None and size_bytes <= inline_max:
        content_text = content
    elif content_bytes is not None:
        artifact_dir = blob_root / f"art-{content_hash}"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        blob_name = f"{name}"
        path = artifact_dir / blob_name
        path.write_bytes(content_bytes)
        content_blob_path = str(path)

    artifact_id = f"art-{uuid.uuid4()}"
    conn.execute(
        """
        INSERT INTO task_artifacts
        (id, task_id, artifact_type, name, content_text, content_blob_path, content_url,
         mime_type, size_bytes, content_hash, metadata, role, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            artifact_id,
            task_id,
            artifact_type,
            name,
            content_text,
            content_blob_path,
            content_url,
            mime_type,
            size_bytes,
            content_hash,
            dumps(metadata),
            role,
            now_iso(),
        ),
    )

    return {
        "artifact_id": artifact_id,
        "task_id": task_id,
        "artifact_type": artifact_type,
        "name": name,
        "content_hash": content_hash,
        "size_bytes": size_bytes,
        "content_blob_path": content_blob_path,
        "content_url": content_url,
        "mime_type": mime_type,
        "metadata": metadata,
        "role": role,
    }


def artifact_get(
    conn,
    *,
    artifact_id: str,
    include_content: bool = False,
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT * FROM task_artifacts WHERE id = ?",
        (artifact_id,),
    ).fetchone()
    if not row:
        return None

    result = _row_to_dict(row)
    if include_content:
        if row["content_text"] is not None:
            result["content"] = row["content_text"]
        elif row["content_blob_path"]:
            path = Path(row["content_blob_path"])
            if path.exists():
                result["content_base64"] = base64.b64encode(path.read_bytes()).decode("utf-8")
    return result


def artifact_list(
    conn,
    *,
    task_id: Optional[str] = None,
    workflow_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if task_id:
        rows = conn.execute(
            "SELECT * FROM task_artifacts WHERE task_id = ? ORDER BY created_at DESC",
            (task_id,),
        ).fetchall()
    elif workflow_id:
        rows = conn.execute(
            """
            SELECT a.*
            FROM task_artifacts a
            JOIN tasks t ON t.id = a.task_id
            WHERE t.workflow_id = ?
            ORDER BY a.created_at DESC
            """,
            (workflow_id,),
        ).fetchall()
    else:
        rows = []
    return [_row_to_dict(row) for row in rows]


def _row_to_dict(row) -> Dict[str, Any]:
    metadata = json.loads(row["metadata"]) if row["metadata"] else None
    return {
        "artifact_id": row["id"],
        "task_id": row["task_id"],
        "artifact_type": row["artifact_type"],
        "name": row["name"],
        "content_text": row["content_text"],
        "content_blob_path": row["content_blob_path"],
        "content_url": row["content_url"],
        "mime_type": row["mime_type"],
        "size_bytes": row["size_bytes"],
        "content_hash": row["content_hash"],
        "metadata": metadata,
        "role": row["role"],
        "created_at": row["created_at"],
    }

from __future__ import annotations

from typing import Any, Dict, List

from hoard.core.mcp.scopes import require_any_scope
from hoard.core.orchestrator import (
    artifact_get,
    artifact_list,
    artifact_put,
    cancel_task,
    claim_task,
    complete_task,
    cost_budget_status,
    cost_summary,
    create_task,
    create_workflow,
    deregister_agent,
    delegate_task,
    fail_task,
    get_task,
    heartbeat_agent,
    list_agents,
    list_tasks,
    poll_events,
    poll_tasks,
    publish_event,
    register_agent,
    report_cost,
    resume_workflow,
    start_task,
    start_workflow,
    workflow_get,
    workflow_list,
    workflow_status,
    pause_workflow,
    cancel_workflow,
    update_agent_capabilities,
)


TOOL_DEFINITIONS: List[Dict[str, Any]] = [
    # Agent layer
    {
        "name": "agent.register",
        "description": "Register a new agent.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "agent_type": {"type": "string"},
                "capabilities": {"type": "array", "items": {"type": "string"}},
                "scopes": {"type": "array", "items": {"type": "string"}},
                "metadata": {"type": "object"},
                "max_concurrent_tasks": {"type": "integer"},
                "default_model": {"type": "string"},
                "model_provider": {"type": "string"},
                "overwrite": {"type": "boolean"},
            },
            "required": ["name", "agent_type"],
        },
    },
    {
        "name": "agent.heartbeat",
        "description": "Send heartbeat for an agent.",
        "inputSchema": {
            "type": "object",
            "properties": {"agent_id": {"type": "string"}, "status": {"type": "string"}},
            "required": ["agent_id"],
        },
    },
    {
        "name": "agent.capabilities",
        "description": "Update agent capabilities.",
        "inputSchema": {
            "type": "object",
            "properties": {"agent_id": {"type": "string"}, "capabilities": {"type": "array", "items": {"type": "string"}}},
            "required": ["agent_id", "capabilities"],
        },
    },
    {
        "name": "agent.list",
        "description": "List agents.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "agent.deregister",
        "description": "Deregister agent.",
        "inputSchema": {"type": "object", "properties": {"agent_id": {"type": "string"}}, "required": ["agent_id"]},
    },
    # Task layer
    {
        "name": "task.create",
        "description": "Create a standalone task.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "description": {"type": "string"},
                "requires_capability": {"type": "string"},
                "priority": {"type": "integer"},
                "input_data": {"type": "object"},
                "input_artifact_ids": {"type": "array", "items": {"type": "string"}},
                "workflow_id": {"type": "string"},
                "workflow_step_id": {"type": "string"},
                "depends_on": {"type": "array", "items": {"type": "object"}},
            },
            "required": ["name"],
        },
    },
    {
        "name": "task.poll",
        "description": "Poll for available tasks.",
        "inputSchema": {"type": "object", "properties": {"limit": {"type": "integer"}}, "required": []},
    },
    {
        "name": "task.claim",
        "description": "Claim a task.",
        "inputSchema": {"type": "object", "properties": {"task_id": {"type": "string"}}, "required": ["task_id"]},
    },
    {
        "name": "task.start",
        "description": "Start a claimed task.",
        "inputSchema": {"type": "object", "properties": {"task_id": {"type": "string"}}, "required": ["task_id"]},
    },
    {
        "name": "task.complete",
        "description": "Complete a task.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "output_summary": {"type": "string"},
                "output_artifact_id": {"type": "string"},
                "tokens_input": {"type": "integer"},
                "tokens_output": {"type": "integer"},
                "estimated_cost_usd": {"type": "number"},
                "model_used": {"type": "string"},
            },
            "required": ["task_id"],
        },
    },
    {
        "name": "task.fail",
        "description": "Fail a task.",
        "inputSchema": {
            "type": "object",
            "properties": {"task_id": {"type": "string"}, "error_message": {"type": "string"}},
            "required": ["task_id"],
        },
    },
    {
        "name": "task.cancel",
        "description": "Cancel a task.",
        "inputSchema": {"type": "object", "properties": {"task_id": {"type": "string"}, "reason": {"type": "string"}}, "required": ["task_id"]},
    },
    {
        "name": "task.delegate",
        "description": "Delegate a task.",
        "inputSchema": {
            "type": "object",
            "properties": {"parent_task_id": {"type": "string"}, "name": {"type": "string"}, "description": {"type": "string"}},
            "required": ["parent_task_id", "name"],
        },
    },
    {
        "name": "task.get",
        "description": "Get task details.",
        "inputSchema": {"type": "object", "properties": {"task_id": {"type": "string"}}, "required": ["task_id"]},
    },
    {
        "name": "task.list",
        "description": "List tasks.",
        "inputSchema": {
            "type": "object",
            "properties": {"status": {"type": "string"}, "workflow_id": {"type": "string"}, "agent_id": {"type": "string"}, "limit": {"type": "integer"}},
            "required": [],
        },
    },
    # Artifact layer
    {
        "name": "artifact.put",
        "description": "Store an artifact for a task.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "name": {"type": "string"},
                "artifact_type": {"type": "string"},
                "content": {"type": "string"},
                "content_base64": {"type": "string"},
                "content_url": {"type": "string"},
                "mime_type": {"type": "string"},
                "metadata": {"type": "object"},
                "role": {"type": "string"},
            },
            "required": ["task_id", "name", "artifact_type"],
        },
    },
    {
        "name": "artifact.get",
        "description": "Retrieve an artifact.",
        "inputSchema": {
            "type": "object",
            "properties": {"artifact_id": {"type": "string"}, "include_content": {"type": "boolean"}},
            "required": ["artifact_id"],
        },
    },
    {
        "name": "artifact.list",
        "description": "List artifacts for task or workflow.",
        "inputSchema": {
            "type": "object",
            "properties": {"task_id": {"type": "string"}, "workflow_id": {"type": "string"}},
            "required": [],
        },
    },
    # Event layer
    {
        "name": "event.publish",
        "description": "Publish an event.",
        "inputSchema": {
            "type": "object",
            "properties": {"event_type": {"type": "string"}, "payload": {"type": "object"}},
            "required": ["event_type", "payload"],
        },
    },
    {
        "name": "event.poll",
        "description": "Poll events since timestamp.",
        "inputSchema": {"type": "object", "properties": {"since": {"type": "string"}, "limit": {"type": "integer"}}, "required": []},
    },
    # Cost layer
    {
        "name": "cost.report",
        "description": "Report cost usage.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "agent_id": {"type": "string"},
                "task_id": {"type": "string"},
                "workflow_id": {"type": "string"},
                "model": {"type": "string"},
                "provider": {"type": "string"},
                "tokens_input": {"type": "integer"},
                "tokens_output": {"type": "integer"},
                "tokens_cache_read": {"type": "integer"},
                "tokens_cache_write": {"type": "integer"},
                "estimated_cost_usd": {"type": "number"},
                "input_price_per_mtok": {"type": "number"},
                "output_price_per_mtok": {"type": "number"},
            },
            "required": ["agent_id", "model", "provider"],
        },
    },
    {
        "name": "cost.summary",
        "description": "Summarize costs.",
        "inputSchema": {"type": "object", "properties": {"period": {"type": "string"}, "group_by": {"type": "string"}}, "required": []},
    },
    {
        "name": "cost.budget",
        "description": "Get budget status.",
        "inputSchema": {"type": "object", "properties": {"agent_id": {"type": "string"}, "workflow_id": {"type": "string"}, "period": {"type": "string"}}, "required": []},
    },
    # Workflow layer
    {
        "name": "workflow.create",
        "description": "Create a workflow.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "description": {"type": "string"},
                "definition": {"type": "object"},
                "trigger_type": {"type": "string"},
                "trigger_config": {"type": "object"},
                "tags": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["name", "definition"],
        },
    },
    {
        "name": "workflow.start",
        "description": "Start a workflow.",
        "inputSchema": {"type": "object", "properties": {"workflow_id": {"type": "string"}, "inputs": {"type": "object"}}, "required": ["workflow_id"]},
    },
    {
        "name": "workflow.pause",
        "description": "Pause a workflow.",
        "inputSchema": {"type": "object", "properties": {"workflow_id": {"type": "string"}}, "required": ["workflow_id"]},
    },
    {
        "name": "workflow.resume",
        "description": "Resume a workflow.",
        "inputSchema": {"type": "object", "properties": {"workflow_id": {"type": "string"}}, "required": ["workflow_id"]},
    },
    {
        "name": "workflow.cancel",
        "description": "Cancel a workflow.",
        "inputSchema": {"type": "object", "properties": {"workflow_id": {"type": "string"}}, "required": ["workflow_id"]},
    },
    {
        "name": "workflow.status",
        "description": "Get workflow status.",
        "inputSchema": {"type": "object", "properties": {"workflow_id": {"type": "string"}}, "required": ["workflow_id"]},
    },
    {
        "name": "workflow.get",
        "description": "Get workflow definition.",
        "inputSchema": {"type": "object", "properties": {"workflow_id": {"type": "string"}}, "required": ["workflow_id"]},
    },
    {
        "name": "workflow.list",
        "description": "List workflows.",
        "inputSchema": {"type": "object", "properties": {"status": {"type": "string"}, "limit": {"type": "integer"}}, "required": []},
    },
]


WRITE_TOOLS = {
    "agent.register",
    "agent.heartbeat",
    "agent.capabilities",
    "agent.deregister",
    "task.create",
    "task.claim",
    "task.start",
    "task.complete",
    "task.fail",
    "task.cancel",
    "task.delegate",
    "artifact.put",
    "event.publish",
    "cost.report",
    "workflow.create",
    "workflow.start",
    "workflow.pause",
    "workflow.resume",
    "workflow.cancel",
}


def dispatch_tool(tool: str, arguments: Dict[str, Any], conn, config: Dict[str, Any], token):
    if tool == "agent.register":
        _require_any(token, {"agent.register", "admin"})
        result = register_agent(
            conn,
            config=config,
            name=arguments.get("name"),
            agent_type=arguments.get("agent_type"),
            capabilities=arguments.get("capabilities"),
            scopes=arguments.get("scopes"),
            metadata=arguments.get("metadata"),
            max_concurrent_tasks=int(arguments.get("max_concurrent_tasks") or 1),
            default_model=arguments.get("default_model"),
            model_provider=arguments.get("model_provider"),
            overwrite=bool(arguments.get("overwrite", False)),
        )
        publish_event(
            conn,
            event_type="agent.registered",
            agent_id=result.get("agent_id"),
            payload={"name": arguments.get("name")},
        )
        return result

    if tool == "agent.heartbeat":
        _require_any(token, {"agent.self", "admin"})
        agent_id = arguments.get("agent_id") or token.name
        success = heartbeat_agent(
            conn,
            agent_id=agent_id,
            status=arguments.get("status"),
        )
        return {"success": success}

    if tool == "agent.capabilities":
        _require_any(token, {"agent.self", "admin"})
        agent_id = arguments.get("agent_id") or token.name
        success = update_agent_capabilities(
            conn,
            agent_id=agent_id,
            capabilities=arguments.get("capabilities") or [],
        )
        return {"success": success}

    if tool == "agent.list":
        _require_any(token, {"agent.read", "admin"})
        return {"agents": list_agents(conn)}

    if tool == "agent.deregister":
        _require_any(token, {"agent.self", "admin"})
        agent_id = arguments.get("agent_id") or token.name
        success = deregister_agent(conn, agent_id=agent_id)
        if success:
            publish_event(
                conn,
                event_type="agent.deregistered",
                agent_id=agent_id,
                payload={},
            )
        return {"success": success}

    if tool == "task.create":
        _require_any(token, {"task.create", "admin"})
        return create_task(conn, **_task_args(arguments))

    if tool == "task.poll":
        _require_any(token, {"task.claim", "admin"})
        agent_id = token.name
        caps = token.capabilities or set()
        limit = int(arguments.get("limit", 5))
        return {"tasks": poll_tasks(conn, agent_id=agent_id, capabilities=caps, limit=limit)}

    if tool == "task.claim":
        _require_any(token, {"task.claim", "admin"})
        task = claim_task(conn, task_id=arguments.get("task_id"), agent_id=token.name)
        return {"task": task}

    if tool == "task.start":
        _require_any(token, {"task.execute", "admin"})
        success = start_task(conn, task_id=arguments.get("task_id"), agent_id=token.name)
        return {"success": success}

    if tool == "task.complete":
        _require_any(token, {"task.execute", "admin"})
        success = complete_task(
            conn,
            task_id=arguments.get("task_id"),
            agent_id=token.name,
            output_summary=arguments.get("output_summary"),
            output_artifact_id=arguments.get("output_artifact_id"),
            tokens_input=int(arguments.get("tokens_input") or 0),
            tokens_output=int(arguments.get("tokens_output") or 0),
            estimated_cost_usd=float(arguments.get("estimated_cost_usd") or 0.0),
            model_used=arguments.get("model_used"),
            config=config,
        )
        return {"success": success}

    if tool == "task.fail":
        _require_any(token, {"task.execute", "admin"})
        success = fail_task(
            conn,
            task_id=arguments.get("task_id"),
            agent_id=token.name,
            error_message=arguments.get("error_message"),
            config=config,
        )
        return {"success": success}

    if tool == "task.cancel":
        _require_any(token, {"task.manage", "admin"})
        success = cancel_task(
            conn,
            task_id=arguments.get("task_id"),
            reason=arguments.get("reason"),
            config=config,
        )
        return {"success": success}

    if tool == "task.delegate":
        _require_any(token, {"task.create", "admin"})
        task = delegate_task(
            conn,
            parent_task_id=arguments.get("parent_task_id"),
            name=arguments.get("name"),
            description=arguments.get("description"),
        )
        return {"task": task}

    if tool == "task.get":
        _require_any(token, {"task.read", "admin"})
        return {"task": get_task(conn, arguments.get("task_id"))}

    if tool == "task.list":
        _require_any(token, {"task.read", "admin"})
        return {
            "tasks": list_tasks(
                conn,
                status=arguments.get("status"),
                workflow_id=arguments.get("workflow_id"),
                agent_id=arguments.get("agent_id"),
                limit=int(arguments.get("limit", 50)),
            )
        }

    if tool == "artifact.put":
        _require_any(token, {"artifact.write", "admin"})
        result = artifact_put(
            conn,
            config=config,
            task_id=arguments.get("task_id"),
            name=arguments.get("name"),
            artifact_type=arguments.get("artifact_type"),
            content=arguments.get("content"),
            content_base64=arguments.get("content_base64"),
            content_url=arguments.get("content_url"),
            mime_type=arguments.get("mime_type"),
            metadata=arguments.get("metadata"),
            role=arguments.get("role", "output"),
        )
        return {"artifact": result}

    if tool == "artifact.get":
        _require_any(token, {"artifact.read", "admin"})
        artifact = artifact_get(
            conn,
            artifact_id=arguments.get("artifact_id"),
            include_content=bool(arguments.get("include_content", False)),
        )
        return {"artifact": artifact}

    if tool == "artifact.list":
        _require_any(token, {"artifact.read", "admin"})
        artifacts = artifact_list(
            conn,
            task_id=arguments.get("task_id"),
            workflow_id=arguments.get("workflow_id"),
        )
        return {"artifacts": artifacts}

    if tool == "event.publish":
        _require_any(token, {"event.write", "admin"})
        result = publish_event(
            conn,
            event_type=arguments.get("event_type"),
            payload=arguments.get("payload") or {},
            agent_id=arguments.get("agent_id"),
            task_id=arguments.get("task_id"),
            workflow_id=arguments.get("workflow_id"),
        )
        return result

    if tool == "event.poll":
        _require_any(token, {"event.read", "admin"})
        events = poll_events(conn, since=arguments.get("since"), limit=int(arguments.get("limit", 50)))
        return {"events": events}

    if tool == "cost.report":
        _require_any(token, {"cost.write", "admin"})
        result = report_cost(
            conn,
            agent_id=arguments.get("agent_id"),
            task_id=arguments.get("task_id"),
            workflow_id=arguments.get("workflow_id"),
            model=arguments.get("model"),
            provider=arguments.get("provider"),
            tokens_input=int(arguments.get("tokens_input") or 0),
            tokens_output=int(arguments.get("tokens_output") or 0),
            tokens_cache_read=int(arguments.get("tokens_cache_read") or 0),
            tokens_cache_write=int(arguments.get("tokens_cache_write") or 0),
            estimated_cost_usd=float(arguments.get("estimated_cost_usd") or 0.0),
            input_price_per_mtok=arguments.get("input_price_per_mtok"),
            output_price_per_mtok=arguments.get("output_price_per_mtok"),
        )
        return result

    if tool == "cost.summary":
        _require_any(token, {"cost.read", "admin"})
        return cost_summary(conn, period=arguments.get("period", "today"), group_by=arguments.get("group_by"))

    if tool == "cost.budget":
        _require_any(token, {"cost.read", "admin"})
        return cost_budget_status(
            conn,
            agent_id=arguments.get("agent_id"),
            workflow_id=arguments.get("workflow_id"),
            period=arguments.get("period", "today"),
            config=config,
        )

    if tool == "workflow.create":
        _require_any(token, {"workflow.create", "admin"})
        workflow = create_workflow(
            conn,
            name=arguments.get("name"),
            description=arguments.get("description"),
            definition=arguments.get("definition"),
            trigger_type=arguments.get("trigger_type", "manual"),
            trigger_config=arguments.get("trigger_config"),
            tags=arguments.get("tags"),
        )
        return {"workflow": workflow}

    if tool == "workflow.start":
        _require_any(token, {"workflow.manage", "admin"})
        workflow = start_workflow(
            conn,
            workflow_id=arguments.get("workflow_id"),
            inputs=arguments.get("inputs"),
        )
        return {"workflow": workflow}

    if tool == "workflow.pause":
        _require_any(token, {"workflow.manage", "admin"})
        success = pause_workflow(conn, workflow_id=arguments.get("workflow_id"))
        return {"success": success}

    if tool == "workflow.resume":
        _require_any(token, {"workflow.manage", "admin"})
        success = resume_workflow(conn, workflow_id=arguments.get("workflow_id"))
        return {"success": success}

    if tool == "workflow.cancel":
        _require_any(token, {"workflow.manage", "admin"})
        success = cancel_workflow(conn, workflow_id=arguments.get("workflow_id"))
        return {"success": success}

    if tool == "workflow.status":
        _require_any(token, {"workflow.read", "admin"})
        return {"workflow": workflow_status(conn, workflow_id=arguments.get("workflow_id"))}

    if tool == "workflow.get":
        _require_any(token, {"workflow.read", "admin"})
        return {"workflow": workflow_get(conn, arguments.get("workflow_id"))}

    if tool == "workflow.list":
        _require_any(token, {"workflow.read", "admin"})
        return {"workflows": workflow_list(conn, status=arguments.get("status"), limit=int(arguments.get("limit", 50)))}

    return None


def _require_any(token, scopes: set[str]) -> None:
    require_any_scope(token, scopes)


def _task_args(arguments: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": arguments.get("name"),
        "description": arguments.get("description"),
        "requires_capability": arguments.get("requires_capability"),
        "priority": int(arguments.get("priority") or 5),
        "input_data": arguments.get("input_data"),
        "input_artifact_ids": arguments.get("input_artifact_ids"),
        "workflow_id": arguments.get("workflow_id"),
        "workflow_step_id": arguments.get("workflow_step_id"),
        "depends_on": arguments.get("depends_on"),
    }

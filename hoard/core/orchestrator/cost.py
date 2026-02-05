from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from hoard.core.orchestrator.utils import now_iso


class CostError(Exception):
    pass


def report_cost(
    conn,
    *,
    agent_id: str,
    model: str,
    provider: str,
    tokens_input: int = 0,
    tokens_output: int = 0,
    tokens_cache_read: int = 0,
    tokens_cache_write: int = 0,
    estimated_cost_usd: float = 0.0,
    input_price_per_mtok: Optional[float] = None,
    output_price_per_mtok: Optional[float] = None,
    task_id: Optional[str] = None,
    workflow_id: Optional[str] = None,
) -> Dict[str, Any]:
    if not agent_id:
        raise CostError("agent_id is required")
    if not model or not provider:
        raise CostError("model and provider are required")

    conn.execute(
        """
        INSERT INTO cost_ledger
        (agent_id, task_id, workflow_id, model, provider,
         tokens_input, tokens_output, tokens_cache_read, tokens_cache_write,
         estimated_cost_usd, input_price_per_mtok, output_price_per_mtok, recorded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            agent_id,
            task_id,
            workflow_id,
            model,
            provider,
            int(tokens_input or 0),
            int(tokens_output or 0),
            int(tokens_cache_read or 0),
            int(tokens_cache_write or 0),
            float(estimated_cost_usd or 0.0),
            input_price_per_mtok,
            output_price_per_mtok,
            now_iso(),
        ),
    )
    return {"success": True}


def cost_summary(
    conn,
    *,
    period: str = "today",
    group_by: Optional[str] = None,
) -> Dict[str, Any]:
    start, end = _period_bounds(period)
    rows = conn.execute(
        """
        SELECT agent_id, model, SUM(tokens_input) AS tokens_input,
               SUM(tokens_output) AS tokens_output,
               SUM(estimated_cost_usd) AS cost_usd,
               COUNT(*) AS entries
        FROM cost_ledger
        WHERE recorded_at >= ? AND recorded_at < ?
        GROUP BY agent_id, model
        """,
        (start, end),
    ).fetchall()

    total = 0.0
    by_agent: Dict[str, Dict[str, Any]] = {}
    by_model: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        total += float(row["cost_usd"] or 0.0)
        agent = row["agent_id"]
        model = row["model"]
        by_agent.setdefault(
            agent,
            {"agent": agent, "tokens_input": 0, "tokens_output": 0, "cost_usd": 0.0, "entries": 0},
        )
        by_agent[agent]["tokens_input"] += int(row["tokens_input"] or 0)
        by_agent[agent]["tokens_output"] += int(row["tokens_output"] or 0)
        by_agent[agent]["cost_usd"] += float(row["cost_usd"] or 0.0)
        by_agent[agent]["entries"] += int(row["entries"] or 0)

        by_model.setdefault(model, {"model": model, "cost_usd": 0.0})
        by_model[model]["cost_usd"] += float(row["cost_usd"] or 0.0)

    return {
        "period": period,
        "total_usd": round(total, 4),
        "by_agent": list(by_agent.values()),
        "by_model": list(by_model.values()),
    }


def cost_budget_status(
    conn,
    *,
    agent_id: Optional[str] = None,
    workflow_id: Optional[str] = None,
    period: str = "today",
    config: Optional[dict] = None,
) -> Dict[str, Any]:
    config = config or {}
    budgets = _load_budget_config(config)

    start, end = _period_bounds(period)
    total = _sum_cost(conn, start, end)
    agent_total = _sum_cost(conn, start, end, agent_id=agent_id)
    workflow_total = _sum_cost(conn, start, end, workflow_id=workflow_id)

    return {
        "period": period,
        "global": {"spent": total, "budget": budgets.get("global_daily") if period == "today" else None},
        "agent": {"agent_id": agent_id, "spent": agent_total, "budget": budgets.get("per_agent_default")},
        "workflow": {
            "workflow_id": workflow_id,
            "spent": workflow_total,
            "budget": budgets.get("per_workflow_default"),
        },
    }


def _period_bounds(period: str) -> tuple[str, str]:
    now = datetime.utcnow()
    if period == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
    elif period == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
    else:
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
    return start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds")


def _sum_cost(conn, start: str, end: str, *, agent_id: str | None = None, workflow_id: str | None = None) -> float:
    sql = "SELECT COALESCE(SUM(estimated_cost_usd), 0) AS total FROM cost_ledger WHERE recorded_at >= ? AND recorded_at < ?"
    params: list[Any] = [start, end]
    if agent_id:
        sql += " AND agent_id = ?"
        params.append(agent_id)
    if workflow_id:
        sql += " AND workflow_id = ?"
        params.append(workflow_id)
    row = conn.execute(sql, params).fetchone()
    return float(row["total"] or 0.0) if row else 0.0


def _load_budget_config(config: dict) -> Dict[str, Any]:
    budgets = config.get("cost", {}).get("budgets", {})
    return {
        "per_agent_default": float(budgets.get("per_agent", {}).get("default", 0.0)),
        "per_workflow_default": float(budgets.get("per_workflow", {}).get("default", 0.0)),
        "global_daily": float(budgets.get("global", {}).get("daily", 0.0)),
    }

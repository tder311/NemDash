"""Conversational NEM analyst agent (OpenAI).

A streaming, tool-calling agent over the dashboard. The LLM orchestrates and
explains; it never computes a number itself — every figure comes from a
deterministic tool. Tools cover live data only (prices, generation, PASA).

Charts/tables: data-producing tools return a short text summary to the model
AND emit a renderable 'artifact' (line chart or table) to the UI as a
side-effect. The model picks the tool and narrates; the tool produces the
exact visual — the model never re-serializes data.

Design notes
------------
* Async OpenAI SDK; the streaming loop yields SSE-shaped dicts.
* OpenAI streams tool-call args as fragments, accumulated by index.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o")
MAX_TOKENS = 4096

_REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]


def _to_region_short(region: str) -> str:
    r = region.upper().strip()
    return r[:-1] if r.endswith("1") else r


# --------------------------------------------------------------------------- #
# Tool schemas (OpenAI function-tool format)
# --------------------------------------------------------------------------- #

TOOLS: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_latest_prices",
            "description": "Most recent regional reference price (RRP, $/MWh) and demand for every NEM region. Use for 'prices right now'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "price_type": {"type": "string", "enum": ["DISPATCH", "TRADING", "PUBLIC"]}
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_price_history",
            "description": "Historical regional prices over a recent window (up to 14 days). Use for trends or recent volatility.",
            "parameters": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "enum": _REGIONS},
                    "hours": {"type": "integer", "description": "Lookback hours (max 336)."},
                },
                "required": ["region", "hours"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_generation_mix",
            "description": "Current generation mix by fuel type for a region (MW and % share).",
            "parameters": {
                "type": "object",
                "properties": {"region": {"type": "string", "enum": _REGIONS}},
                "required": ["region"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_pasa_outlook",
            "description": "AEMO ST PASA demand & reserve adequacy forecast (~7 days): forecast demand, available capacity, surplus reserve. Use for system-tightness questions.",
            "parameters": {
                "type": "object",
                "properties": {"region": {"type": "string", "enum": _REGIONS}},
                "required": ["region"],
            },
        },
    },
]

SYSTEM_PROMPT = (
    "You are the NemDash analyst — an assistant for Australia's National "
    "Electricity Market (NEM). You answer questions about live prices, "
    "generation and system adequacy by calling the provided tools.\n\n"
    "Rules:\n"
    "- NEVER state a price, MW or date from memory. Every figure must "
    "come from a tool call in this conversation. If you don't have it, call the "
    "tool — don't guess.\n"
    "- Regions are NSW1, QLD1, VIC1, SA1, TAS1. Map 'NSW'/'South Australia' to "
    "the regionid.\n"
    "- Prices are $/MWh (can be negative or spike to ~$16,600). Demand/"
    "generation are MW.\n"
    "- Many tools draw a chart or table automatically — don't recite long lists "
    "of numbers the chart already shows; give the headline figures and insight, "
    "like a market desk note. Note the data's timestamp."
)


# --------------------------------------------------------------------------- #
# Tool dispatch — returns (summary_text, artifact|None)
# --------------------------------------------------------------------------- #

ToolResult = Tuple[str, Optional[Dict[str, Any]]]


async def _execute_tool(db, name: str, args: Dict[str, Any]) -> ToolResult:
    """Run one tool; return (text for the model, artifact for the UI or None)."""
    try:
        if name == "get_latest_prices":
            df = await db.get_latest_prices(args.get("price_type", "DISPATCH"))
            if df is None or df.empty:
                return "No price data available.", None
            rows = [
                {
                    "region": r["region"],
                    "price": round(float(r["price"]), 2),
                    "demand_mw": round(float(r["totaldemand"]), 1) if r.get("totaldemand") is not None else None,
                    "settlementdate": str(r["settlementdate"]),
                }
                for _, r in df.iterrows()
            ]
            return json.dumps(rows), None

        if name == "get_price_history":
            region = _to_region_short(args["region"])
            hours = min(int(args["hours"]), 336)
            end = datetime.now()
            df = await db.get_price_history(end - timedelta(hours=hours), end, region=region, price_type="PUBLIC")
            if df is None or df.empty:
                return f"No price history for {args['region']} in the last {hours}h.", None
            prices = df["price"].astype(float)
            summary = {
                "region": args["region"], "hours": hours, "n": int(len(prices)),
                "min": round(float(prices.min()), 2), "max": round(float(prices.max()), 2),
                "mean": round(float(prices.mean()), 2), "latest": round(float(prices.iloc[-1]), 2),
            }
            artifact = {
                "kind": "line", "title": f"{args['region']} price — last {hours}h",
                "x": [str(t) for t in df["settlementdate"]],
                "series": [{"name": "Price ($/MWh)", "y": [round(float(v), 2) for v in prices]}],
            }
            return json.dumps(summary), artifact

        if name == "get_generation_mix":
            region = _to_region_short(args["region"])
            df = await db.get_region_fuel_mix(region)
            if df is None or df.empty:
                return f"No generation data for {args['region']}.", None
            total = float(df["generation_mw"].sum())
            mix = [
                {"fuel": r["fuel_source"], "mw": round(float(r["generation_mw"]), 1),
                 "pct": round(100 * float(r["generation_mw"]) / total, 1) if total else 0}
                for _, r in df.iterrows()
            ]
            artifact = {
                "kind": "table", "title": f"{args['region']} generation mix",
                "columns": ["Fuel", "MW", "% share"],
                "rows": [[m["fuel"], m["mw"], m["pct"]] for m in mix],
            }
            return json.dumps({"region": args["region"], "total_mw": round(total, 1), "mix": mix}), artifact

        if name == "get_pasa_outlook":
            region = args["region"].upper()
            rows = await db.get_latest_stpasa(region)
            if not rows:
                return f"No ST PASA outlook for {region}.", None
            sample = [
                {"interval": str(r["interval_datetime"]), "demand50": r.get("demand50"),
                 "available_mw": r.get("aggregatecapacityavailable"), "surplus_reserve_mw": r.get("surplusreserve")}
                for r in rows[:: max(1, len(rows) // 12)]
            ]
            artifact = {
                "kind": "line", "title": f"{region} ST PASA outlook",
                "x": [str(r["interval_datetime"]) for r in rows],
                "series": [
                    {"name": "Forecast demand (MW)", "y": [r.get("demand50") for r in rows]},
                    {"name": "Available capacity (MW)", "y": [r.get("aggregatecapacityavailable") for r in rows]},
                ],
            }
            return json.dumps({"region": region, "run_datetime": str(rows[0]["run_datetime"]),
                               "intervals": len(rows), "sample": sample}), artifact

        return f"ERROR: unknown tool '{name}'.", None
    except Exception as e:  # noqa: BLE001
        return f"ERROR running {name}: {e}", None


# --------------------------------------------------------------------------- #
# Streaming tool-use loop
# --------------------------------------------------------------------------- #


def _sse(event: str, data: Dict[str, Any]) -> Dict[str, str]:
    return {"event": event, "data": json.dumps(data)}


async def stream_chat(
    client, db, messages: List[Dict[str, Any]], max_iters: int = 6
) -> AsyncGenerator[Dict[str, str], None]:
    """Run the agentic loop, yielding SSE events.

    Emits: 'tool', 'artifact' (chart/table from a tool), 'text', 'done', 'error'.
    """
    convo = [{"role": "system", "content": SYSTEM_PROMPT}] + messages

    for _ in range(max_iters):
        text_parts: List[str] = []
        tool_acc: Dict[int, Dict[str, str]] = {}
        usage = None
        try:
            stream = await client.chat.completions.create(
                model=MODEL, max_tokens=MAX_TOKENS, messages=convo,
                tools=TOOLS, stream=True, stream_options={"include_usage": True},
            )
            async for chunk in stream:
                if getattr(chunk, "usage", None) is not None:
                    usage = chunk.usage
                if not chunk.choices:
                    continue
                delta = chunk.choices[0].delta
                if getattr(delta, "content", None):
                    text_parts.append(delta.content)
                    yield _sse("text", {"text": delta.content})
                for tc in getattr(delta, "tool_calls", None) or []:
                    slot = tool_acc.setdefault(tc.index, {"id": "", "name": "", "args": ""})
                    if tc.id:
                        slot["id"] = tc.id
                    if tc.function and tc.function.name:
                        slot["name"] = tc.function.name
                    if tc.function and tc.function.arguments:
                        slot["args"] += tc.function.arguments
        except Exception as e:  # noqa: BLE001
            yield _sse("error", {"message": str(e)})
            return

        if not tool_acc:
            yield _sse("done", {
                "input_tokens": getattr(usage, "prompt_tokens", None),
                "output_tokens": getattr(usage, "completion_tokens", None),
                "cached_tokens": getattr(getattr(usage, "prompt_tokens_details", None), "cached_tokens", None),
            })
            return

        ordered = [tool_acc[i] for i in sorted(tool_acc)]
        convo.append({
            "role": "assistant", "content": "".join(text_parts) or None,
            "tool_calls": [
                {"id": t["id"], "type": "function",
                 "function": {"name": t["name"], "arguments": t["args"] or "{}"}}
                for t in ordered
            ],
        })

        for t in ordered:
            try:
                tool_args = json.loads(t["args"] or "{}")
            except json.JSONDecodeError:
                tool_args = {}
            yield _sse("tool", {"name": t["name"], "input": tool_args})
            summary, artifact = await _execute_tool(db, t["name"], tool_args)
            if artifact is not None:
                yield _sse("artifact", artifact)
            convo.append({"role": "tool", "tool_call_id": t["id"], "content": summary})

    yield _sse("error", {"message": f"Tool loop exceeded {max_iters} iterations."})

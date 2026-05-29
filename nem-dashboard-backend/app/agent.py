"""Conversational NEM analyst agent (OpenAI).

A streaming, tool-calling agent over the dashboard's read-only data. The LLM
orchestrates and explains; it never computes a number itself — every figure it
quotes comes from a deterministic tool that hits Postgres. v1 exposes
read-only tools only (prices, generation mix, PASA outlook); the heavier
compute tools (forecast, optimiser, bid bands) can be added later.

Design notes
------------
* Uses the async OpenAI SDK (FastAPI is async). The streaming loop yields
  SSE-shaped dicts so the endpoint can forward them verbatim.
* OpenAI streams tool-call arguments as string fragments — they're
  accumulated by tool-call index before dispatch.
* OpenAI auto-caches long prompt prefixes server-side (no explicit markers);
  the 'done' event reports cached_tokens so you can see it working.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, AsyncGenerator, Dict, List

MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o")
MAX_TOKENS = 4096

# NEM regions the agent knows about (regionid form).
_REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]


def _to_region_short(region: str) -> str:
    """Agent speaks regionid ('NSW1'); price_data stores 'NSW'."""
    r = region.upper().strip()
    return r[:-1] if r.endswith("1") else r


# --------------------------------------------------------------------------- #
# Tool schemas (read-only) — OpenAI function-tool format
# --------------------------------------------------------------------------- #

TOOLS: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_latest_prices",
            "description": (
                "Get the most recent regional reference price (RRP, $/MWh) and "
                "demand for every NEM region. Use for 'what are prices right now'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "price_type": {
                        "type": "string",
                        "enum": ["DISPATCH", "TRADING", "PUBLIC"],
                        "description": "DISPATCH=5-min spot (default), TRADING/PUBLIC=30-min.",
                    }
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_price_history",
            "description": (
                "Get historical regional prices over a recent window (up to 14 "
                "days). Use for trends or recent volatility."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "region": {"type": "string", "enum": _REGIONS},
                    "hours": {
                        "type": "integer",
                        "description": "Lookback window in hours (max 336 = 14 days).",
                    },
                },
                "required": ["region", "hours"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_generation_mix",
            "description": (
                "Get the current generation mix by fuel type for a region (MW and "
                "% share). Use for fuel split or renewables share."
            ),
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
            "description": (
                "Get AEMO's latest demand and reserve adequacy forecast (ST PASA, "
                "~7 days) for a region: forecast demand, available capacity, "
                "surplus reserve. Use for system-tightness / adequacy questions."
            ),
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
    "generation, and system adequacy by calling the provided tools.\n\n"
    "Rules:\n"
    "- NEVER state a price, MW figure, or date from memory. Every number you "
    "report must come from a tool call in this conversation. If a tool hasn't "
    "given you a figure, call the tool — don't guess.\n"
    "- Regions are NSW1, QLD1, VIC1, SA1, TAS1. If a user says 'NSW' or 'South "
    "Australia', map it to the regionid.\n"
    "- Prices are $/MWh and can be negative or spike to the market cap "
    "(~$16,600). Demand and generation are MW.\n"
    "- Be concise and analytical, like a market desk note. Lead with the "
    "answer, then the supporting figures. Note the timestamp of the data.\n"
    "- If a question is outside live NEM data (e.g. a price forecast or battery "
    "dispatch), say it's not yet wired into this chat and point to the "
    "Forecast / Dispatch / Bid Bands tabs."
)


# --------------------------------------------------------------------------- #
# Tool dispatch — deterministic, hits Postgres
# --------------------------------------------------------------------------- #


async def _execute_tool(db, name: str, args: Dict[str, Any]) -> str:
    """Run one tool against the DB and return a compact text result.

    Returns a string (JSON or a human-readable error) for the tool message.
    Errors are returned as text with a leading 'ERROR:' so the model can
    recover rather than the whole turn failing.
    """
    try:
        if name == "get_latest_prices":
            df = await db.get_latest_prices(args.get("price_type", "DISPATCH"))
            if df is None or df.empty:
                return "No price data available."
            rows = [
                {
                    "region": r["region"],
                    "price": round(float(r["price"]), 2),
                    "demand_mw": round(float(r["totaldemand"]), 1)
                    if r.get("totaldemand") is not None
                    else None,
                    "settlementdate": str(r["settlementdate"]),
                }
                for _, r in df.iterrows()
            ]
            return json.dumps(rows)

        if name == "get_price_history":
            region = _to_region_short(args["region"])
            hours = min(int(args["hours"]), 336)
            end = datetime.now()
            start = end - timedelta(hours=hours)
            df = await db.get_price_history(start, end, region=region, price_type="PUBLIC")
            if df is None or df.empty:
                return f"No price history for {args['region']} in the last {hours}h."
            prices = df["price"].astype(float)
            return json.dumps(
                {
                    "region": args["region"],
                    "hours": hours,
                    "n": int(len(prices)),
                    "min": round(float(prices.min()), 2),
                    "max": round(float(prices.max()), 2),
                    "mean": round(float(prices.mean()), 2),
                    "latest": round(float(prices.iloc[-1]), 2),
                }
            )

        if name == "get_generation_mix":
            region = _to_region_short(args["region"])
            df = await db.get_region_fuel_mix(region)
            if df is None or df.empty:
                return f"No generation data for {args['region']}."
            total = float(df["generation_mw"].sum())
            mix = [
                {
                    "fuel": r["fuel_source"],
                    "mw": round(float(r["generation_mw"]), 1),
                    "pct": round(100 * float(r["generation_mw"]) / total, 1) if total else 0,
                }
                for _, r in df.iterrows()
            ]
            return json.dumps({"region": args["region"], "total_mw": round(total, 1), "mix": mix})

        if name == "get_pasa_outlook":
            region = args["region"].upper()
            rows = await db.get_latest_stpasa(region)
            if not rows:
                return f"No ST PASA outlook for {region}."
            sample = [
                {
                    "interval": str(r["interval_datetime"]),
                    "demand50": r.get("demand50"),
                    "available_mw": r.get("aggregatecapacityavailable"),
                    "surplus_reserve_mw": r.get("surplusreserve"),
                }
                for r in rows[:: max(1, len(rows) // 12)]  # ~12 samples across the horizon
            ]
            return json.dumps(
                {
                    "region": region,
                    "run_datetime": str(rows[0]["run_datetime"]),
                    "intervals": len(rows),
                    "sample": sample,
                }
            )

        return f"ERROR: unknown tool '{name}'."
    except Exception as e:  # noqa: BLE001 - surface to model, don't crash the turn
        return f"ERROR running {name}: {e}"


# --------------------------------------------------------------------------- #
# Streaming tool-use loop
# --------------------------------------------------------------------------- #


def _sse(event: str, data: Dict[str, Any]) -> Dict[str, str]:
    """Shape one Server-Sent Event for the FastAPI StreamingResponse."""
    return {"event": event, "data": json.dumps(data)}


async def stream_chat(
    client, db, messages: List[Dict[str, Any]], max_iters: int = 6
) -> AsyncGenerator[Dict[str, str], None]:
    """Run the agentic loop, yielding SSE events as it goes.

    Emits: 'tool' (a tool is being called), 'text' (assistant token), 'done'
    (final, with usage), 'error'. ``messages`` is the running conversation
    (OpenAI chat format); the system prompt is prepended here, and tool turns
    are appended in place across iterations.
    """
    convo = [{"role": "system", "content": SYSTEM_PROMPT}] + messages

    for _ in range(max_iters):
        # Accumulators for this streamed completion.
        text_parts: List[str] = []
        tool_acc: Dict[int, Dict[str, str]] = {}  # index -> {id, name, args}
        usage = None
        try:
            stream = await client.chat.completions.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                messages=convo,
                tools=TOOLS,
                stream=True,
                stream_options={"include_usage": True},
            )
            async for chunk in stream:
                if getattr(chunk, "usage", None) is not None:
                    usage = chunk.usage
                if not chunk.choices:
                    continue  # usage-only final chunk
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

        # No tools requested -> we're done.
        if not tool_acc:
            yield _sse(
                "done",
                {
                    "input_tokens": getattr(usage, "prompt_tokens", None),
                    "output_tokens": getattr(usage, "completion_tokens", None),
                    "cached_tokens": getattr(
                        getattr(usage, "prompt_tokens_details", None), "cached_tokens", None
                    ),
                },
            )
            return

        # Persist the assistant turn with its tool calls (OpenAI shape).
        ordered = [tool_acc[i] for i in sorted(tool_acc)]
        convo.append(
            {
                "role": "assistant",
                "content": "".join(text_parts) or None,
                "tool_calls": [
                    {
                        "id": t["id"],
                        "type": "function",
                        "function": {"name": t["name"], "arguments": t["args"] or "{}"},
                    }
                    for t in ordered
                ],
            }
        )

        # Execute each tool, append a 'tool' message per call.
        for t in ordered:
            try:
                args = json.loads(t["args"] or "{}")
            except json.JSONDecodeError:
                args = {}
            yield _sse("tool", {"name": t["name"], "input": args})
            result = await _execute_tool(db, t["name"], args)
            convo.append({"role": "tool", "tool_call_id": t["id"], "content": result})

    yield _sse("error", {"message": f"Tool loop exceeded {max_iters} iterations."})

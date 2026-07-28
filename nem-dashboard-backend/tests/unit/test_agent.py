"""Unit tests for the conversational agent — no network / API key required.

A fake OpenAI streaming client and a fake DB exercise the tool-use loop:
text streaming, fragmented tool-call accumulation, tool-result feedback,
artifact emission, and termination.
"""

import json
import types

import pandas as pd
import pytest

from app import agent as nem_agent


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #


class FakeDB:
    async def get_latest_prices(self, price_type="DISPATCH"):
        return pd.DataFrame(
            [
                {"region": "NSW", "price": 92.5, "totaldemand": 7200.0,
                 "settlementdate": "2026-05-28 16:00:00"},
                {"region": "SA", "price": -15.0, "totaldemand": 1400.0,
                 "settlementdate": "2026-05-28 16:00:00"},
            ]
        )

    async def get_region_fuel_mix(self, region):
        return pd.DataFrame(
            [
                {"fuel_source": "Coal", "generation_mw": 4000.0},
                {"fuel_source": "Wind", "generation_mw": 1000.0},
            ]
        )

    async def get_price_history(self, start, end, region=None, price_type="PUBLIC"):
        idx = pd.date_range("2026-05-27 16:00:00", periods=4, freq="30min")
        return pd.DataFrame({"settlementdate": idx, "price": [40.0, 55.0, 30.0, 60.0]})

    async def get_latest_stpasa(self, region):
        idx = pd.date_range("2026-05-28 00:00:00", periods=4, freq="30min")
        return [
            {"interval_datetime": t, "demand50": 7000.0 + i, "aggregatecapacityavailable": 9000.0,
             "surplusreserve": 2000.0 - i, "run_datetime": "2026-05-28 00:00:00"}
            for i, t in enumerate(idx)
        ]


class EmptyDB:
    async def get_latest_prices(self, price_type="DISPATCH"):
        return pd.DataFrame()

    async def get_price_history(self, start, end, region=None, price_type="PUBLIC"):
        return pd.DataFrame()

    async def get_region_fuel_mix(self, region):
        return pd.DataFrame()

    async def get_latest_stpasa(self, region):
        return []


class BrokenDB:
    async def get_latest_prices(self, price_type="DISPATCH"):
        raise RuntimeError("db connection lost")


def _text_chunk(text):
    delta = types.SimpleNamespace(content=text, tool_calls=None)
    return types.SimpleNamespace(choices=[types.SimpleNamespace(delta=delta, finish_reason=None)], usage=None)


def _tool_fragment(index, *, id=None, name=None, args=None):
    fn = types.SimpleNamespace(name=name, arguments=args)
    tc = types.SimpleNamespace(index=index, id=id, function=fn)
    delta = types.SimpleNamespace(content=None, tool_calls=[tc])
    return types.SimpleNamespace(choices=[types.SimpleNamespace(delta=delta, finish_reason=None)], usage=None)


def _usage_chunk():
    usage = types.SimpleNamespace(prompt_tokens=100, completion_tokens=20,
                                  prompt_tokens_details=types.SimpleNamespace(cached_tokens=64))
    return types.SimpleNamespace(choices=[], usage=usage)


class _FakeStream:
    def __init__(self, chunks):
        self._chunks = chunks

    def __aiter__(self):
        async def gen():
            for c in self._chunks:
                yield c
        return gen()


class FakeCompletions:
    def __init__(self, scripted):
        self._scripted = list(scripted)
        self.calls = []

    async def create(self, **kwargs):
        self.calls.append(kwargs.get("messages"))
        return _FakeStream(self._scripted.pop(0))


class FakeClient:
    def __init__(self, scripted):
        self.chat = types.SimpleNamespace(completions=FakeCompletions(scripted))


async def _drain(gen):
    return [ev async for ev in gen]


# --------------------------------------------------------------------------- #
# Tool dispatch — now returns (summary, artifact)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_execute_latest_prices_no_artifact():
    summary, artifact = await nem_agent._execute_tool(FakeDB(), "get_latest_prices", {})
    data = json.loads(summary)
    assert {r["region"] for r in data} == {"NSW", "SA"}
    assert any(r["price"] == -15.0 for r in data)
    assert artifact is None  # latest-prices is a snapshot, no chart


@pytest.mark.asyncio
async def test_execute_generation_mix_emits_table_artifact():
    summary, artifact = await nem_agent._execute_tool(FakeDB(), "get_generation_mix", {"region": "NSW1"})
    data = json.loads(summary)
    assert data["total_mw"] == 5000.0
    assert artifact["kind"] == "table"
    assert artifact["columns"] == ["Fuel", "MW", "% share"]
    coal_row = next(r for r in artifact["rows"] if r[0] == "Coal")
    assert coal_row[2] == 80.0  # % share


@pytest.mark.asyncio
async def test_execute_unknown_tool_returns_error():
    summary, artifact = await nem_agent._execute_tool(FakeDB(), "nope", {})
    assert summary.startswith("ERROR") and artifact is None


@pytest.mark.asyncio
async def test_execute_latest_prices_empty_returns_message():
    summary, artifact = await nem_agent._execute_tool(EmptyDB(), "get_latest_prices", {})
    assert summary == "No price data available."
    assert artifact is None


@pytest.mark.asyncio
async def test_execute_price_history_emits_line_artifact():
    summary, artifact = await nem_agent._execute_tool(
        FakeDB(), "get_price_history", {"region": "NSW1", "hours": 24}
    )
    data = json.loads(summary)
    assert data["region"] == "NSW1"
    assert data["n"] == 4
    assert data["max"] == 60.0
    assert artifact["kind"] == "line"
    assert len(artifact["x"]) == 4


@pytest.mark.asyncio
async def test_execute_price_history_no_data():
    summary, artifact = await nem_agent._execute_tool(
        EmptyDB(), "get_price_history", {"region": "SA1", "hours": 24}
    )
    assert "No price history" in summary
    assert artifact is None


@pytest.mark.asyncio
async def test_execute_price_history_caps_hours():
    # hours above the 336 cap should be clamped, not raise.
    summary, _ = await nem_agent._execute_tool(
        FakeDB(), "get_price_history", {"region": "NSW1", "hours": 10000}
    )
    data = json.loads(summary)
    assert data["hours"] == 336


@pytest.mark.asyncio
async def test_execute_generation_mix_empty():
    summary, artifact = await nem_agent._execute_tool(EmptyDB(), "get_generation_mix", {"region": "QLD1"})
    assert "No generation data" in summary
    assert artifact is None


@pytest.mark.asyncio
async def test_execute_pasa_outlook_emits_line_artifact():
    summary, artifact = await nem_agent._execute_tool(FakeDB(), "get_pasa_outlook", {"region": "NSW1"})
    data = json.loads(summary)
    assert data["region"] == "NSW1"
    assert data["intervals"] == 4
    assert artifact["kind"] == "line"
    assert artifact["series"][0]["name"] == "Forecast demand (MW)"


@pytest.mark.asyncio
async def test_execute_pasa_outlook_no_data():
    summary, artifact = await nem_agent._execute_tool(EmptyDB(), "get_pasa_outlook", {"region": "NSW1"})
    assert "No ST PASA outlook" in summary
    assert artifact is None


@pytest.mark.asyncio
async def test_execute_tool_catches_exception():
    summary, artifact = await nem_agent._execute_tool(BrokenDB(), "get_latest_prices", {})
    assert summary.startswith("ERROR running get_latest_prices")
    assert artifact is None


# --------------------------------------------------------------------------- #
# Streaming loop
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_loop_streams_text_then_done():
    client = FakeClient([[_text_chunk("Prices "), _text_chunk("are calm."), _usage_chunk()]])
    events = await _drain(nem_agent.stream_chat(client, FakeDB(), [{"role": "user", "content": "hi"}]))
    assert [e["event"] for e in events] == ["text", "text", "done"]
    assert json.loads(events[-1]["data"])["cached_tokens"] == 64


@pytest.mark.asyncio
async def test_loop_emits_artifact_around_tool():
    # Fragmented tool call for generation mix (which emits a table artifact).
    iter1 = [
        _tool_fragment(0, id="c1", name="get_generation_mix"),
        _tool_fragment(0, args='{"region":'),
        _tool_fragment(0, args=' "NSW1"}'),
        _usage_chunk(),
    ]
    iter2 = [_text_chunk("Coal dominates."), _usage_chunk()]
    client = FakeClient([iter1, iter2])
    events = await _drain(nem_agent.stream_chat(client, FakeDB(), [{"role": "user", "content": "mix?"}]))

    kinds = [e["event"] for e in events]
    assert kinds.index("tool") < kinds.index("artifact")  # artifact follows the tool call
    art = json.loads(next(e for e in events if e["event"] == "artifact")["data"])
    assert art["kind"] == "table"
    # second model call must include the tool result
    assert any(m["role"] == "tool" for m in client.chat.completions.calls[1])


@pytest.mark.asyncio
async def test_loop_runaway_guard():
    def tool_iter():
        return [_tool_fragment(0, id="t", name="get_latest_prices", args="{}"), _usage_chunk()]
    client = FakeClient([tool_iter() for _ in range(3)])
    events = await _drain(nem_agent.stream_chat(client, FakeDB(), [{"role": "user", "content": "x"}], max_iters=3))
    assert events[-1]["event"] == "error"


class _FailingCompletions:
    async def create(self, **kwargs):
        raise RuntimeError("upstream API down")


class _FailingClient:
    def __init__(self):
        self.chat = types.SimpleNamespace(completions=_FailingCompletions())


@pytest.mark.asyncio
async def test_loop_yields_error_on_api_exception():
    events = await _drain(
        nem_agent.stream_chat(_FailingClient(), FakeDB(), [{"role": "user", "content": "hi"}])
    )
    assert events[-1]["event"] == "error"
    assert "upstream API down" in events[-1]["data"]


@pytest.mark.asyncio
async def test_loop_handles_malformed_tool_json():
    # Args fragment is invalid JSON; the loop should fall back to {} instead of raising.
    iter1 = [
        _tool_fragment(0, id="c1", name="get_latest_prices"),
        _tool_fragment(0, args='{not valid json'),
        _usage_chunk(),
    ]
    iter2 = [_text_chunk("done"), _usage_chunk()]
    client = FakeClient([iter1, iter2])
    events = await _drain(
        nem_agent.stream_chat(client, FakeDB(), [{"role": "user", "content": "prices?"}])
    )
    tool_event = next(e for e in events if e["event"] == "tool")
    assert json.loads(tool_event["data"])["input"] == {}

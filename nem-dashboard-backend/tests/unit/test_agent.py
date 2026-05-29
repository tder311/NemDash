"""Unit tests for the conversational agent — no network / API key required.

A fake OpenAI streaming client and a fake DB exercise the tool-use loop:
text streaming, fragmented tool-call accumulation, tool-result feedback, and
termination.
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


def _text_chunk(text):
    delta = types.SimpleNamespace(content=text, tool_calls=None)
    choice = types.SimpleNamespace(delta=delta, finish_reason=None)
    return types.SimpleNamespace(choices=[choice], usage=None)


def _tool_fragment(index, *, id=None, name=None, args=None):
    fn = types.SimpleNamespace(name=name, arguments=args)
    tc = types.SimpleNamespace(index=index, id=id, function=fn)
    delta = types.SimpleNamespace(content=None, tool_calls=[tc])
    choice = types.SimpleNamespace(delta=delta, finish_reason=None)
    return types.SimpleNamespace(choices=[choice], usage=None)


def _usage_chunk():
    usage = types.SimpleNamespace(
        prompt_tokens=100,
        completion_tokens=20,
        prompt_tokens_details=types.SimpleNamespace(cached_tokens=64),
    )
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
        self._scripted = list(scripted)  # list of chunk-lists, one per iteration
        self.calls = []  # records the `messages` passed on each create()

    async def create(self, **kwargs):
        self.calls.append(kwargs.get("messages"))
        return _FakeStream(self._scripted.pop(0))


class FakeClient:
    def __init__(self, scripted):
        self.chat = types.SimpleNamespace(completions=FakeCompletions(scripted))


async def _drain(gen):
    return [ev async for ev in gen]


# --------------------------------------------------------------------------- #
# Tool dispatch (unchanged DB logic)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_execute_get_latest_prices():
    out = await nem_agent._execute_tool(FakeDB(), "get_latest_prices", {})
    data = json.loads(out)
    assert {r["region"] for r in data} == {"NSW", "SA"}
    assert any(r["price"] == -15.0 for r in data)  # negative prices preserved


@pytest.mark.asyncio
async def test_execute_generation_mix_percentages():
    out = await nem_agent._execute_tool(FakeDB(), "get_generation_mix", {"region": "NSW1"})
    data = json.loads(out)
    assert data["total_mw"] == 5000.0
    coal = next(m for m in data["mix"] if m["fuel"] == "Coal")
    assert coal["pct"] == 80.0


@pytest.mark.asyncio
async def test_execute_unknown_tool_returns_error_text():
    out = await nem_agent._execute_tool(FakeDB(), "nonexistent", {})
    assert out.startswith("ERROR")


# --------------------------------------------------------------------------- #
# Streaming loop
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_loop_streams_text_then_done_when_no_tool():
    client = FakeClient([[_text_chunk("Prices "), _text_chunk("are calm."), _usage_chunk()]])
    messages = [{"role": "user", "content": "summarise"}]
    events = await _drain(nem_agent.stream_chat(client, FakeDB(), messages))

    kinds = [e["event"] for e in events]
    assert kinds == ["text", "text", "done"]
    done = json.loads(events[-1]["data"])
    assert done["cached_tokens"] == 64  # usage surfaced


@pytest.mark.asyncio
async def test_loop_accumulates_fragmented_tool_call_then_continues():
    # Iteration 1: a tool call whose name + JSON args arrive in fragments.
    iter1 = [
        _tool_fragment(0, id="call_1", name="get_latest_prices"),
        _tool_fragment(0, args='{"price_'),
        _tool_fragment(0, args='type": "DISPATCH"}'),
        _usage_chunk(),
    ]
    # Iteration 2: the model answers in text.
    iter2 = [_text_chunk("NSW1 is $92.50/MWh."), _usage_chunk()]
    client = FakeClient([iter1, iter2])
    messages = [{"role": "user", "content": "price in NSW?"}]
    events = await _drain(nem_agent.stream_chat(client, FakeDB(), messages))

    kinds = [e["event"] for e in events]
    assert "tool" in kinds and kinds[-1] == "done"
    tool_ev = next(e for e in events if e["event"] == "tool")
    payload = json.loads(tool_ev["data"])
    assert payload["name"] == "get_latest_prices"
    assert payload["input"] == {"price_type": "DISPATCH"}  # fragments reassembled + parsed


@pytest.mark.asyncio
async def test_second_call_includes_tool_result_and_system_prompt():
    iter1 = [_tool_fragment(0, id="call_x", name="get_latest_prices", args="{}"), _usage_chunk()]
    iter2 = [_text_chunk("done"), _usage_chunk()]
    client = FakeClient([iter1, iter2])
    await _drain(nem_agent.stream_chat(client, FakeDB(), [{"role": "user", "content": "go"}]))

    completions = client.chat.completions
    assert len(completions.calls) == 2  # one tool round-trip, then the answer
    first_call = completions.calls[0]
    assert first_call[0]["role"] == "system"  # system prompt prepended
    second_call = completions.calls[1]
    roles = [m["role"] for m in second_call]
    assert "assistant" in roles and "tool" in roles  # tool result fed back
    tool_msg = next(m for m in second_call if m["role"] == "tool")
    assert tool_msg["tool_call_id"] == "call_x"


@pytest.mark.asyncio
async def test_loop_emits_error_on_runaway():
    def tool_iter():
        return [_tool_fragment(0, id="t", name="get_latest_prices", args="{}"), _usage_chunk()]
    client = FakeClient([tool_iter() for _ in range(3)])
    messages = [{"role": "user", "content": "loop"}]
    events = await _drain(nem_agent.stream_chat(client, FakeDB(), messages, max_iters=3))
    assert events[-1]["event"] == "error"

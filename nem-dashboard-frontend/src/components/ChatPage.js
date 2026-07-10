import React, { useState, useRef, useEffect } from 'react';
import Plot from 'react-plotly.js';
import { baseLayout, chartColors } from '../theme';
import './ChatPage.css';

const SUGGESTIONS = [
  'What are prices across the NEM right now?',
  "What's the price forecast for NSW1 this week?",
  'I have a 100MW 2hr battery in NSW1 — what should my bid bands be?',
  'How should I dispatch a 100MW 2hr battery in SA1 and how much could it earn?',
];

const API_BASE = process.env.REACT_APP_API_URL || '';

// Render one tool artifact: a line chart (Plotly) or a table.
function ChatArtifact({ artifact, darkMode }) {
  if (artifact.kind === 'line') {
    const data = artifact.series.map((s, i) => ({
      x: artifact.x.map((t) => new Date(t)),
      y: s.y,
      name: s.name,
      type: 'scatter',
      mode: 'lines',
      line: { width: 2, shape: 'hv' },
      yaxis: s.axis === 'right' ? 'y2' : 'y',
    }));
    const hasRight = artifact.series.some((s) => s.axis === 'right');
    const base = baseLayout(darkMode);
    const c = chartColors(darkMode);
    const layout = {
      ...base,
      title: { text: artifact.title, font: { size: 14, color: c.text } },
      height: 320,
      margin: { l: 50, r: hasRight ? 50 : 20, t: 36, b: 40 },
      xaxis: { ...base.xaxis },
      yaxis: { ...base.yaxis },
      ...(hasRight && { yaxis2: { overlaying: 'y', side: 'right', color: c.text2, showgrid: false } }),
      font: { ...base.font, size: 11 },
      legend: { orientation: 'h', y: -0.2 },
      showlegend: artifact.series.length > 1,
    };
    return (
      <div className="chat-artifact">
        <Plot data={data} layout={layout} useResizeHandler style={{ width: '100%' }}
          config={{ displayModeBar: false, responsive: true }} />
      </div>
    );
  }
  if (artifact.kind === 'table') {
    return (
      <div className="chat-artifact">
        <div className="chat-artifact-title">{artifact.title}</div>
        <table className="chat-table">
          <thead>
            <tr>{artifact.columns.map((c) => <th key={c}>{c}</th>)}</tr>
          </thead>
          <tbody>
            {artifact.rows.map((row, i) => (
              <tr key={i}>{row.map((cell, j) => <td key={j}>{cell}</td>)}</tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }
  return null;
}

function ChatPage({ darkMode }) {
  const [messages, setMessages] = useState([]); // {role, content, artifacts?}
  const [input, setInput] = useState('');
  const [streaming, setStreaming] = useState(false);
  const [toolStatus, setToolStatus] = useState(null);
  const [error, setError] = useState(null);
  const scrollRef = useRef(null);

  useEffect(() => {
    scrollRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, toolStatus]);

  const send = async (text) => {
    const question = (text ?? input).trim();
    if (!question || streaming) return;
    setError(null);
    setInput('');

    // History sent to the backend is text-only {role, content}.
    const history = [...messages.map((m) => ({ role: m.role, content: m.content })),
      { role: 'user', content: question }];
    setMessages((prev) => [...prev,
      { role: 'user', content: question },
      { role: 'assistant', content: '', artifacts: [] }]);
    setStreaming(true);

    try {
      const resp = await fetch(`${API_BASE}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ messages: history }),
      });
      if (!resp.ok) {
        const detail = await resp.json().catch(() => ({}));
        throw new Error(detail.detail || `Request failed (${resp.status})`);
      }

      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      const patchLast = (fn) =>
        setMessages((prev) => {
          const next = [...prev];
          next[next.length - 1] = fn(next[next.length - 1]);
          return next;
        });

      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const blocks = buffer.split('\n\n');
        buffer = blocks.pop();

        for (const block of blocks) {
          const evLine = block.split('\n').find((l) => l.startsWith('event:'));
          const dataLine = block.split('\n').find((l) => l.startsWith('data:'));
          if (!evLine || !dataLine) continue;
          const event = evLine.slice(6).trim();
          const data = JSON.parse(dataLine.slice(5).trim());

          if (event === 'text') {
            setToolStatus(null);
            patchLast((m) => ({ ...m, content: m.content + data.text }));
          } else if (event === 'tool') {
            setToolStatus(`Running ${data.name}…`);
          } else if (event === 'artifact') {
            setToolStatus(null);
            patchLast((m) => ({ ...m, artifacts: [...(m.artifacts || []), data] }));
          } else if (event === 'error') {
            setError(data.message);
          }
        }
      }
    } catch (e) {
      setError(e.message);
    } finally {
      setStreaming(false);
      setToolStatus(null);
    }
  };

  const onKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      send();
    }
  };

  return (
    <div className={`chat-container ${darkMode ? 'dark' : 'light'}`}>
      <div className="chat-log">
        {messages.length === 0 && (
          <div className="chat-empty">
            <p>Ask the NemDash analyst about live prices, forecasts, battery dispatch, and bid bands.</p>
            <div className="chat-suggestions">
              {SUGGESTIONS.map((s) => (
                <button key={s} className="chat-suggestion" onClick={() => send(s)}>{s}</button>
              ))}
            </div>
          </div>
        )}

        {messages.map((m, i) => (
          <div key={i} className={`chat-msg ${m.role} ${(m.artifacts || []).length ? 'has-artifact' : ''}`}>
            <div className="chat-bubble">
              {m.content || (m.role === 'assistant' && streaming
                ? <span className="chat-cursor">▋</span> : '')}
              {(m.artifacts || []).map((a, j) => (
                <ChatArtifact key={j} artifact={a} darkMode={darkMode} />
              ))}
            </div>
          </div>
        ))}

        {toolStatus && <div className="chat-tool-status">{toolStatus}</div>}
        {error && <div className="chat-error">{error}</div>}
        <div ref={scrollRef} />
      </div>

      <div className="chat-inputbar">
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={onKeyDown}
          placeholder="Ask about prices, forecasts, dispatch, or bid bands…"
          rows={2}
          disabled={streaming}
        />
        <button onClick={() => send()} disabled={streaming || !input.trim()}>
          {streaming ? '…' : 'Send'}
        </button>
      </div>
    </div>
  );
}

export default ChatPage;

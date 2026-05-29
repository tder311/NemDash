import React, { useState, useRef, useEffect } from 'react';
import './ChatPage.css';

const SUGGESTIONS = [
  'What are prices across the NEM right now?',
  'How have NSW1 prices moved over the last 24 hours?',
  "What's the generation mix in SA1?",
  'Is the QLD1 system tight this week?',
];

// API base mirrors the axios client (proxy in dev, REACT_APP_API_URL in prod).
const API_BASE = process.env.REACT_APP_API_URL || '';

function ChatPage({ darkMode }) {
  const [messages, setMessages] = useState([]); // {role, content} (content = string)
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

    const history = [...messages, { role: 'user', content: question }];
    setMessages([...history, { role: 'assistant', content: '' }]);
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

      // Parse the SSE stream: blocks separated by \n\n, each "event:" + "data:".
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const blocks = buffer.split('\n\n');
        buffer = blocks.pop(); // keep the trailing partial block

        for (const block of blocks) {
          const evLine = block.split('\n').find((l) => l.startsWith('event:'));
          const dataLine = block.split('\n').find((l) => l.startsWith('data:'));
          if (!evLine || !dataLine) continue;
          const event = evLine.slice(6).trim();
          const data = JSON.parse(dataLine.slice(5).trim());

          if (event === 'text') {
            setToolStatus(null);
            setMessages((prev) => {
              const next = [...prev];
              next[next.length - 1] = {
                role: 'assistant',
                content: next[next.length - 1].content + data.text,
              };
              return next;
            });
          } else if (event === 'tool') {
            setToolStatus(`Calling ${data.name}…`);
          } else if (event === 'error') {
            setError(data.message);
          }
          // 'done' carries usage; nothing to render.
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
            <p>Ask the NemDash analyst about live NEM prices, generation, and system adequacy.</p>
            <div className="chat-suggestions">
              {SUGGESTIONS.map((s) => (
                <button key={s} className="chat-suggestion" onClick={() => send(s)}>
                  {s}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((m, i) => (
          <div key={i} className={`chat-msg ${m.role}`}>
            <div className="chat-bubble">
              {m.content || (m.role === 'assistant' && streaming ? <span className="chat-cursor">▋</span> : '')}
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
          placeholder="Ask about NEM prices, generation, or reserve outlook…"
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

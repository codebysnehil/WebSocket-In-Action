import { useState, useEffect, useRef, useCallback } from "react";
import "./App.css";

const SOCKET_URL = "wss://websocket-project-3.onrender.com/";
const MAX_RECONNECT_ATTEMPTS = 6;
const AUTO_ENTER_DELAY_MS = 950;
const SKIP_BUTTON_DELAY_MS = 1200;

type ConnectionState = "connecting" | "open" | "reconnecting" | "offline";
type EventType =
  | "connecting"
  | "connected"
  | "reconnecting"
  | "offline"
  | "manual-retry";

interface ChatMessage {
  id: string;
  text: string;
  self: boolean;
  timestamp: number;
}

interface ConnEvent {
  id: string;
  type: EventType;
  timestamp: number;
  detail?: string;
}

function formatClock(ts: number): string {
  const d = new Date(ts);
  return d.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatDuration(seconds: number): string {
  const m = Math.floor(seconds / 60)
    .toString()
    .padStart(2, "0");
  const s = Math.floor(seconds % 60)
    .toString()
    .padStart(2, "0");
  return `${m}:${s}`;
}

function formatRelative(ts: number): string {
  const diff = Math.max(0, Math.floor((Date.now() - ts) / 1000));
  if (diff < 5) return "just now";
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

function makeId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

/* ---------------- Icons ---------------- */

function SendIcon() {
  return (
    <svg viewBox="0 0 20 20" width="16" height="16" aria-hidden="true">
      <path
        d="M3 10h11m0 0-4.5-4.5M14 10l-4.5 4.5"
        stroke="currentColor"
        strokeWidth="1.7"
        fill="none"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function ShieldIcon() {
  return (
    <svg viewBox="0 0 16 16" width="11" height="11" aria-hidden="true">
      <path
        d="M8 1.6 13 3.4v3.9c0 3.4-2.1 5.9-5 7.1-2.9-1.2-5-3.7-5-7.1V3.4L8 1.6Z"
        stroke="currentColor"
        strokeWidth="1.2"
        fill="none"
        strokeLinejoin="round"
      />
      <path
        d="M5.6 8.1 7.3 9.8l3.1-3.4"
        stroke="currentColor"
        strokeWidth="1.2"
        fill="none"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function SettingsIcon() {
  const common = {
    stroke: "currentColor",
    strokeWidth: 1.6,
    fill: "none",
    strokeLinecap: "round" as const,
    strokeLinejoin: "round" as const,
  };
  return (
    <svg viewBox="0 0 18 18" width="16" height="16">
      <circle cx="9" cy="9" r="2.4" {...common} />
      <path
        d="M9 2.8v1.6M9 13.6v1.6M15.2 9h-1.6M4.4 9H2.8M13.2 4.8l-1.1 1.1M5.9 12.1l-1.1 1.1M13.2 13.2l-1.1-1.1M5.9 5.9 4.8 4.8"
        {...common}
      />
    </svg>
  );
}

function Orb({ state, size = 108 }: { state: ConnectionState; size?: number }) {
  return (
    <div
      className={`orb orb-${state}`}
      style={{ width: size, height: size }}
      role="img"
      aria-label={`Connection: ${state}`}
    >
      <span className="orb-glow" />
      <span className="orb-ring" />
      <span className="orb-core" />
    </div>
  );
}

function statusWord(state: ConnectionState, attempt: number): string {
  switch (state) {
    case "connecting":
      return "Connecting";
    case "open":
      return "Live";
    case "reconnecting":
      return `Reconnecting (${attempt})`;
    case "offline":
      return "Offline";
  }
}

function eventLabel(e: ConnEvent): string {
  switch (e.type) {
    case "connecting":
      return "Opening connection";
    case "connected":
      return "Connected";
    case "reconnecting":
      return e.detail ? `Reconnect attempt ${e.detail}` : "Reconnecting";
    case "offline":
      return "Connection lost";
    case "manual-retry":
      return "Manual reconnect requested";
  }
}

const NAV_ITEMS = [{ id: "settings", label: "Settings" }];

type Theme = "dark" | "light";

function App() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [events, setEvents] = useState<ConnEvent[]>([]);
  const [currentMessage, setCurrentMessage] = useState<string>("");
  const [connectionState, setConnectionState] =
    useState<ConnectionState>("connecting");
  const [reconnectAttempt, setReconnectAttempt] = useState(0);
  const [uptimeSeconds, setUptimeSeconds] = useState(0);
  const [entered, setEntered] = useState(false);
  const [showSkip, setShowSkip] = useState(false);
  const [justSent, setJustSent] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [theme, setTheme] = useState<Theme>(() => {
    if (typeof window === "undefined") return "dark";
    return (localStorage.getItem("relay-theme") as Theme) || "dark";
  });
  const [, forceTick] = useState(0);

  const socketRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const chatWindowRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const attemptRef = useRef(0);
  const unmountedRef = useRef(false);
  const connectStartRef = useRef<number>(0);
  const connectedAtRef = useRef<number | null>(null);
  const autoEnterTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  // Bumped every time we open a new socket. Any event from an older, stale
  // socket (e.g. a leftover from React StrictMode's dev-only double-mount)
  // is ignored instead of being able to append a duplicate message.
  const connGenerationRef = useRef(0);
  // Messages we just sent, waiting to see if the server echoes them back to
  // us. If it does, we treat that as confirmation rather than a new incoming
  // message — otherwise every send shows up twice.
  const pendingSentRef = useRef<{ id: string; text: string; ts: number }[]>([]);

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    try {
      localStorage.setItem("relay-theme", theme);
    } catch {
      // ignore storage errors (e.g. private browsing)
    }
  }, [theme]);

  const pushEvent = useCallback((type: EventType, detail?: string) => {
    setEvents((prev) =>
      [{ id: makeId(), type, timestamp: Date.now(), detail }, ...prev].slice(
        0,
        20,
      ),
    );
  }, []);

  const connect = useCallback(() => {
    const myGen = ++connGenerationRef.current;
    const isStale = () => connGenerationRef.current !== myGen;

    setConnectionState(
      attemptRef.current === 0 ? "connecting" : "reconnecting",
    );
    if (attemptRef.current === 0) pushEvent("connecting");
    connectStartRef.current = performance.now();

    const ws = new WebSocket(SOCKET_URL);
    socketRef.current = ws;

    ws.onopen = () => {
      if (isStale()) {
        ws.close();
        return;
      }
      attemptRef.current = 0;
      setReconnectAttempt(0);
      connectedAtRef.current = Date.now();
      setConnectionState("open");
      pushEvent("connected");
    };

    ws.onmessage = (event) => {
      if (isStale()) return;
      try {
        const parsed = JSON.parse(event.data);
        const text = parsed.message;

        // If this is the echo of a message we just sent ourselves, treat it
        // as delivery confirmation instead of a brand-new incoming message.
        const pending = pendingSentRef.current;
        const matchIndex = pending.findIndex(
          (p) => p.text === text && Date.now() - p.ts < 4000,
        );
        if (matchIndex !== -1) {
          pending.splice(matchIndex, 1);
          return;
        }

        setMessages((prev) => [
          ...prev,
          { id: makeId(), text, self: false, timestamp: Date.now() },
        ]);
      } catch (err) {
        console.error("Error parsing message:", err);
      }
    };

    ws.onclose = () => {
      if (isStale() || unmountedRef.current) return;
      connectedAtRef.current = null;
      if (attemptRef.current < MAX_RECONNECT_ATTEMPTS) {
        const nextAttempt = attemptRef.current + 1;
        attemptRef.current = nextAttempt;
        setReconnectAttempt(nextAttempt);
        setConnectionState("reconnecting");
        pushEvent("reconnecting", String(nextAttempt));
        const delay = Math.min(1000 * 2 ** (nextAttempt - 1), 15000);
        reconnectTimer.current = setTimeout(connect, delay);
      } else {
        setConnectionState("offline");
        pushEvent("offline");
      }
    };

    ws.onerror = () => {
      ws.close();
    };
  }, [pushEvent]);

  useEffect(() => {
    unmountedRef.current = false;
    connect();
    return () => {
      unmountedRef.current = true;
      connGenerationRef.current += 1; // invalidate any in-flight socket immediately
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
      if (autoEnterTimer.current) clearTimeout(autoEnterTimer.current);
      socketRef.current?.close();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (connectionState === "open" && !entered) {
      autoEnterTimer.current = setTimeout(
        () => setEntered(true),
        AUTO_ENTER_DELAY_MS,
      );
    }
    return () => {
      if (autoEnterTimer.current) clearTimeout(autoEnterTimer.current);
    };
  }, [connectionState, entered]);

  useEffect(() => {
    const t = setTimeout(() => setShowSkip(true), SKIP_BUTTON_DELAY_MS);
    return () => clearTimeout(t);
  }, []);

  useEffect(() => {
    const t = setInterval(() => {
      setUptimeSeconds(
        connectedAtRef.current
          ? (Date.now() - connectedAtRef.current) / 1000
          : 0,
      );
      forceTick((n) => n + 1);
    }, 1000);
    return () => clearInterval(t);
  }, []);

  useEffect(() => {
    if (chatWindowRef.current) {
      chatWindowRef.current.scrollTop = chatWindowRef.current.scrollHeight;
    }
  }, [messages]);

  const handleSendMessage = () => {
    const text = currentMessage.trim();
    const ws = socketRef.current;
    if (!text || !ws || ws.readyState !== WebSocket.OPEN) return;

    const localId = makeId();
    pendingSentRef.current.push({ id: localId, text, ts: Date.now() });
    ws.send(JSON.stringify({ message: text }));
    setMessages((prev) => [
      ...prev,
      { id: localId, text, self: true, timestamp: Date.now() },
    ]);
    setCurrentMessage("");
    inputRef.current?.focus();
    setJustSent(true);
    setTimeout(() => setJustSent(false), 260);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") handleSendMessage();
  };

  const handleRetryNow = () => {
    attemptRef.current = 0;
    setReconnectAttempt(0);
    pushEvent("manual-retry");
    connect();
  };

  const isLive = connectionState === "open";

  return (
    <div className={`app-shell ${entered ? "is-entered" : ""}`}>
      {/* ---------- Welcome sequence ---------- */}
      <div
        className={`welcome ${entered ? "welcome-exit" : ""}`}
        aria-hidden={entered}
      >
        <div className="welcome-aurora" aria-hidden="true" />
        <div className="welcome-field" aria-hidden="true" />
        <div className="welcome-content">
          <Orb state={connectionState} />
          <div className="welcome-wordmark">Relay</div>
          <p className="welcome-tagline">
            A quieter place for real-time conversation.
          </p>
          <div className={`welcome-state status-${connectionState}`}>
            <span className="welcome-state-dot" />
            {statusWord(connectionState, reconnectAttempt)}
          </div>
          {showSkip && (
            <button
              className="welcome-skip"
              type="button"
              onClick={() => setEntered(true)}
            >
              <span>Enter</span>
              <SendIcon />
            </button>
          )}
        </div>
      </div>

      {/* ---------- Product shell ---------- */}
      <div className="product">
        <aside className="sidebar">
          <div className="sidebar-brand">
            <span className="brand-mark" />
            <span className="brand-name">Relay</span>
          </div>
          <nav className="sidebar-nav">
            {NAV_ITEMS.map((item) => (
              <button
                key={item.id}
                type="button"
                className={`nav-item ${settingsOpen ? "nav-item-active" : ""}`}
                onClick={() => setSettingsOpen((s) => !s)}
                aria-expanded={settingsOpen}
              >
                <SettingsIcon />
                <span>{item.label}</span>
              </button>
            ))}
          </nav>

          <div
            className={`settings-panel ${settingsOpen ? "settings-panel-open" : ""}`}
          >
            <div className="settings-row">
              <span>Appearance</span>
              <div className="theme-toggle" role="group" aria-label="Theme">
                <button
                  type="button"
                  className={theme === "light" ? "theme-btn-active" : ""}
                  onClick={() => setTheme("light")}
                >
                  Light
                </button>
                <button
                  type="button"
                  className={theme === "dark" ? "theme-btn-active" : ""}
                  onClick={() => setTheme("dark")}
                >
                  Dark
                </button>
              </div>
            </div>
          </div>

          <div className="sidebar-footer">
            <span className={`sidebar-dot status-${connectionState}`} />
            <div>
              <div className="sidebar-footer-title">Workspace</div>
              <div className="sidebar-footer-sub">
                {statusWord(connectionState, reconnectAttempt)}
              </div>
            </div>
          </div>
        </aside>

        <div className="product-main">
          <header className="topbar">
            <div className="topbar-title">
              <h1>Live console</h1>
              <p>A single persistent, always-reconnecting channel.</p>
            </div>
            <div className="topbar-status">
              <span className={`status-pill status-${connectionState}`}>
                <span className="status-pill-dot" />
                {statusWord(connectionState, reconnectAttempt)}
              </span>
            </div>
          </header>

          <div className="workspace">
            <section className="relay">
              <header className="relay-header">
                <div className="relay-identity">
                  <Orb state={connectionState} size={22} />
                  <div className="relay-identity-text">
                    <h2 className="relay-title">Message channel</h2>
                    <p className="relay-freq">
                      <ShieldIcon />
                      Encrypted in transit
                    </p>
                  </div>
                </div>
                <div className="relay-metrics">
                  {isLive && (
                    <span className="metric-chip">
                      {formatDuration(uptimeSeconds)}
                    </span>
                  )}
                </div>
              </header>

              <div ref={chatWindowRef} className="relay-log">
                {messages.length === 0 && connectionState === "open" && (
                  <div className="log-empty">
                    <p>Say something — it&rsquo;ll show up here instantly.</p>
                  </div>
                )}

                {messages.map((msg) => (
                  <div
                    key={msg.id}
                    className={`message-row ${msg.self ? "self" : "other"}`}
                  >
                    <div
                      className={`bubble ${msg.self ? "bubble-self" : "bubble-other"}`}
                    >
                      <span className="bubble-text">{msg.text}</span>
                      <span className="bubble-time">
                        {formatClock(msg.timestamp)}
                      </span>
                    </div>
                  </div>
                ))}

                {connectionState === "offline" && (
                  <div className="log-offline">
                    <p>Connection couldn&rsquo;t be re-established.</p>
                    <button
                      className="retry-button"
                      onClick={handleRetryNow}
                      type="button"
                    >
                      Reconnect
                    </button>
                  </div>
                )}
              </div>

              <div className="relay-controls">
                <input
                  ref={inputRef}
                  type="text"
                  value={currentMessage}
                  onChange={(e) => setCurrentMessage(e.target.value)}
                  onKeyDown={handleKeyDown}
                  placeholder={isLive ? "Message" : "Waiting for connection…"}
                  disabled={!isLive}
                  aria-label="Message to send"
                />
                <button
                  className={`transmit-button ${justSent ? "transmit-pulse" : ""}`}
                  onClick={handleSendMessage}
                  disabled={!isLive || currentMessage.trim() === ""}
                  type="button"
                  aria-label="Send message"
                >
                  <SendIcon />
                </button>
              </div>
            </section>

            <aside className="rail">
              <div className="card">
                <div className="card-title">Connection</div>
                <div className="card-connection">
                  <Orb state={connectionState} size={40} />
                  <div>
                    <div
                      className={`card-connection-state status-${connectionState}`}
                    >
                      {statusWord(connectionState, reconnectAttempt)}
                    </div>
                  </div>
                </div>
                <div className="card-row">
                  <span>Uptime</span>
                  <span className="card-mono">
                    {isLive ? formatDuration(uptimeSeconds) : "—"}
                  </span>
                </div>
              </div>

              <div className="card">
                <div className="card-title">Under the hood</div>
                <ul className="highlight-list">
                  <li>Auto-reconnect with exponential backoff</li>
                  <li>Echo-based duplicate message prevention</li>
                  <li>Generation-guarded sockets (StrictMode-safe)</li>
                  <li>Theme preference persisted across sessions</li>
                </ul>
              </div>

              <div className="card card-grow">
                <div className="card-title">Connection log</div>
                <div className="activity-feed">
                  {events.length === 0 && (
                    <div className="activity-empty">No events yet</div>
                  )}
                  {events.map((e) => (
                    <div key={e.id} className="activity-item">
                      <span className={`activity-dot activity-${e.type}`} />
                      <div className="activity-text">
                        <div className="activity-label">{eventLabel(e)}</div>
                        <div className="activity-time">
                          {formatRelative(e.timestamp)}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </aside>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;

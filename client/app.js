/*
 * Working but deliberately naive LiveRecall browser starter.
 *
 * Provided mechanical behavior:
 *   - opens a WebSocket
 *   - sends CLIENT_HELLO
 *   - visibly appends every received CAPTION_UPDATE
 *   - displays whether an update is provisional/final
 *
 * Graded TODOs:
 *   - replace revisions rather than appending duplicates
 *   - maintain >=60 s of finalized local state
 *   - implement 30 s default recall + 15/30/60 s views
 *   - automatic reconnect + correct resume state
 *   - HISTORY merge/deduplication
 *   - usable final UI
 */

const WS_URL = "ws://127.0.0.1:8765";
const statusEl = document.getElementById("status");
const liveEl = document.getElementById("live");
const recallEl = document.getElementById("recall");

let ws;
let lastFinal = { segment: 0, revision: 0 };
let retryAttempt = 0;
let retryTimer = null;
let recallSeconds = 30;
const captions = new Map(); // segment -> latest caption
const elements = new Map(); // segment -> DOM element

function numericClientId() {
  const key = "liverecall-client-id";
  let value = sessionStorage.getItem(key);
  if (!value) {
    value = String(Math.floor(100000 + Math.random() * 900000));
    sessionStorage.setItem(key, value);
  }
  return Number(value);
}

function renderRecall() {
  const cutoff = Date.now() / 1000 - recallSeconds;

  const items = [...captions.values()]
    .filter(c => c.final && c.source_ts >= cutoff)
    .sort((a, b) => a.segment - b.segment);

  recallEl.replaceChildren();

  if (items.length === 0) {
    recallEl.textContent =
      `No captions in the last ${recallSeconds} seconds.`;
    return;
  }

  for (const caption of items) {
    const row = document.createElement("div");
    row.textContent = caption.text;
    recallEl.appendChild(row);
  }
}

function applyCaption(msg) {
  const existingCaption = captions.get(msg.segment)
  
  // updating the caption
  if (existingCaption) {
    if (existingCaption.final) {
      return
    }
    if (msg.revision <= existingCaption.revision) {
      return
    }

    if (msg.final) {
      renderRecall()
    }
  }

  captions.set(msg.segment, msg)
  let line = elements.get(msg.segment)

  if (!line) {
    line = document.createElement("div");
    elements.set(msg.segment, line);
    liveEl.appendChild(line);
  }

  line.className = "caption-row";
  line.classList.add(msg.final ? "final-line" : "provisional-line");
  line.textContent = 
    `[${msg.final ? "FINAL" : "PROVISIONAL"}]` + 
    `segment ${msg.segment} rev ${msg.revision}: ${msg.text}`;
 
  if (msg.final && msg.segment > lastFinal.segment) {
  lastFinal = { segment: msg.segment, revision: msg.revision };
  }
}

function scheduleReconnect() {
  if (retryTimer !== null) return;

  // make each failed attempt wait longer: 0.5s, 1s, 2s up to 10s max,
  // randomised between half and the full step so clients that dropped
  // together don't all reconnect at the same moment
  const cap = Math.min(10000, 500 * 2 ** retryAttempt++);
  const delay = cap / 2 + Math.random() * (cap / 2);
  statusEl.textContent = `reconnecting in ${(delay / 1000).toFixed(1)} s`;

  retryTimer = setTimeout(() => {
    retryTimer = null;
    connect();
  }, delay);
}

function connect() {
  statusEl.textContent = 
  lastFinal.segment === 0 ? "connecting..." : "reconnecting...";

  ws = new WebSocket(WS_URL);

  ws.addEventListener("open", () => {
    statusEl.textContent = "connected";
    ws.send(JSON.stringify({
      type: "CLIENT_HELLO",
      client_id: numericClientId(),
      session: "L1",
      last_segment: lastFinal.segment,       // TODO: resume latest finalized state
      last_revision: lastFinal.revision,
      history_seconds: 60
    }));
  });

  ws.addEventListener("message", (event) => {
    const msg = JSON.parse(event.data);
    if (msg.type === "CAPTION_UPDATE") {
      applyCaption(msg);
    } else if (msg.type === "HISTORY") {
      console.log(`HISTORY with ${msg.items?.length ?? 0} item(s)`, msg);
      // merge history without stale/duplicate final captions.
      for (const item of msg.items ?? []) {
        applyCaption(item)
      }

      if (msg.active) {
        applyCaption(msg.active)
      }

      retryAttempt = 0; // reset retry count on successful message
    }
  });

  ws.addEventListener("close", () => {
    statusEl.textContent = "disconnected";
    // automatic reconnect after a short delay, using finalized resume state.
    scheduleReconnect();
  });

  ws.addEventListener("error", () => {
    statusEl.textContent = "connection error";
  });
}

for (const button of document.querySelectorAll("button[data-seconds]")) {
  button.addEventListener("click", () => {
    recallSeconds = Number(button.dataset.seconds);
    renderRecall();
  });
}

// show the default 30-second view
renderRecall();
// update the recall view every second
setInterval(renderRecall, 1000);

connect();

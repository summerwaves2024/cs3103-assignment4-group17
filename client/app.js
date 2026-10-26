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
const recallEmptyEl = document.getElementById("recall-empty");
const recallPanel = document.getElementById("recall-panel");
const recallOpenButton = document.getElementById("recall-open");
const recallWindowButtons = document.querySelectorAll("button[data-seconds]");

let ws;
let lastFinal = { segment: 0, revision: 0 };
let retryAttempt = 0;
let retryTimer = null;
let recallSeconds = 30;
let recallFollow = true; // keep the recall list at the newest caption
const recallRows = new Map(); // segment -> row in the recall list
const captions = new Map(); // segment -> latest caption
const elements = new Map(); // segment -> DOM element
const KEEP_SECONDS = 90; // messages beyond KEEP_SECONDS ago are no longer stored in device

function numericClientId() {
  const key = "liverecall-client-id";
  let value = sessionStorage.getItem(key);
  if (!value) {
    value = String(Math.floor(100000 + Math.random() * 900000));
    sessionStorage.setItem(key, value);
  }
  return Number(value);
}

function captionTime(caption) {
  return new Date(caption.source_ts * 1000).toLocaleTimeString([], {
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
  });
}

function fillCaptionRow(row, caption) {
  row.className = "caption-row " + (caption.final ? "final-line" : "provisional-line");
  const time = document.createElement("span");
  time.className = "caption-time";
  time.textContent = caption.final ? captionTime(caption) : "now";
  const text = document.createElement("span");
  text.className = "caption-text";
  text.textContent = caption.text;
  row.replaceChildren(time, text);
}

function renderRecall() {
  if (recallPanel.hidden) return;
  const cutoff = Date.now() / 1000 - recallSeconds;

  const items = [...captions.values()]
    .filter(c => c.final && c.source_ts >= cutoff)
    .sort((a, b) => a.segment - b.segment);
  const inWindow = new Set(items.map(c => c.segment));

  // Update the list in place instead of rebuilding
  // so a user who scrolled up keeps their place. 
  //
  // For rows that left the window, remove them and
  // scroll up by their height so the text being read
  // does not move
  let removedHeight = 0;
  for (const [segment, row] of recallRows) {
    if (!inWindow.has(segment)) {
      removedHeight += row.offsetHeight;
      row.remove();
      recallRows.delete(segment);
    }
  }
  if (!recallFollow) {
    recallEl.scrollTop -= removedHeight;
  }

  // Add new captions, in segment order.
  for (const caption of items) {
    if (recallRows.has(caption.segment)) continue;
    const row = document.createElement("div");
    fillCaptionRow(row, caption);
    const next = [...recallRows].find(([segment]) => segment > caption.segment);
    recallEl.insertBefore(row, next ? next[1] : null);
    recallRows.set(caption.segment, row);
  }

  recallEmptyEl.hidden = items.length > 0;
  recallEmptyEl.textContent = `No captions in the last ${recallSeconds} seconds.`;

  if (recallFollow) {
    recallEl.scrollTop = recallEl.scrollHeight;
  }
}

// Follow the newest caption while the user is at the bottom. Then stop as soon as
// they scroll up to read, and resume when they scroll back down.
recallEl.addEventListener("scroll", () => {
  recallFollow = recallEl.scrollHeight - recallEl.scrollTop - recallEl.clientHeight < 8;
});

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
  }

  captions.set(msg.segment, msg)
  if (msg.final) {
      renderRecall()
    }
  let line = elements.get(msg.segment)

  if (!line) {
    line = document.createElement("div");
    elements.set(msg.segment, line);
    liveEl.appendChild(line);
  }

  fillCaptionRow(line, msg);
 
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
      last_segment: lastFinal.segment,
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

function setRecallWindow(seconds, followLatest = true) {
  recallSeconds = seconds;
  recallFollow = followLatest;

  for (const button of recallWindowButtons) {
    button.setAttribute(
      "aria-pressed",
      String(Number(button.dataset.seconds) === seconds)
    );
  }

  renderRecall();
}

// "What did I miss?" opens on demand and always starts at the 30-second view
function openRecall() {
  recallPanel.hidden = false;
  recallOpenButton.setAttribute("aria-expanded", "true");
  setRecallWindow(30, false);

  requestAnimationFrame(() => {
    recallEl.scrollTop = 0;
  });
}

function closeRecall() {
  recallPanel.hidden = true;
  recallOpenButton.setAttribute("aria-expanded", "false");
}

recallOpenButton.addEventListener("click", () => {
  if (recallPanel.hidden) {
    openRecall();
  } else {
    closeRecall();
  }
});
document.getElementById("recall-close").addEventListener("click", closeRecall);

for (const button of recallWindowButtons) {
  button.addEventListener("click", () => setRecallWindow(Number(button.dataset.seconds)));
}

function pruneOldCaptions() {
  const cutoff = Date.now() / 1000 - KEEP_SECONDS;
  for (const [segment, caption] of captions) {
    if (caption.source_ts < cutoff) {
      captions.delete(segment);
      elements.get(segment)?.remove();
      elements.delete(segment);
    }
  }
}

// every second: drop old captions, and keep the recall view current while it is open
setInterval(() => {
  pruneOldCaptions();
  renderRecall();
}, 1000);

connect();

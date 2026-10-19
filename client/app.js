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

function applyCaption(msg) {
  const existingCaption = captions.get(msg.segment)

  // updating the caption
  if (existingCaption) {
    if (existingCaption.final) {
      return
    }
    if (msg.revision <= existing.revision) {
      return
    }
  }

  captions.set(msg.segment, msg)

  // the general idea is 
  // new segment -> create a new line
  // new revision -> update the existing line
  let line = elements.get(msg.segment)

  if (!line) {
    row = document.createElement('div')
    elements.set(msg.segment, line)
    liveEl.appendChild(line)
  }
  line.className = msg.final ? "final-line" : "provisional-line";
  line.textContent = `[${msg.final ? "FINAL" : "PROVISIONAL"}] ` +
    `segment ${msg.segment} rev ${msg.revision}: ${msg.text}`;
  

}

function connect() {
  statusEl.textContent = "connecting";
  ws = new WebSocket(WS_URL);

  ws.addEventListener("open", () => {
    statusEl.textContent = "connected";
    ws.send(JSON.stringify({
      type: "CLIENT_HELLO",
      client_id: numericClientId(),
      session: "L1",
      last_segment: 0,       // TODO: resume latest finalized state
      last_revision: 0,
      history_seconds: 30
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

      if (msg.caption) {
        applyCaption(msg.item)
      }
    }
  });

  ws.addEventListener("close", () => {
    statusEl.textContent = "disconnected";
    // TODO: automatic reconnect after a short delay, using finalized resume state.
  });

  ws.addEventListener("error", () => {
    statusEl.textContent = "connection error";
  });
}

for (const button of document.querySelectorAll("button[data-seconds]")) {
  button.addEventListener("click", () => {
    const seconds = Number(button.dataset.seconds);
    recallEl.textContent = `TODO: show finalized captions from the previous ${seconds} seconds using local state.`;
  });
}

connect();

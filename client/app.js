// Browser-side logic for LiveRecall.
// This covers connection management and caption rendering.
// It also covers recovery and panel controls.

// DOM references and client-side configuration.
const WS_URL = "ws://127.0.0.1:8765";
const statusEl = document.getElementById("status");
const liveEl = document.getElementById("live");
const recallEl = document.getElementById("recall");
const recallEmptyEl = document.getElementById("recall-empty");
const noticeEl = document.getElementById("notice");
const recallPanel = document.getElementById("recall-panel");
const appMainEl = document.querySelector(".app-main");
const dividerEl = document.getElementById("divider");
const recallOpenButton = document.getElementById("recall-open");
const recallWindowButtons = document.querySelectorAll("button[data-seconds]");
const recallLatestButton = document.getElementById("recall-latest");

// Connection state and caption state for this browser tab.
let ws; // Stores the current WebSocket connection to the server. 
let lastFinal = { segment: 0, revision: 0 }; // Tracks the newest finalised caption that is received.
let retryAttempt = 0; // Tracks reconnect attempts and the timer used before trying again.
let retryTimer = null;
let resumeFrom = 0; // last final segment this connection resumed from (0 = fresh join)
let noticeTimer = null; // Stores the timer for temporary notices
let recallSeconds = 30; // Controls the selected recall window.
let recallFollow = false; // whether recall follows newly arriving captions
let recallJumping = false; // smooth scroll to the newest caption in progress
const recallRows = new Map(); // segment -> row in the recall list
const captions = new Map(); // segment -> latest caption
const elements = new Map(); // segment -> DOM element
let latestLiveElement = null;
const KEEP_SECONDS = 90; // messages beyond KEEP_SECONDS ago are no longer stored in device

// Give each browser tab a temporary numeric client ID.
function numericClientId() {
  const key = "liverecall-client-id";
  // Defines the sessionStorage key used to save the ID.
  let value = sessionStorage.getItem(key);
  if (!value) {
    // Reuse the tab's ID when possible; otherwise create and store one.
    value = String(Math.floor(100000 + Math.random() * 900000));
    sessionStorage.setItem(key, value);
  }
  return Number(value);
}

// Convert a caption's Unix timestamp into a readable clock time.
function captionTime(caption) {
  return new Date(caption.source_ts * 1000).toLocaleTimeString([], {
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
  });
}

// Update the visible live-caption row.
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

// Refresh the recall panel while preserving the user's reading position.
function renderRecall() {
  if (recallPanel.hidden) return;
  const cutoff = Date.now() / 1000 - recallSeconds;

  // Keep the newest finalised caption per segment.
  // Apply the selected time window
  // and render the results in chronological order.
  const items = [...captions.values()]
    .filter(c => c.final && c.source_ts >= cutoff)
    .sort((a, b) => a.segment - b.segment);
  // Identify captions still inside of the window.
  const inWindow = new Set(items.map(c => c.segment));

  // Update rows in place so a user who is reading older captions keeps their place.
  //
  // For rows that left the window remove them and
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
  // Hide the button if the user is no longer following the newest caption.
  recallLatestButton.hidden = recallFollow;

  // Add new captions in segment order.
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

  // Follow the newest caption
  if (recallFollow) {
    recallEl.scrollTop = recallEl.scrollHeight;
  }
  // Updates whether the "newest caption" button should be visible.
  updateLatestButton();
}

// Show "Newest caption" only when there is something below the visible part
// of the list
function updateLatestButton() {
  recallLatestButton.hidden =
    recallEl.scrollHeight - recallEl.scrollTop - recallEl.clientHeight < 8;
}

// Follow the newest caption while the user is at the bottom. Then stop as soon as
// they scroll up to read and resume when they scroll back down.
recallEl.addEventListener("scroll", () => {
  if (recallJumping) return;
  recallFollow =
    recallEl.scrollHeight - recallEl.scrollTop - recallEl.clientHeight < 8;

  updateLatestButton();
});

// snap to the real bottom after the scroll animation
// This handles cases where new captions arrive during scrolling.
function finishRecallJump() {
  if (!recallJumping) return;
  recallJumping = false;
  recallEl.scrollTop = recallEl.scrollHeight;
}
recallEl.addEventListener("scrollend", finishRecallJump);

recallLatestButton.addEventListener("click", () => {
  // only when the user clicks the button
  recallFollow = true;
  recallJumping = true;
  recallEl.scrollTo({
    top: recallEl.scrollHeight,
    behavior: "smooth",
  });
  // smoothly scrolls to the bottom of the recall panel
  recallLatestButton.hidden = true;
  setTimeout(finishRecallJump, 1000); // fallback where "scrollend" is not supported
});

// Apply a caption if it is newer than the stored revision for that segment.
// Return false for stale or duplicate updates.
function applyCaption(msg) {
  const existingCaption = captions.get(msg.segment)
  
  // updating the caption
  if (existingCaption) {
    if (existingCaption.final) {
      return false
    }
    if (msg.revision <= existingCaption.revision) {
      return false
    }
  }

  // Stores the accepted caption as the latest version of the segment
  captions.set(msg.segment, msg)
  // Recall contains finalised captions only.
  if (msg.final) {
      renderRecall()
    }
  // Reuse an existing row when this segment already has a rendered entry.
  let line = elements.get(msg.segment)

  // Create a row for a new segment; revisions update the existing row.
  if (!line) {
    line = document.createElement("div");
    elements.set(msg.segment, line);
    liveEl.appendChild(line);
  }

  // Highlights the newest caption
  latestLiveElement?.classList.remove("latest-line");
  line.classList.add("latest-line");
  latestLiveElement = line;
  // Removes the highlight from the previous row and applies it to the current update.
  line.dataset.segment = String(msg.segment);

  // Update the row's text
  fillCaptionRow(line, msg);
 
  // Record the latest final segment and revision for reconnect recovery.
  if (msg.final && msg.segment > lastFinal.segment) {
    lastFinal = { segment: msg.segment, revision: msg.revision };
  }
  return true
}

// Display a temporary status message in the interface.
function showNotice(text) {
  noticeEl.textContent = text;
  noticeEl.hidden = false;
  clearTimeout(noticeTimer);
  // Start a six-second timer.
  // The notice is then hidden automatically.
  noticeTimer = setTimeout(() => { noticeEl.hidden = true; }, 6000);
}

// Schedule a reconnect after the current WebSocket connection fails.
function scheduleReconnect() {
  if (retryTimer !== null) return;

  // Increase the delay after repeated failures and add jitter so clients that
  // disconnect together do not all reconnect at exactly the same time.
  const cap = Math.min(10000, 500 * 2 ** retryAttempt++);
  const delay = cap / 2 + Math.random() * (cap / 2);
  statusEl.textContent = `Reconnecting in ${(delay / 1000).toFixed(1)} s (attempt ${retryAttempt})`;

  retryTimer = setTimeout(() => {
    retryTimer = null;
    connect();
  }, delay);
}

// Establish the browser's WebSocket connection to the LiveRecall server.
function connect() {
  statusEl.textContent = 
  lastFinal.segment === 0 ? "Connecting…" : "Reconnecting…";

  // Open a connection to the configured server URL.
  ws = new WebSocket(WS_URL);

  // Mark the client as live and send its recovery checkpoint.
  ws.addEventListener("open", () => {
    statusEl.textContent = "● Live";
    resumeFrom = lastFinal.segment;
    ws.send(JSON.stringify({
      type: "CLIENT_HELLO",
      client_id: numericClientId(),
      session: "L1",
      last_segment: lastFinal.segment,
      last_revision: lastFinal.revision,
      history_seconds: 60
    }));
  });

  // Handle each server message after decoding its JSON payload.
  ws.addEventListener("message", (event) => {
    const msg = JSON.parse(event.data);
      // Apply both provisional and finalised caption updates through the same
      // revision-aware rendering path.
    if (msg.type === "CAPTION_UPDATE") {
      applyCaption(msg);
    } else if (msg.type === "HISTORY") {
      // Logs the number of recovered items for debugging.
      console.log(`HISTORY with ${msg.items?.length ?? 0} item(s)`, msg);
      // Merge recovered history through the same deduplication logic.
      let recovered = 0;
      for (const item of msg.items ?? []) {
        if (applyCaption(item) && item.final) recovered++;
      }

      // Tell the user whether reconnect recovery found missed captions.
      // If the client was already connected show a reconnect message.
      const s = recovered === 1 ? "" : "s";
      if (resumeFrom > 0) {
        showNotice(recovered > 0
          ? `Reconnected: recovered ${recovered} missed caption${s}, no duplicates.`
          : "Reconnected. Nothing was missed.");
      } else if (recovered > 0) {
        // Otherwise the client has joined during the lecture.
        showNotice(`Joined mid-lecture: loaded ${recovered} earlier caption${s}.`);
      }

      // Render the source's current provisional caption when one exists.
      if (msg.active) {
        applyCaption(msg.active)
      }

      retryAttempt = 0; // reset retry count on successful message
    }
  });

  // Reconnect after the WebSocket closes.
  ws.addEventListener("close", () => {
    statusEl.textContent = "Connection lost";
  // Resume from the latest finalised checkpoint after a backoff delay.
    scheduleReconnect();
  });

  // Updates the status if the WebSocket gets an error.
  ws.addEventListener("error", () => {
    statusEl.textContent = "Connection lost";
  });
}

// Change the amount of history shown in the recall panel.
function setRecallWindow(seconds, followLatest = true) {
  recallSeconds = seconds;
  // Store the selected duration.
  recallFollow = followLatest;

  // Marks the duration as active for styling and accessibility
  for (const button of recallWindowButtons) {
    button.setAttribute(
      "aria-pressed",
      String(Number(button.dataset.seconds) === seconds)
    );
  }

  // Recalculates the time cutoff and updates the displayed finalised captions.
  renderRecall();
}

// Open the recall panel at its default 30-second view.
function openRecall() {
  // Opens the recall panel and sets the initial view
  recallPanel.hidden = false;
  dividerEl.hidden = false;
  recallOpenButton.setAttribute("aria-expanded", "true");
  // Restores the panel to the default 30-second history window
  setRecallWindow(30, false);

  requestAnimationFrame(() => {
    recallEl.scrollTop = 0;
  });
}

// Close the recall panel without clearing the locally retained history.
function closeRecall() {
  // Hides the recall panel and its caption list.  
  recallPanel.hidden = true;
  dividerEl.hidden = true;
  recallOpenButton.setAttribute("aria-expanded", "false");
}

// Toggle the recall panel when its open button is clicked.
recallOpenButton.addEventListener("click", () => {
  if (recallPanel.hidden) {
    openRecall();
  } else {
    closeRecall();
  }
});
// Closing with the panel's close button.
document.getElementById("recall-close").addEventListener("click", closeRecall);

// Connect the recall buttons to setRecallWindow().
for (const button of recallWindowButtons) {
  button.addEventListener("click", () => setRecallWindow(Number(button.dataset.seconds)));
}

// Resize the recall panel by dragging its divider. The selected width is
// The width is persisted locally.
// The remaining space belongs to the live-caption view.
const RECALL_MIN_WIDTH = 260;
const RECALL_WIDTH_KEY = "liverecall-recall-width";

// Clamp the recall panel width.
// Apply the width and persist it locally.
function setRecallWidth(px) {
  // Defines a function that applies a new width to the recall panel and save it for future visits.
  const width = Math.round(Math.min(Math.max(px, RECALL_MIN_WIDTH), appMainEl.clientWidth * 0.6));
  appMainEl.style.setProperty("--recall-width", `${width}px`);
  try { localStorage.setItem(RECALL_WIDTH_KEY, String(width)); } catch {}
}

// In charge of handling the panel resizing and restoring the saved width
dividerEl.addEventListener("pointerdown", (event) => {
  const startX = event.clientX;
  const startWidth = recallPanel.getBoundingClientRect().width;
  dividerEl.setPointerCapture(event.pointerId);
  document.body.classList.add("resizing");

  // The recall panel is on the right.
  // Dragging left makes it wider.
  const onMove = (e) => setRecallWidth(startWidth + startX - e.clientX);
  // Stops tracking pointer movement and restores normal cursor/text-selection behaviour.
  const onEnd = () => {
    dividerEl.removeEventListener("pointermove", onMove);
    dividerEl.removeEventListener("lostpointercapture", onEnd);
    document.body.classList.remove("resizing");
  };
  dividerEl.addEventListener("pointermove", onMove);
  dividerEl.addEventListener("lostpointercapture", onEnd);
  // Prevent the browser's default drag or text-selection behaviour.
  event.preventDefault();
});

// Restore the previously saved panel width when the page loads.
try {
  const saved = Number(localStorage.getItem(RECALL_WIDTH_KEY));
  if (saved > 0) appMainEl.style.setProperty("--recall-width", `${saved}px`);
} catch {}

// Remove locally retained captions older than the browser's recall window.
function pruneOldCaptions() {
  const cutoff = Date.now() / 1000 - KEEP_SECONDS;
  // Remove old captions from the caption map.
  // Remove the matching live row and element reference.
  for (const [segment, caption] of captions) {
    if (caption.source_ts < cutoff) {
      captions.delete(segment);
      elements.get(segment)?.remove();
      elements.delete(segment);
    }
  }
}

// Periodically prune old captions and refresh the open recall view.
setInterval(() => {
  pruneOldCaptions();
  renderRecall();
}, 1000);

// Start the initial WebSocket connection.
connect();

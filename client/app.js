// As a whole, this file contians the browser-side logic for LiveRecall. 
// It is responsible for:
// 1. Connecting the browser to the WebSocket server. 
// 2. Receiving and display live caption updates. 
// 3. Handling caption revisions, alongside finalised captions. 
// 4. Managing reconnect recovery and missed-caption history. 
// 5. Providing the recall panel, scrolling and resize controls.

// declare constants
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

// As a whole, all of these variables store the browser client's connection, caption and UI state.
let ws; // Stores the current WebSocket connection to the server. 
let lastFinal = { segment: 0, revision: 0 }; // Tracks the newest finalised caption that is received.
let retryAttempt = 0; // Tracks reconnect attempts and the timer used before trying again.
let retryTimer = null;
let resumeFrom = 0; // last final segment this connection resumed from (0 = fresh join)
let noticeTimer = null; // Stores the timer for temporary notices
let recallSeconds = 30; // Controls the selected recall window: 15, 30, or 60 seconds
let recallFollow = false; // whether recall follows newly arriving captions
let recallJumping = false; // smooth scroll to the newest caption in progress
const recallRows = new Map(); // segment -> row in the recall list
const captions = new Map(); // segment -> latest caption
const elements = new Map(); // segment -> DOM element
let latestLiveElement = null;
const KEEP_SECONDS = 90; // messages beyond KEEP_SECONDS ago are no longer stored in device

// It gives each browser tab a temporary numeric client ID. 
function numericClientId() {
  const key = "liverecall-client-id";
  // Defines the sessionStorage key used to save the ID.
  let value = sessionStorage.getItem(key);
  if (!value) {
    // Checks whether the browser already has an ID. If no ID exists, a random six-digit number is stored.
    value = String(Math.floor(100000 + Math.random() * 900000));
    sessionStorage.setItem(key, value);
  }
  return Number(value);
}

// Converts a caption's Unix timestamp into a readable clock time. 
// Is mainly for finalised captions, as provisional captions will only display "now".
function captionTime(caption) {
  return new Date(caption.source_ts * 1000).toLocaleTimeString([], {
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
  });
}

// As a whole, it is meant to update a caption row in the browser interface.
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

// This is in charge of refreshing the "What did I miss?" panel
// while perserving the user's scrolling position. 
function renderRecall() {
  if (recallPanel.hidden) return;
  const cutoff = Date.now() / 1000 - recallSeconds;

  // It gets the latest caption for every segment, keeps finalised captions only, 
  // removes captions older than the selected window and sorts them chronologically.
  const items = [...captions.values()]
    .filter(c => c.final && c.source_ts >= cutoff)
    .sort((a, b) => a.segment - b.segment);
  // Identify captions still inside of the window.
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
  // Hide the button if the user is no longer following the newest caption.
  recallLatestButton.hidden = recallFollow;

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
// they scroll up to read, and resume when they scroll back down.
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

// Returns true if the caption was applied, false if it was stale or a duplicate.
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
  // Finals captions are added to the recall panel since recall only displays finalised captions
  if (msg.final) {
      renderRecall()
    }
  // Sees whether a DOM row already exists
  let line = elements.get(msg.segment)

  // Creates a new row for a previously unseen segment. 
  // If the row already exists, it is reused for the newer revision.
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

// Displays a temporary status message in the interface.
function showNotice(text) {
  noticeEl.textContent = text;
  noticeEl.hidden = false;
  clearTimeout(noticeTimer);
  // Start a new six-second timer; after six seconds, the notice is hidden automatically.
  noticeTimer = setTimeout(() => { noticeEl.hidden = true; }, 6000);
}

// Schedules a new WebSocket connection after the current connection fails.
function scheduleReconnect() {
  if (retryTimer !== null) return;

  // make each failed attempt wait longer (exponential backoff): 0.5s, 1s, 2s up to 10s max,
  // randomised between half and the full step so clients that dropped
  // together don't all reconnect at the same moment
  const cap = Math.min(10000, 500 * 2 ** retryAttempt++);
  const delay = cap / 2 + Math.random() * (cap / 2);
  statusEl.textContent = `Reconnecting in ${(delay / 1000).toFixed(1)} s (attempt ${retryAttempt})`;

  retryTimer = setTimeout(() => {
    retryTimer = null;
    connect();
  }, delay);
}

// As a whole, it establishes the browser's WebSocket connection to the LiveRecall server.
function connect() {
  statusEl.textContent = 
  lastFinal.segment === 0 ? "Connecting…" : "Reconnecting…";

  // Creates the WebSocket.
  // Opens a connection to the server URL, like ws://127.0..0.1:8765.
  ws = new WebSocket(WS_URL);

  // Once connected, updates the UI to show that the client is live.
  ws.addEventListener("open", () => {
    statusEl.textContent = "● Live";
    // Stores the segment from which this connection is resuming.
    resumeFrom = lastFinal.segment;
    // Sends the client a hello.
    ws.send(JSON.stringify({
      type: "CLIENT_HELLO",
      client_id: numericClientId(),
      session: "L1",
      last_segment: lastFinal.segment,
      last_revision: lastFinal.revision,
      history_seconds: 60
    }));
  });

  // Runs whenever the server sends a message and converts the JSON text into a JavaScript object.
  ws.addEventListener("message", (event) => {
    const msg = JSON.parse(event.data);
    // Passes live drafts and final captions, in which validates the revision and updates the live
    // caption display.
    if (msg.type === "CAPTION_UPDATE") {
      applyCaption(msg);
    } else if (msg.type === "HISTORY") {
      // Logs the number of recovered items for debugging.
      console.log(`HISTORY with ${msg.items?.length ?? 0} item(s)`, msg);
      // merge history without stale/duplicate final captions.
      let recovered = 0;
      for (const item of msg.items ?? []) {
        if (applyCaption(item) && item.final) recovered++;
      }

      // Tell the user what recovery did.
      // If the client had already been connected, it shows a reconnect message. 
      const s = recovered === 1 ? "" : "s";
      if (resumeFrom > 0) {
        showNotice(recovered > 0
          ? `Reconnected: recovered ${recovered} missed caption${s}, no duplicates.`
          : "Reconnected. Nothing was missed.");
      } else if (recovered > 0) {
        // Else, if it is a new client.
        showNotice(`Joined mid-lecture: loaded ${recovered} earlier caption${s}.`);
      }

      // Displays the source's currently active provision caption, if one exists.
      if (msg.active) {
        applyCaption(msg.active)
      }

      retryAttempt = 0; // reset retry count on successful message
    }
  });

  // Runs when the WebSocket closes
  ws.addEventListener("close", () => {
    statusEl.textContent = "Connection lost";
    // automatic reconnect after a short delay, using finalized resume state.
    scheduleReconnect();
  });

  // Updates the status if the WebSocket gets an error.
  ws.addEventListener("error", () => {
    statusEl.textContent = "Connection lost";
  });
}

// As a whole, this changes the amount of history in which is shown in the recall panel.
function setRecallWindow(seconds, followLatest = true) {
  recallSeconds = seconds;
  // Stores the selected duration, such as 15, 30, or 60.
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

// "What did I miss?" opens on demand and always starts at the 30-second view
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

// Closes the "What did I miss?" recall panel; upon reopening the panel, you can see the existing 
// recent history again.
function closeRecall() {
  // Hides the recall panel and its caption list.  
  recallPanel.hidden = true;
  dividerEl.hidden = true;
  recallOpenButton.setAttribute("aria-expanded", "false");
}

// As a whole, makes the "What did I miss?" button act as a toggle:
// If the panel is closed, open it; 
// If the panel is open, close it. 
recallOpenButton.addEventListener("click", () => {
  if (recallPanel.hidden) {
    openRecall();
  } else {
    closeRecall();
  }
});
// Closing with the panel's close button.
document.getElementById("recall-close").addEventListener("click", closeRecall);

// Adds behaviour to the 15s, 30s and 60s buttons; when clicked, the selected duration is passed
// to the setRecallWindow();
for (const button of recallWindowButtons) {
  button.addEventListener("click", () => setRecallWindow(Number(button.dataset.seconds)));
}

// Resizing the recall panel by dragging the divider.
// The width is kept between visits (width stored in local storage) and the live captions take the rest.
const RECALL_MIN_WIDTH = 260;
const RECALL_WIDTH_KEY = "liverecall-recall-width";

// Sets and saves the recall panel's width
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

  // The recall panel is on the right, so dragging left makes it wider.
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

// Reads the previously saved panel width from localStorage and applies it when the page loads.
try {
  const saved = Number(localStorage.getItem(RECALL_WIDTH_KEY));
  if (saved > 0) appMainEl.style.setProperty("--recall-width", `${saved}px`);
} catch {}

// Calculates the timestamp for the oldest caption the browser should retain.
function pruneOldCaptions() {
  const cutoff = Date.now() / 1000 - KEEP_SECONDS;
  // Removes captions older than 90 seconds from: the browser's caption map, the live-caption DOM, the element lookup map
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

// starts the initial WebSocket connection to the LiveRecall server.
connect();

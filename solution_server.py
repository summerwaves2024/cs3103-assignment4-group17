#!/usr/bin/env python3
"""
This WebSocket server accepts caption updates from the deterministic source,
validates revisions, keeps bounded finalised-caption history, and delivers
updates to browser clients with per-client flow control and reconnect recovery.

As per the assignment specifications, it can be run with this command:
    python solution_server.py --port 8765
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from contextlib import suppress
from typing import Any
from collections import deque
from data_structs import Caption, ClientState, SourceHello, ClientHello
from flow_control import MONITOR_INTERVAL
from revision_state_manager import RevisionStateManager

import websockets
from websockets.asyncio.server import ServerConnection

HISTORY_KEEP_SECONDS = 90  # How long finalised captions remain available for recovery.
MAX_HISTORY_SECONDS = 60  # Maximum history window a client may request.
DEFAULT_HISTORY_SECONDS = 30  # Default recall window when the client omits one.
MAX_PENDING_FINALS = 256  # Maximum per-client finalised-caption backlog.



finals_log: deque[Caption] = deque()  # Final captions in segment order.
active: Caption | None = None  # Newest provisional caption for the current segment.
clients: dict[ServerConnection, ClientState] = {}  # Client state keyed by socket.
overloaded_clients: set[ServerConnection] = set()
rsm: RevisionStateManager | None = None  # Created when the source first connects.

# Parse and validate one caption update received from the source.
def parse_caption(msg: dict[str, Any]) -> Caption | None:
    try:
        # Convert fields to the types used by the server.
        caption = Caption.from_dict(msg)
        caption.segment = int(caption.segment)
        caption.revision = int(caption.revision)
        caption.source_ts = float(caption.source_ts)
    except (TypeError, ValueError):
        # Invalid field types mean this message cannot be processed safely.
        return None
    # Reject invalid revision numbers and non-string caption text.
    if caption.revision < 1 or not isinstance(caption.text, str):
        return None
    # Normalise the final flag before revision processing.
    caption.final = bool(caption.final)
    return caption

# Convert an internal caption into the public WebSocket message format.
def caption_to_dict(caption: Caption) -> dict[str, Any]:
    """only forward the necessary fields"""
    # Keep the wire format explicit so internal server state is not exposed.
    return {
        "type": "CAPTION_UPDATE",
        "session": caption.session,
        "segment": caption.segment,
        "revision": caption.revision,
        "source_ts": caption.source_ts,
        "final": caption.final,
        "text": caption.text,
    }

# Remove finalised captions outside the recovery window and prune their
# corresponding revision state from the manager.
def trim_finals_log(now: float) -> None:
    # Store the oldest segment removed during this pass for RSM pruning.
    pruned_seg = 0
    # Entries are timestamp-ordered.
    # Old captions can therefore be removed from the left.
    while finals_log and finals_log[0].source_ts < now - HISTORY_KEEP_SECONDS:
        pruned_seg = max(pruned_seg, finals_log.popleft().segment)
    if pruned_seg:
        # Discard revision state that can no longer be recovered.
        rsm.prune(pruned_seg)

# Make the newest provisional revision available to every connected client.
def on_draft(caption: Caption) -> None:
    global active
    # The global active value is used when a new client joins mid-segment.
    active = caption

    for state in clients.values():
        # Drafts are replaceable.
        # Retain only the newest unsent revision.
        if state.pending_active is not None:
            state.drafts_dropped += 1

        # Replace the client's pending draft with the newest revision.
        state.pending_active = caption

        # Notify the sender task that work is available.
        state.wake.set()

# Queue a finalised caption for recovery and delivery to each client.
def on_final(caption: Caption) -> None:
    global active
    # Trim history before adding the new finalised caption.
    trim_finals_log(caption.source_ts)
    # Keep the caption for reconnect and late-join recovery.
    finals_log.append(caption)

    # If the finalised caption belongs to the active segment
    # clear the global draft because that segment is complete.
    if active is not None and active.segment == caption.segment:
        active = None
    # Each client has an independent queue and sender task.
    for state in clients.values():
        # A client that is already being closed is no longer a delivery target.
        if state.ws in overloaded_clients:
            continue
        # Final captions must not be silently dropped. If this queue is too
        # large close the client.
        # Reconnect recovery can then fill the gap.
        if len(state.pending_finals) >= MAX_PENDING_FINALS:
            # Do not silently discard finalised captions. Close an excessively
            # delayed client so it can reconnect and recover from finals_log.
            overloaded_clients.add(state.ws)
            state.pending_active = None
            print(f"client too far behind: id={state.client_id}; "
                  f"pending_finals={len(state.pending_finals)}", flush=True)
            asyncio.create_task(
                state.ws.close(code=1013, reason="Client too far behind")
            )
            continue
        # Queue this final independently for this specific client.
        state.pending_finals.append(caption)
        # The final supersedes any unsent draft of the same segment.
        if state.pending_active is not None and state.pending_active.segment == caption.segment:
            state.pending_active = None
        # Wake the sender so it can deliver the final immediately.
        state.wake.set()

# Log a flow-control transition when the state changes.
def log_mode(state: ClientState, now: float) -> None:
    # update_mode() returns the previous mode only when a transition occurred.
    old = state.flow.update_mode(now)
    if old is not None:
        # flush=True makes transitions visible immediately during experiments.
        print(f"client {state.client_id}: {old.name} -> {state.flow.mode.name}", flush=True)

# Process completion of an application-level ping used for RTT monitoring.
def on_pong(state: ClientState, future: asyncio.Future) -> None:
    # Ignore failed or cancelled ping operations.
    if future.cancelled() or future.exception() is not None:
        return
    # Monotonic time is appropriate for elapsed-time measurements.
    now = time.monotonic()
    # Let flow control update RTT estimates and determine the current mode.
    state.flow.on_pong(now)
    log_mode(state, now)
    # A pong may release a held draft.
    # Wake the sender so it can re-check the draft.
    state.wake.set()

# Send updates to one browser client without blocking the source or other clients.
async def client_sender(state: ClientState) -> None:
    # This is the only task that calls ws.send for this client. Finalized
    # captions are sent in order before any provisional draft.
    flow = state.flow
    try:
        while True:
            # Sleep until a caption or flow-control event makes work available.
            await state.wake.wait()
            state.wake.clear()
            # Drain all currently available work before waiting again.
            while state.pending_finals or state.pending_active is not None:
                now = time.monotonic()
                # Finalized captions are mandatory and take priority.
                if state.pending_finals:
                    # popleft() preserves finalised-caption order.
                    caption = state.pending_finals.popleft()
                # Only send a draft after the finalised queue is empty.
                else:
                    log_mode(state, now)
                    # Flow control decides whether the draft may be sent now.
                    delay = flow.draft_delay(now)
                    if delay is None or delay > 0:
                        # Hold the draft while the client is behind. A newer draft
                        # may replace it before the delay expires.
                        try:
                            await asyncio.wait_for(state.wake.wait(), delay)
                        except asyncio.TimeoutError:
                            pass
                        state.wake.clear()
                        continue
                    caption = state.pending_active
                    state.pending_active = None
                # Removing a draft before sending prevents the same draft from
                # being sent twice if a newer source update wakes this task.
                # Only this per-client task sends through that client's WebSocket.
                await state.ws.send(json.dumps(caption_to_dict(caption)))
                # Probe periodically after a send. The resulting pong provides
                # an application-level indication of client responsiveness.
                probe_now = time.monotonic()
                if flow.should_probe(probe_now):
                    # Mark the probe before waiting for its pong so another
                    # probe cannot be started while this one is outstanding.
                    flow.on_probe(probe_now)
                    pong = await state.ws.ping()
                    # The callback records the RTT when the pong arrives.
                    pong.add_done_callback(lambda fut: on_pong(state, fut))
    except websockets.ConnectionClosed:
        pass  # handle_client's finally does the cleanup

# Receive and process the connection from caption_source.py.
async def handle_source(ws: ServerConnection, hello: SourceHello) -> None:
    global rsm
    # Read the lecture/session identifier from SOURCE_HELLO.
    session = hello.get("session")
    print(f"source connected: session={session}", flush=True)

    # Keep one revision manager for the server lifetime so a reconnecting
    # source cannot reintroduce stale updates.
    if rsm is None:
        rsm = RevisionStateManager(session)

    try:
        # Source input is handled independently from each browser sender.
        async for raw in ws:
            try:
                # Ignore malformed JSON rather than terminating the server.
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type")
            # Validate and accept only well-formed caption updates.
            if msg_type == "CAPTION_UPDATE":
                caption = parse_caption(msg)
                if caption is None:
                    print(f"dropped malformed update: {raw!r:.120}", flush=True)
                    continue
                # Reject duplicates and stale revisions.
                is_accepted, _ = rsm.process_caption(caption)
                if not is_accepted:
                    continue

                if caption.final:
                    # Finalized captions enter history and every client queue.
                    on_final(caption)
                else:
                    # Provisional captions update only the replaceable draft.
                    on_draft(caption)
            elif msg_type == "SOURCE_DONE":
                # SOURCE_DONE is a test-control message; clients do not need it.
                print("source finished", flush=True)
            else:
                # Ignore unknown message types for forward compatibility.
                continue
    except websockets.ConnectionClosed:
        pass  # Clients can still reconnect and recover from bounded history.
    finally:
        print("source disconnected", flush=True)

# Select finalised captions for a reconnecting or late-joining client.
def history_items(last_segment: int, last_revision: int,
                  history_seconds: float, now: float) -> list[Caption]:
    # Restrict recovery to the requested time window.
    cutoff = now - history_seconds
    # Resume after the client's checkpoint and avoid duplicates. A newer
    # revision of the same segment is also eligible for recovery.
    return [c for c in finals_log
            if (c.segment > last_segment
                or (c.segment == last_segment and c.revision > last_revision))
            and c.source_ts >= cutoff]

async def handle_client(ws: ServerConnection, hello: ClientHello) -> None:
    # CLIENT_HELLO carries the client's last finalised checkpoint. The server
    # uses it to send only captions the client may have missed.
    client_id = hello.get("client_id")

    try:
        # Normalise the checkpoint and requested history window from the hello.
        last_segment = max(0, int(hello.get("last_segment") or 0))
        last_revision = int(hello.get("last_revision") or 0)
        history_seconds = float(hello.get("history_seconds", DEFAULT_HISTORY_SECONDS))
    except (TypeError, ValueError):
        await ws.close(code=1008, reason="Invalid CLIENT_HELLO fields")
        return
    history_seconds = min(max(history_seconds, 0), MAX_HISTORY_SECONDS)
    if last_segment > 0:
        # A reconnecting client receives the maximum recoverable window.
        # rather than a short first-join recall window.
        history_seconds = MAX_HISTORY_SECONDS

    items = history_items(last_segment, last_revision,
                          history_seconds, time.time())
    # Recover the current provisional caption separately from finalised history.
    # It is sent only when it is newer than the client's checkpoint.
    pending = (
        active
        if active is not None
        and (active.segment > last_segment
             or (active.segment == last_segment
                 and active.revision > last_revision))
        else None
    )
    state = ClientState(ws=ws, pending_finals=deque(),
                        pending_active=pending,
                        client_id=client_id)
    # Register the client before live source updates can be distributed to it.
    clients[ws] = state
    print(f"client connected: id={client_id}; last_segment={last_segment}; "
          f"history={len(items)}; clients={len(clients)}", flush=True)

    try:
        # Send the recovery snapshot before starting live delivery so the
        # client receives missed finalised captions in a predictable order.
        await ws.send(json.dumps({"type": "HISTORY",
                                  "items": [caption_to_dict(c) for c in items]}))
        # Start the sender after the recovery snapshot; any active draft follows
        # as a normal CAPTION_UPDATE.
        state.sender = asyncio.create_task(client_sender(state))
        # The sender initially wakes to deliver any recovered active draft.
        state.wake.set()
        # Clients normally send nothing after CLIENT_HELLO.
        # Keep the socket
        # open until the client disconnects.
        async for _ in ws:
            pass
    except websockets.ConnectionClosed:
        pass
    finally:
        # Remove the client before cancelling its sender so new source updates
        # cannot enqueue additional work for a socket that is closing.
        clients.pop(ws, None)
        overloaded_clients.discard(ws)
        if state.monitor is not None:
            state.monitor.cancel()
            with suppress(asyncio.CancelledError):
                await state.monitor
        if state.sender is not None:
            # Stop and await the sender so it cannot outlive the connection.
            state.sender.cancel()
            with suppress(asyncio.CancelledError):
                await state.sender
        print(f"client disconnected: id={client_id}; mode={state.flow.mode.name}; "
              f"drafts_dropped={state.drafts_dropped}; "
              f"clients={len(clients)}", flush=True)

# Route each new WebSocket connection after validating its initial hello.
async def handler(ws) -> None:
    try:
        # Require the initial hello within five seconds.
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        # Converts the JSON message into a Python object.
        hello = json.loads(raw)
    except (asyncio.TimeoutError, json.JSONDecodeError, websockets.ConnectionClosed):
        # Reject a timeout or invalid JSON.
        # Reject a connection that closes before the hello.
        await ws.close(code=1008, reason="Expected a valid hello message")
        return
    
    # Route the connection according to the hello message type.
    msg_type = hello.get("type") if isinstance(hello, dict) else None
    if msg_type == "SOURCE_HELLO":
        await handle_source(ws, hello)
    elif msg_type == "CLIENT_HELLO":
        await handle_client(ws, hello)
    else:
        # Reject connections that do not identify as a source or client.
        await ws.close(code=1008, reason="Expected SOURCE_HELLO or CLIENT_HELLO")

# Start the WebSocket server and keep it running.
async def main_async(port: int) -> None:
    # Bind on all interfaces and cap individual WebSocket messages at 1 MiB.
    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        max_size=2**20,
        # client_monitor owns ping probes and the liveness timeout.
        ping_interval=None,
    ):
        print(f"LiveRecall baseline listening on ws://0.0.0.0:{port}", flush=True)
        await asyncio.Future()

def main() -> None:
    # Keep command-line compatibility with the required server entry point.
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    try:
        asyncio.run(main_async(args.port))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()

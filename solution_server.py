#!/usr/bin/env python3
"""
This is the fully implemented WebSocket broadcaster in which accepts caption
updates from the determinstic source, validates revisions, maintains bounded
finalised-caption history and broadcasts updates to browse clients
with per-client flow control and reconnect recovery.

As per the assignment specifications, it can be run with this command:
    python solution_server.py --port 8765
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import replace
from importlib import import_module
from typing import Any
from collections import deque
from data_structs import BaseClientState, Caption, ClientState, SourceHello, ClientHello
from revision_state_manager import RevisionStateManager
from flow_control import FlowControlManager

import websockets
from websockets.asyncio.server import ServerConnection

HISTORY_KEEP_SECONDS = 90 # how long the server keeps finalised captions
MAX_HISTORY_SECONDS = 60 # the maximum history a client can request
DEFAULT_HISTORY_SECONDS = 30 # default/recall window when the client does not specify one
MAX_PENDING_FINALS = 256  # maximum for per-client finalized-caption backlog



finals_log: deque[Caption] = deque()  # finals in segment order, trimmed to HISTORY_KEEP_SECONDS
active: Caption | None = None          # newest draft of the current segment
clients: dict[ServerConnection, BaseClientState] = {} # keyed by socket
overloaded_clients: set[ServerConnection] = set()
rsm: RevisionStateManager | None = None  # created on the first SOURCE_HELLO
flow_control = FlowControlManager()
source_done = False  # no new drafts can arrive after SOURCE_DONE

# In charge of parsing and validating an incoming caption message from the original source.
def parse_caption(msg: dict[str, Any]) -> Caption | None:
    try:
        # Converts to the expected types.
        caption = Caption.from_dict(msg)
        caption.segment = int(caption.segment)
        caption.revision = int(caption.revision)
        caption.source_ts = float(caption.source_ts)
    except (TypeError, ValueError):
        return None
    # Rejects revisions below 1 and ensures the caption text is a string.
    if caption.revision < 1 or not isinstance(caption.text, str):
        return None
    # Converts the final-status field to a Boolean.
    caption.final = bool(caption.final)
    return caption

# Converts an internal Caption object into a JSON-compatible
# dictionary for WebSocket transmission.
# By sending only the necessary things, it ensures that the message
# is small and avoids exposing unnecessary server-side data.
def caption_to_dict(caption: Caption) -> dict[str, Any]:
    """only forward the necessary fields"""
    return {
        "type": "CAPTION_UPDATE",
        "session": caption.session,
        "segment": caption.segment,
        "revision": caption.revision,
        "source_ts": caption.source_ts,
        "final": caption.final,
        "text": caption.text,
    }

#  Drop finals older than the 90-second recovery window and prune their
#  corresponding revision state.
def trim_finals_log(now: float) -> None:
    pruned_seg = 0
    # while finals_log is not empty and the oldest finalised caption is more than 90 seconds
    while finals_log and finals_log[0].source_ts < now - HISTORY_KEEP_SECONDS:
        pruned_seg = max(pruned_seg, finals_log.popleft().segment)
    if pruned_seg:
        # Remove the oldest caption from the left side of the deque
        rsm.prune(pruned_seg)


# Distribute the newest provisional caption to every client.
def on_draft(caption: Caption) -> None:
    global active
    active = caption
    # Stores the newest draft globally; as a segment can only receive multiple revision,
    # one should note only the last matters
    for state in clients.values():
        # Updates every connected browser client by itself.
        if state.pending_active is not None:
            state.drafts_dropped += 1
        # If a previous draft was waiting to be sent, it is now obsolete since a newer draft arrived.
        state.pending_active = caption
        # Replaces the client's draft with the newest one.
        state.wake.set()

# This handles a finalised caption from the original source.
def on_final(caption: Caption) -> None:
    global active
    # First, it removes finalised captions older than the history window.
    trim_finals_log(caption.source_ts)
    # Tries to store the new final caption for reconnect and late-join recovery.
    finals_log.append(caption)

    # If the finalised caption originally belongs to the current active segment,
    # the global draft is cleared since that segment is complete.
    if active is not None and active.segment == caption.segment:
        active = None
    # Processes every connected client independently.
    for state in clients.values():
        if state.ws in overloaded_clients:
            continue
        if len(state.pending_finals) >= MAX_PENDING_FINALS:
            # Do not silently drop finalized captions. Close an excessively
            # delayed client so it reconnects and recovers from finals_log.
            overloaded_clients.add(state.ws)
            state.pending_active = None
            print(f"client too far behind: id={state.client_id}; "
                  f"pending_finals={len(state.pending_finals)}", flush=True)
            asyncio.create_task(
                state.ws.close(code=1013, reason="Client too far behind")
            )
            continue
        # Queue timing belongs to each client, not the shared source caption.
        state.pending_finals.append(replace(caption, queue_ts=time.monotonic()))
        # The final supersedes any unsent draft of the same segment.
        if state.pending_active is not None and state.pending_active.segment == caption.segment:
            state.pending_active = None
        # Wakes the client's sender task so it can send the final caption.
        state.wake.set()


async def client_sender(state: BaseClientState) -> None:
    """Per-client sender: every final in order, then only the newest draft.

    This is the only place that awaits ws.send for this client, so a slow
    client never blocks the source or the other clients.
    """
    try:
        while True:
            # Sender sleeps until a new draft, final caption or even pong wakes it up
            await state.wake.wait()
            state.wake.clear()

            while state.pending_finals or state.pending_active is not None:
                # send unsent finals first (from oldest to newest)
                if state.pending_finals:
                    caption = state.pending_finals.popleft()
                    state.inflight_final = caption
                # if no more finals left to send, send the actively changing one
                else:
                    # Once the source has ended, only queued finals matter.
                    # Do not leave the final drain behind a draft policy that
                    # is designed for a still-live stream.
                    delay = 0.0 if source_done else state.draft_delay()
                    if delay is None or delay > 0:
                        # Client is behind: hold the draft (a newer one may replace
                        # it). Wait `delay` seconds, or with no timeout (None) until
                        # woken by the pong, a new final or a new draft.
                        try:
                            await asyncio.wait_for(state.wake.wait(), delay)
                        except asyncio.TimeoutError:
                            pass
                        state.wake.clear()
                        continue
                    caption = state.pending_active
                    state.pending_active = None

                try:
                    state.send_started_t = time.monotonic()

                    # Send a caption
                    await state.ws.send(json.dumps(caption_to_dict(caption)))
                    if caption.final:
                        state.last_final = (caption.segment, caption.revision)
                finally:
                    state.inflight_final = None
                    state.send_started_t = None

                # Observe a completed caption send (final or draft) for flow control.
                await state.after_send()

                # Yield to other runnable tasks (like ping flow control)
                await asyncio.sleep(0)
    except websockets.ConnectionClosed:
        pass  # handle_client's finally does the cleanup

# As a whole, it manages the connection from caption_source.py.
async def handle_source(ws: ServerConnection, hello: SourceHello) -> None:
    global rsm, source_done
    # Reads the lecture/session identifier from the source's initial hello message
    session = hello.get("session")
    source_done = False
    print(f"source connected: session={session}", flush=True)

    # One manager for the lifetime of the server, so a reconnecting source
    # can't re-send segments that clients already hold.
    if rsm is None:
        rsm = RevisionStateManager(session)

    try:
        # Waits for the caption messages from the source without blocking the browser clients.
        # Without handling, the server might wait for the source and then delay/block messages
        # to the browser clients.
        async for raw in ws:
            try:
                # Malformed JSON is thereby ignored so that the server does not crash.
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type")
            # For caption updates, it validates and then converts the raw message into a Caption.
            if msg_type == "CAPTION_UPDATE":
                caption = parse_caption(msg)
                if caption is None:
                    print(f"dropped malformed update: {raw!r:.120}", flush=True)
                    continue
                # Checks whether the caption is a valid newer revision.
                is_accepted, _ = rsm.process_caption(caption)
                if not is_accepted:
                    continue
                now = time.monotonic()
                # The legacy manager measures source rate. The newer policy
                # validates recovery using completed sends in after_send().
                flow_control.record_update(now, caption.final)

                if caption.final:
                    on_final(caption)
                else:
                    on_draft(caption)
            elif msg_type == "SOURCE_DONE":
                # Test-control message. The specification does not require it
                # to be forwarded to clients.
                source_done = True
                for state in clients.values():
                    state.wake.set()
                print("source finished", flush=True)
            else:
                # Forward compatibility: ignore fields/messages this baseline
                # doesn't understand rather than crashing.
                continue
    except websockets.ConnectionClosed:
        pass  # abrupt source drop: clients keep being served from history
    finally:
        print("source disconnected", flush=True)

# It selects the finalised captions in order to send it to a
# reconnecting or late-joining client.
def history_items(last_segment: int, last_revision: int,
                  history_seconds: float, now: float) -> list[Caption]:
    # Calculating the oldest timestamp that should still be included.
    cutoff = now - history_seconds
    # Returns only captions that satisfy both conditions:
    # c.segment > last_segment: The client has not already received this segment.
    # c.source_ts >= cutoff: the caption is within the requested history window.

    # This essentially prevents duplicate captions during reconnect. Additionally, it
    # also avoids sending captions older than the selected recall period.
    return [c for c in finals_log
            if (c.segment > last_segment
                or (c.segment == last_segment and c.revision > last_revision))
            and c.source_ts >= cutoff]

async def handle_client(ws: ServerConnection, hello: ClientHello) -> None:
    client_id = hello.get("client_id")

    try:
        last_segment = max(0, int(hello.get("last_segment") or 0))
        last_revision = int(hello.get("last_revision") or 0)
        history_seconds = float(hello.get("history_seconds", DEFAULT_HISTORY_SECONDS))
    except (TypeError, ValueError):
        await ws.close(code=1008, reason="Invalid CLIENT_HELLO fields")
        return
    history_seconds = min(max(history_seconds, 0), MAX_HISTORY_SECONDS)
    if last_segment > 0:
        # Resuming client: recover every final it missed (capped at 60 s),
        history_seconds = MAX_HISTORY_SECONDS

    items = history_items(last_segment, last_revision,
                          history_seconds, time.time())
    pending = (
        active
        if active is not None
        and (active.segment > last_segment
             or (active.segment == last_segment
                 and active.revision > last_revision))
        else None
    )
    wake = asyncio.Event()
    client_options = dict(ws=ws, pending_finals=deque(),
                          pending_active=pending, last_final=(last_segment, last_revision),
                          client_id=client_id, wake=wake)
    state: BaseClientState
    if flow_control.enabled:
        state = ClientState(**client_options,
                            flow=flow_control.create_policy(
                                ws, wake, client_id,
                                get_finals=lambda: (state.pending_finals, state.inflight_final),
                                get_send_started=lambda: state.send_started_t,
                            ))
    else:
        state = BaseClientState(**client_options)
    clients[ws] = state
    print(f"client connected: id={client_id}; last_segment={last_segment}; "
          f"history={len(items)}; clients={len(clients)}", flush=True)

    try:
        # Callbacks can now read the fully constructed state. Start before
        # history so a stalled history transfer is visible to the monitor.
        state.start()
        state.send_started_t = time.monotonic()
        try:
            await ws.send(json.dumps({"type": "HISTORY",
                                      "items": [caption_to_dict(c) for c in items]}))
        finally:
            state.send_started_t = None
        # The current draft (if any) follows as a normal CAPTION_UPDATE.
        state.sender = asyncio.create_task(client_sender(state))
        state.wake.set()
        # Student clients normally don't send further messages after hello.
        # Keep the WebSocket open until the client disconnects.
        async for _ in ws:
            pass
    except websockets.ConnectionClosed:
        pass
    finally:
        clients.pop(ws, None)
        state.close()
        overloaded_clients.discard(ws)
        if state.sender is not None:
            state.sender.cancel()
            await asyncio.gather(state.sender, return_exceptions=True)
        await state.aclose()
        print(f"client disconnected: id={client_id}; mode={state.delivery_status}; "
              f"drafts_dropped={state.drafts_dropped}; "
              f"clients={len(clients)}", flush=True)

# Should be the first function called for ever new WebSocket connection / new client connection.
async def handler(ws) -> None:
    try:
        # Waits for the intial hello and this must be sent within 5 seconds
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        # Converts the JSON message into a Python object.
        hello = json.loads(raw)
    except (asyncio.TimeoutError, json.JSONDecodeError, websockets.ConnectionClosed):
        # If the message times out, is invalid JSON or the connection closes, the server rejects it.
        await ws.close(code=1008, reason="Expected a valid hello message")
        return

    # Checks whether the decoded JSOn is an object and obtains its type.
    msg_type = hello.get("type") if isinstance(hello, dict) else None
    # Routes a caption source to handle_source()
    if msg_type == "SOURCE_HELLO":
        await handle_source(ws, hello)
    # Routes a browser client to handle_client()
    elif msg_type == "CLIENT_HELLO":
        await handle_client(ws, hello)
    else:
        # Else, it closes the connection as the message is not recognised
        await ws.close(code=1008, reason="Expected SOURCE_HELLO or CLIENT_HELLO")

# Start and keep the WebSocket server running.
async def main_async(port: int, flow_control_mode: str = "adaptive",
                     flow_control_module: str | None = None) -> None:
    global flow_control
    manager_type = FlowControlManager
    if flow_control_module is not None:
        if flow_control_module not in ("flow_control", "flow_control_old"):
            raise ValueError("Unknown flow-control module")
        manager_type = import_module(flow_control_module).FlowControlManager
    flow_control = manager_type(enabled=flow_control_mode == "adaptive")
    # Bind all network interfaces and limit each message to 1 MiB.
    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        max_size=2**20,
        ping_interval=20,
        ping_timeout=20,
    ):
        print(f"LiveRecall baseline listening on ws://0.0.0.0:{port}", flush=True)
        # Confirm that it started successfully.
        await asyncio.Future()

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--flow-control", choices=("adaptive", "none"), default="adaptive",
                        help="Flow-control policy (default: adaptive)")
    parser.add_argument("--flow-control-module", choices=("flow_control", "flow_control_old"),
                        help="Implementation module (default: imported FlowControlManager)")
    args = parser.parse_args()
    try:
        asyncio.run(main_async(args.port, args.flow_control, args.flow_control_module))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()

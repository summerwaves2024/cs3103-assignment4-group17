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
from typing import Any
from collections import deque
from functools import lru_cache
from data_structs import Caption, ClientState, SourceHello, ClientHello
from revision_state_manager import RevisionStateManager

import websockets
from websockets.asyncio.server import ServerConnection

HISTORY_KEEP_SECONDS = 90 # how long the server keeps finaslied captions 
MAX_HISTORY_SECONDS = 60 # the maximum history a client can request 
DEFAULT_HISTORY_SECONDS = 30 # default/recall window when the client does not specify one
MAX_PENDING_FINALS = 256  # maximum for per-client finalized-caption backlog
STREAM_WINDOW = 10.0 # duration of the window used to measure the source stream rate, which is then used in flow control



finals_log: deque[Caption] = deque()  # finals in segment order, trimmed to HISTORY_KEEP_SECONDS
active: Caption | None = None          # newest draft of the current segment
clients: dict[ServerConnection, ClientState] = {} # keyed by socket
overloaded_clients: set[ServerConnection] = set()
rsm: RevisionStateManager | None = None  # created on the first SOURCE_HELLO
update_times: deque[float] = deque() # source updates used to measure stream rate
source_done = False
terminal_finals_snapshot: tuple[Caption, ...] = ()
active_session: str | None = None

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


@lru_cache(maxsize=2048)
def _caption_json(session: str, segment: int, revision: int,
                 source_ts: float, final: bool, text: str) -> str:
    """Serialize an immutable caption payload once for all recipients.

    Caption objects are not mutated after they pass revision validation, so the
    tuple of wire fields is a safe cache key.  The bound prevents a long-lived
    source from turning this optimisation into unbounded memory growth.
    """
    return json.dumps({
        "type": "CAPTION_UPDATE",
        "session": session,
        "segment": segment,
        "revision": revision,
        "source_ts": source_ts,
        "final": final,
        "text": text,
    })


def caption_to_json(caption: Caption) -> str:
    return _caption_json(
        caption.session, caption.segment, caption.revision,
        caption.source_ts, caption.final, caption.text,
    )

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

# Overall, is in charge of calculating how quickly the caption source 
# is sending updates.
def stream_rate(now: float) -> float:
    while update_times and update_times[0] < now - STREAM_WINDOW:
        update_times.popleft()
    return len(update_times) / STREAM_WINDOW

# In short, it handles a new provisional caption from the original source
# and distributes the latest version to every client.
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
        # Coalesce redundant wakeups when several source revisions arrive
        # before this client's sender task gets scheduled.
        if not state.wake.is_set():
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
        state.pending_finals.append(caption)
        # Queue pressure is per-client. Once even one final is waiting, hold
        # drafts until that final is delivered so provisional traffic cannot
        # delay finalized-caption recovery for a slow client.
        state.final_pressure = bool(state.pending_finals)
        # The final supersedes any unsent draft of the same segment.
        if state.pending_active is not None and state.pending_active.segment == caption.segment:
            state.pending_active = None
        # Wakes the client's sender task so it can send the final caption.
        if not state.wake.is_set():
            state.wake.set()

# As a whole, it is called when a client replies to the server's ping.
def on_pong(state: ClientState, future: asyncio.Future) -> None:
    # Ignores the failed or cancelled pong operations
    if future.cancelled() or future.exception() is not None:
        return
    # Gets a monotonic time stamp to accurately measure time
    now = time.monotonic()
    # Passes the pong time so that it can: calculate the ping round-trip time, determine if the client
    # is late, update the reading rate and change between the states. 
    state.flow.on_pong(now)
    # Wake up the sender so that it can recheck whether a held draft can be resent.
    if not state.wake.is_set():
        state.wake.set()

# client_sender is the dedicated sending loop to one browser client. As a whole, 
# the purpose is to ensure that a slow client does not block the source or other clients.
async def client_sender(state: ClientState) -> None:
    ## Per-client sender: every final in order, then only the newest draft.
    # This is the only place that awaits ws.send for this client, so a slow
    # client never blocks the source or the other clients.
    flow = state.flow
    try:
        while True:
            # Sender sleeps until a new draft, final caption or even pong wakes it up
            await state.wake.wait()
            state.wake.clear()
            while (state.pending_finals or state.pending_active is not None
                   or (source_done and not state.terminal_sync_sent)):
                if source_done and not state.terminal_sync_sent:
                    missing = [
                        c for c in terminal_finals_snapshot
                        if (c.segment > state.last_final[0]
                            or (c.segment == state.last_final[0]
                                and c.revision > state.last_final[1]))
                    ]
                    state.pending_finals.clear()
                    state.pending_active = None
                    state.terminal_sync_sent = True
                    if missing:
                        await state.ws.send(json.dumps({
                            "type": "HISTORY",
                            "items": [
                                {key: value for key, value in caption_to_dict(c).items()
                                 if key != "type"}
                                for c in missing
                            ],
                        }))
                        state.last_final = (
                            missing[-1].segment, missing[-1].revision
                        )
                    continue

                now = time.monotonic()
                # send unsent finals first (from oldest to newest)
                if state.pending_finals:
                    caption = state.pending_finals.popleft()
                # if no more finals left to send, send the actively changing one
                else:
                    # If there are no pending finals, it checks flow control.
                    delay = (
                        None if state.final_pressure
                        else flow.draft_delay(now)
                    )
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
                # Only this per-client task sends through that client's WebSocket.
                await state.ws.send(caption_to_json(caption))
                # Advance the reconnect checkpoint only after the WebSocket
                # send completes. If the connection drops during send, the
                # final remains recoverable from history on reconnect.
                if caption.final:
                    state.last_final = (caption.segment, caption.revision)
                flow.on_send()
                if caption.final and not state.pending_finals:
                    state.final_pressure = False
                # Ping after sending: the pong only returns once the client has
                # read up to here, which is how flow control sees its lag.
                # In short, the pong is how quickly the client is consuming data.
                if flow.should_probe(now):
                    flow.on_probe(now)
                    pong = await state.ws.ping()
                    pong.add_done_callback(lambda fut: on_pong(state, fut))
    except websockets.ConnectionClosed:
        pass  # handle_client's finally does the cleanup

# As a whole, it manages the connection from caption_source.py. 
async def handle_source(ws: ServerConnection, hello: SourceHello) -> None:
    global rsm, source_done, active_session, active, terminal_finals_snapshot
    # Reads the lecture/session identifier from the source's initial hello message
    session = hello.get("session")

    # A different session is a new lecture, not a source reconnect.  Do not
    # let the previous lecture's revision state or history reject/serve the
    # new lecture's captions.
    if active_session is not None and session != active_session:
        # Existing sockets belong to the old lecture. Close them before the
        # new source starts publishing so they cannot receive mixed sessions.
        old_clients = list(clients)
        clients.clear()
        for old_ws in old_clients:
            asyncio.create_task(
                old_ws.close(code=1001, reason="Lecture session changed")
            )
        finals_log.clear()
        terminal_finals_snapshot = ()
        update_times.clear()
        active = None
        rsm = RevisionStateManager(session)

    source_done = False
    terminal_finals_snapshot = ()
    active_session = session
    # A source reconnect starts a new delivery phase for existing clients.
    # Otherwise a client that saw an earlier SOURCE_DONE would never receive
    # the completion fence for this resumed source connection.
    for state in clients.values():
        state.terminal_sync_sent = False
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

                # Prune update_times so that the flow calculation remains
                # bounded even if no clients are connected.
                stream_rate(now)

                update_times.append(now)

                if caption.final:
                    on_final(caption)
                else:
                    on_draft(caption)
            elif msg_type == "SOURCE_DONE":
                # Test-control message. The specification does not require it
                # to be forwarded to clients.
                source_done = True
                # Freeze one shared completion boundary.  All clients now
                # drain the same finalized history, even if their sender
                # tasks are scheduled at slightly different times.
                terminal_finals_snapshot = tuple(finals_log)
                for state in clients.values():
                    if not state.wake.is_set():
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
    session = hello.get("session")
    if active_session is not None and session != active_session:
        await ws.close(code=1008, reason="Client session does not match source")
        return
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
    state = ClientState(ws=ws, pending_finals=deque(),
                        pending_active=pending, last_final=(last_segment, last_revision),
                        client_id=client_id)
    clients[ws] = state
    print(f"client connected: id={client_id}; last_segment={last_segment}; "
          f"history={len(items)}; clients={len(clients)}", flush=True)

    try:
        await ws.send(json.dumps({"type": "HISTORY",
                                  "items": [caption_to_dict(c) for c in items]}))
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
        overloaded_clients.discard(ws)
        if state.sender is not None:
            state.sender.cancel()
        print(f"client disconnected: id={client_id}; mode={state.flow.mode.name}; "
              f"drafts_dropped={state.drafts_dropped}; "
              f"pending_finals={len(state.pending_finals)}; "
              f"last_final={state.last_final}; "
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

# As a whole, it starts and keeps the WebSockets server running.
async def main_async(port: int) -> None:
    # Starts the server with the given port and binds to all the network interfaces.
    # By having 2**20 for the max size, you limit the WebSocket message to 1 MiB, thereby 
    # preventing unexpectedly large messages.
    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        max_size=2**20,
        # Keepalive is handled by the clients and the application-level
        # flow-control probes; avoid a second server-side ping stream.
        ping_interval=None,
    ):
        print(f"LiveRecall baseline listening on ws://0.0.0.0:{port}", flush=True)
        # Confirm that it started successfully.
        await asyncio.Future()

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    try:
        asyncio.run(main_async(args.port))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()

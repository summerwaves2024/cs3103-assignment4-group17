#!/usr/bin/env python3
"""
CS3103 Assignment 4 - LiveRecall naive working baseline.

This starter is intentionally useful but incomplete. It provides the required
CLI and a basic WebSocket broadcaster so that caption_source.py -> server ->
browser/load clients works on day one.

It DOES NOT implement the graded networking mechanisms:
  - revision-state management or deduplication
  - recent-history recovery or late-join recovery
  - bounded per-client application queues
  - slow-client isolation
  - semantic coalescing / backpressure
  - constrained-bandwidth overload handling

Required evaluator command:
    python solution_server.py --port 8765

Teams should replace or substantially extend the implementation while keeping
this command-line interface and the protocol in the assignment specification.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from typing import Any
from collections import deque
from data_structs import Caption, ClientState, SourceHello, ClientHello
from revision_state_manager import RevisionStateManager

import websockets
from websockets.asyncio.server import ServerConnection

HISTORY_KEEP_SECONDS = 90
MAX_HISTORY_SECONDS = 60
DEFAULT_HISTORY_SECONDS = 30

# Naive global set of currently connected student clients.
# There is deliberately no per-client queue or recovery state here.
CLIENTS: set[Any] = set()

finals_log: deque[Caption] = deque()  # finals in segment order, trimmed to HISTORY_KEEP_SECONDS
active: Caption | None = None          # newest draft of the current segment
clients: dict[int, ClientState] = {}
rsm: RevisionStateManager | None = None  # created on the first SOURCE_HELLO


async def broadcast_naively(raw_message: str) -> None:
    """Send one message directly to every connected client.

    This is intentionally a naive baseline. The source handler waits for all
    sends to complete. Under pressure, this design may accumulate delay and is
    not the target architecture for the assignment.
    """
    if not CLIENTS:
        return

    snapshot = list(CLIENTS)
    results = await asyncio.gather(
        *(client.send(raw_message) for client in snapshot),
        return_exceptions=True,
    )
    for client, result in zip(snapshot, results):
        if isinstance(result, Exception):
            CLIENTS.discard(client)


def parse_caption(msg: dict[str, Any]) -> Caption | None:
    try:
        caption = Caption.from_dict(msg)
        caption.segment = int(caption.segment)
        caption.revision = int(caption.revision)
        caption.source_ts = float(caption.source_ts)
    except (TypeError, ValueError):
        return None
    if caption.revision < 1 or not isinstance(caption.text, str):
        return None
    caption.final = bool(caption.final)
    return caption


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


def trim_finals_log(now: float) -> None:
    """Drop finals older than the history window, and their segment state"""
    pruned_seg = 0
    while finals_log and finals_log[0].source_ts < now - HISTORY_KEEP_SECONDS:
        pruned_seg = max(pruned_seg, finals_log.popleft().segment)
    if pruned_seg:
        rsm.prune(pruned_seg)


def on_draft(caption: Caption) -> None:
    global active
    active = caption


def on_final(caption: Caption) -> None:
    global active
    trim_finals_log(caption.source_ts)
    finals_log.append(caption)

    if active is not None and active.segment == caption.segment:
        active = None


async def handle_source(ws: ServerConnection, hello: SourceHello) -> None:
    global rsm
    session = hello.get("session")
    print(f"source connected: session={session}", flush=True)

    # One manager for the lifetime of the server, so a reconnecting source
    # can't re-send segments that clients already hold.
    if rsm is None:
        rsm = RevisionStateManager(session)

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type")
            if msg_type == "CAPTION_UPDATE":
                caption = parse_caption(msg)
                if caption is None:
                    print(f"dropped malformed update: {raw!r:.120}", flush=True)
                    continue
                is_accepted, _ = rsm.process_caption(caption)
                if not is_accepted:
                    continue
                if caption.final:
                    on_final(caption)
                else:
                    on_draft(caption)
                await broadcast_naively(json.dumps(caption_to_dict(caption)))
            elif msg_type == "SOURCE_DONE":
                # Test-control message. The specification does not require it
                # to be forwarded to clients.
                print("source finished", flush=True)
            else:
                # Forward compatibility: ignore fields/messages this baseline
                # doesn't understand rather than crashing.
                continue
    except websockets.ConnectionClosed:
        pass  # abrupt source drop: clients keep being served from history
    finally:
        print("source disconnected", flush=True)


def history_items(last_segment: int, history_seconds: float, now: float) -> list[Caption]:
    cutoff = now - history_seconds
    return [c for c in finals_log
            if c.segment > last_segment and c.source_ts >= cutoff]


async def handle_client(ws: ServerConnection, hello: ClientHello) -> None:
    client_id = hello.get("client_id")

    try:
        last_segment = max(0, int(hello.get("last_segment") or 0))
        history_seconds = float(hello.get("history_seconds", DEFAULT_HISTORY_SECONDS))
    except (TypeError, ValueError):
        await ws.close(code=1008, reason="Invalid CLIENT_HELLO fields")
        return
    history_seconds = min(max(history_seconds, 0), MAX_HISTORY_SECONDS)
    if last_segment > 0:
        # Resuming client: recover every final it missed (capped at 60 s),
        history_seconds = MAX_HISTORY_SECONDS

    items = history_items(last_segment, history_seconds, time.time())
    CLIENTS.add(ws)
    print(f"client connected: id={client_id}; last_segment={last_segment}; "
          f"history={len(items)}; clients={len(CLIENTS)}", flush=True)

    try:
        await ws.send(json.dumps({"type": "HISTORY",
                                  "items": [caption_to_dict(c) for c in items]}))
        # Student clients normally don't send further messages after hello.
        # Keep the WebSocket open until the client disconnects.
        async for _ in ws:
            pass
    except websockets.ConnectionClosed:
        pass
    finally:
        CLIENTS.discard(ws)
        print(f"client disconnected: id={client_id}; clients={len(CLIENTS)}", flush=True)


async def handler(ws) -> None:
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        hello = json.loads(raw)
    except (asyncio.TimeoutError, json.JSONDecodeError, websockets.ConnectionClosed):
        await ws.close(code=1008, reason="Expected a valid hello message")
        return

    msg_type = hello.get("type") if isinstance(hello, dict) else None
    if msg_type == "SOURCE_HELLO":
        await handle_source(ws, hello)
    elif msg_type == "CLIENT_HELLO":
        await handle_client(ws, hello)
    else:
        await ws.close(code=1008, reason="Expected SOURCE_HELLO or CLIENT_HELLO")


async def main_async(port: int) -> None:
    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        max_size=2**20,
        ping_interval=20,
        ping_timeout=20,
    ):
        print(f"LiveRecall baseline listening on ws://0.0.0.0:{port}", flush=True)
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

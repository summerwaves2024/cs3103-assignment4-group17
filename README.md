# Team 17 - LiveRecall

## Team members

- Lim Jinle, Alexius / A0309160M / flow control
- Ong Li Min / A0309313M / client logic and UI
- James Chin Jia Jun / <Matric number> / revision replacement
- Tey Kai Jie / A0305815H / reconnection

## Overview

LiveRecall is a real-time lecture-captioning system. A deterministic caption source sends provisional and finalized revisions to a WebSocket server, which distributes them to multiple browser clients. The system supports revision replacement, bounded history, reconnect recovery, late joining, local recall, per-client flow control, and slow-client isolation.

## Ubuntu setup

The recommended environment for reproducible performance measurements is Ubuntu Linux.

Required software:

- Python 3
- python3-venv
- python3-pip
- iproute2

## Install

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip iproute2
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Required server

```bash
python solution_server.py --port 8765
```

## Browser client

```bash
python -m http.server 8000 --directory client
```
Open `http://127.0.0.1:8000` in one or more browser windows.

## Public deterministic tests

The following commands reproduce the public T1–T5 experiments. Each experiment performs three independent runs and reports the median results.

### T1 – Basic correctness

```bash
python run_demo.py --clients 20 --duration 20 --runs 3 --output-stem t1
```

### T2 – Public scale

```bash
python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t2
```

### T3 – Slow clients

```bash
python run_demo.py --clients 100 --duration 60 --runs 3 \
  --slow-percent 10 --slow-ms 500 --output-stem t3
```

Optional memory observation in another terminal:

```bash
python monitor_server.py --match solution_server.py --duration 60 --output t3_memory.csv
```

`monitor_server.py` is an optional Ubuntu observation tool. It locates the
server process and records its resident set size at regular intervals in a
CSV file. The output can be used to check that per-client queues and bounded
history remain within a predictable memory range during the experiment. The
monitor does not change the server or the grading protocol.

### T4 – Reconnect and recovery

```bash
python run_demo.py --clients 100 --duration 60 --runs 3 \
  --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5 \
  --output-stem t4
```

### T5 – Network impairment

Run this test inside Ubuntu. First apply the network profile:

```bash
sudo ./network_profiles/delay_loss.sh lo
```

Then run the experiment:

```bash
python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t5
```

Reset the interface after the experiment:

```bash
sudo ./network_profiles/reset.sh lo
```

WebSocket runs over TCP. Packet loss is therefore expected to appear mainly as retransmission, head-of-line delay, and increased tail latency rather than as ordinary application-message loss.

## Mandatory deterministic application demo

Run the following components in separate terminals.

### Terminal 1: WebSocket server

```bash
python solution_server.py --port 8765
```

### Terminal 2: Browser client server

```bash
python -m http.server 8000 --directory client
```

### Terminal 3: Deterministic caption source

```bash
python caption_source.py --duration 120
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000) in one or more browser windows.

### Revision replacement

Observe the live-caption panel while the source is running. The same segment should update as newer provisional revisions arrive, and its finalized revision should replace the provisional text without creating a duplicate row.

### Recall

Click **What did I miss?** and switch between the 15-, 30-, and 60-second options. The panel displays recent finalized captions in chronological order.

### Late joining

Open another browser window after the source has been running for a while. It should load recent finalized captions and continue receiving live updates.

### Reconnect and recovery

Close or temporarily disconnect one browser client, wait briefly, and reconnect it. The client should recover missed finalized captions and display a reconnect notice without creating duplicate rows.

### Optional: slow-client demo with `tools/slow_proxy.py`

A slow browser tab does not slow down WebSocket pongs, because the browser answers pings outside the page's JavaScript. To show flow control with real browsers, `tools/slow_proxy.py` sits between the tabs and the server and limits how fast each connection receives data, so a throttled tab is slow on the network. It is a demo helper only and is not part of the evaluated server.

The browser client always connects to port 8765, so the proxy listens there and the server moves to 8767. Run each command in its own terminal:

```bash
python solution_server.py --port 8767                        # server
python tools/slow_proxy.py --listen 8765 --target 8767       # proxy (browser tabs connect here)
python -m http.server 8000 --directory client                # browser client
python caption_source.py --uri ws://127.0.0.1:8767 --duration 180   # source, after opening the tabs
```

Open two tabs at [http://127.0.0.1:8000](http://127.0.0.1:8000), then type commands into the proxy terminal. Rates are bytes per second from the server to the client; `0` means unthrottled.

| Command | Effect |
|---|---|
| `list` | Show connections (`[1]`, `[2]`, ...) and their rates |
| `<id> <rate>` | Throttle one connection, e.g. `2 100` |
| `all <rate>` | Throttle every connection, including ones opened later |

Throttling a tab with `2 100` makes it fall behind. Within about 10–20 s the server log shows `NORMAL -> BEHIND -> FINALS_ONLY` for that client, and the tab keeps receiving finalized captions while the other tab still receives every revision. `2 0` removes the limit. Once the finals-only period ends (60 s), the server log shows `FINALS_ONLY -> NORMAL` and provisional captions resume.

At about 200 B/s a tab barely lags, 100 B/s is an appropriate amount of lag to demonstrate a slow client, and around 50 B/s even finalized captions alone cannot keep up. Refreshing a tab opens a new connection with the default rate, so use `list` to find its new id.

## Design notes

### Revision and deduplication state

The system stores caption state by segment and revision. Newer revisions replace older provisional text, while stale or duplicate updates are rejected. Once a caption is finalized, it cannot be overwritten by a later update. On the browser side, each segment has one DOM element, so revisions update the existing row rather than creating duplicates.

### Bounded server history

The server maintains a bounded 90-second history of finalized captions for reconnect recovery and late joining. Captions outside this window are removed from both the finalized-caption history and the revision manager, while the browser applies the same retention limit to its local caption state.

### Reconnect and late-join recovery

When a client connects or reconnects, it sends the segment and revision of its latest finalized caption. The server uses this checkpoint to return only the finalized captions missed within the recovery window. The browser merges recovered captions using the same revision and deduplication rules as live updates, then displays a notice indicating whether captions were recovered or whether the client joined partway through the lecture.

### Per-client buffering and backpressure

Each browser client has an independent outgoing queue and sender task. As a result, a slow receiver can fall behind without blocking the caption source or delaying other clients. Finalized captions are retained and delivered in order, while only the newest unsent provisional caption is kept because older drafts are superseded by later revisions. WebSocket ping/pong measurements are used to estimate client responsiveness and decide when provisional updates should be held.

The flow-control manager uses three states. In `NORMAL`, both finalized captions and provisional updates may be sent. If a ping becomes overdue, the client enters `BEHIND`: provisional updates are held, but finalized captions continue to be delivered. A second late pong within the configured lag interval indicates sustained lag and moves the client to `FINALS_ONLY`, where the server sends finalized captions without issuing further probes. After a bounded recovery period, the client returns to `NORMAL` and provisional delivery is attempted again. This prevents fast pongs from an already-throttled client from being mistaken for full recovery.

Finalized captions remain prioritised whenever a client has both finalised and
provisional updates waiting. Recovery uses the checkpoint the client reports in
`CLIENT_HELLO`, not a record of what the server sent, so a caption lost when a
connection drops mid-delivery is still resent on reconnect.

The server also performs periodic responsiveness monitoring through
WebSocket ping and pong messages. A completed pong provides an RTT sample for
the relevant client. The flow-control policy uses these samples to distinguish
normal delivery from a client that may be falling behind. Monitoring is kept
per client so a delayed browser does not affect the delivery policy for other
clients.

### Optional server monitoring

The repository includes `monitor_server.py` as a lightweight diagnostic tool
for Ubuntu. It samples the server process resident memory and writes elapsed
time together with memory usage to a CSV file. This is useful when comparing
queue behaviour across public experiments but it is not required to run the
application and it does not participate in caption delivery.

### Overload behaviour and trade-offs

If a client cannot keep up with the incoming stream, provisional captions may be delayed or discarded because newer drafts supersede older ones. Finalized captions are prioritized and retained in the client’s pending queue. This protects final-caption completeness, although severely slow clients may receive provisional updates late or not at all.

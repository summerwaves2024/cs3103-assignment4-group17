# Team 17 - LiveRecall

## Team members

- Lim Jinle, Alexius / <Matric number> / <responsibility>
- Ong Li Min / <Matric number> / <responsibility>
- James Chin Jia Jun / <Matric number> / <responsibility>
- Tey Kai Jie / <Matric number> / <responsibility>

## Overview

LiveRecall is a real-time lecture captioning system. A deterministic caption source sends provisional and final caption revisions to a WebSocket server, which distributes them to multiple browser clients. The system supports revision replacement, bounded history, reconnect recovery, late joining, local recall, per-client flow control, and slow-client isolation.

## Ubuntu setup

The reproducible performance environment is Ubuntu Linux.

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
Open `http://127.0.0.1:8000` in at least two browser windows.

## Public deterministic tests

Exact commands and configuration used for T1-T5, including independent runs.

### T1 - Basic correctness

```bash
python run_demo.py --clients 20 --duration 20 --runs 3 --output-stem t1
```

### T2 - Public scale

```bash
python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t2
```

### T3 - Slow clients

```bash
python run_demo.py --clients 100 --duration 60 --runs 3 \
  --slow-percent 10 --slow-ms 500 --output-stem t3
```

Optional memory observation in another terminal:

```bash
python monitor_server.py --match solution_server.py --duration 60 --output t3_memory.csv
```

### T4 - Reconnect

```bash
python run_demo.py --clients 100 --duration 60 --runs 3 \
  --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5 \
  --output-stem t4
```

### T5 - Network impairment

Run this test inside Ubuntu. In one terminal:

```bash
sudo ./network_profiles/delay_loss.sh lo
```

Then run, for example:

```bash
python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t5
```

Always reset afterwards:

```bash
sudo ./network_profiles/reset.sh lo
```

WebSocket runs over TCP. Packet loss is therefore expected to show up mainly as retransmission/head-of-line delay and larger tail latency, not as ordinary application-message loss.

## Mandatory deterministic application demo

Run the components in separate terminals.

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

Observe the live-caption panel while the source is running. The same caption segment should update as newer provisional revisions arrive, and the final revision should replace the provisional text without creating duplicate rows.

### Recall

Click **What did I miss?** and switch between the 15-, 30-, and 60-second options. The panel displays recent finalized captions in chronological order.

### Late joining

Open a second browser window after the source has been running for a while. It should load recent finalized captions and continue receiving live updates.

### Reconnect and recovery

Close or temporarily disconnect one browser client, wait briefly, and reconnect it. The client should recover missed finalized captions and display a reconnect notice without creating duplicate rows.

## Design notes

### Revision and deduplication state

The system stores caption state by segment and revision, allowing newer revisions to replace older provisional text while rejecting stale or duplicate updates. Once a caption has been finalized, it cannot be overwritten by a later update. On the browser side, each segment has a single DOM element, so revisions update the existing caption row rather than creating duplicates.

### Bounded server history

The server maintains a bounded 90-second history of finalized captions to support reconnect recovery and late joining without storing the entire lecture indefinitely. Captions outside this time window are removed from both the finalized-caption history and the revision manager, while the browser applies the same retention limit to its local caption state.

### Reconnect and late-join recovery

When a client connects or reconnects, it sends the segment and revision of its latest finalized caption. The server uses this information to identify and return only the finalized captions that the client missed within the recovery window. The browser merges recovered captions using the same revision and deduplication rules as live updates and displays a notice indicating whether captions were recovered or whether the client joined partway through the lecture.

### Per-client buffering and backpressure
Each browser client has an independent outgoing buffer and sender task, ensuring that a slow receiver does not block the source or other connected clients. Finalized captions are queued separately and delivered in order, while only the newest unsent provisional caption is retained because earlier drafts become obsolete when newer revisions arrive. Ping/pong measurements are used to estimate each client's responsiveness and determine whether provisional updates should be delayed.

### Overload behaviour and trade-offs

If a client cannot keep up with the incoming stream, provisional captions may be delayed or discarded because newer drafts supersede older ones, while finalized captions are prioritized and retained in the client's pending queue. This preserves final-caption completeness, although severely slow clients may experience delayed provisional updates.

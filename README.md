# Team 17 - LiveRecall

## Team members

- Lim Jinle, Alexius / <Matric number> / <responsibility>
- Ong Li Min / <Matric number> / <responsibility>
- James Chin Jia Jun / <Matric number> / <responsibility>
- Tey Kai Jie / <Matric number> / <responsibility>

## Overview
LiveRecall is a real-time lecture captioning system. A deterministic caption source sends provisional and final caption revisions to a WebSocket server, which distributes them to multiple browser clients. The system supports revision replacement, bounded history, reconnect recovery, late joining, local recall, per-client flow control and slow-client isolation.

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

Ubuntu only. In one terminal:

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

Terminal 1: WebSocket Server 
```bash 
python solution_server.py --port 8765
```
Termainal 2: Browser Client Server
```bash
python -m http.server 8000 --directory client
```
Terminal 3: Start the Determinstic Caption Source
```bash 
python caption_source.py --duration 120
```
Open http://127.0.0.1:8000 in one or more browser windows.

### To test revision replacement
Observe the live-caption panel while the source is running. The same caption segment should update as newer provisional revisions arrive, and the final revision should replace the provisional text without creating duplicate rows. 

### To test recall 
Click on *What did I miss?* and switch between the 15, 30 and 60 second options. Indeed, the panel should display the recent finalised captions in chronological order. 

### To test late joining
Open a second browser window after the source has been running for a while. It should load recent finalised captions and continue receiving live updates. 

### To test reconnect and recovery
Close or temporarily disconnect one browser client, wait briefly and reconnect it. The client should be able to recover missed finalised captions and display a reconnect notice without having duplicate rows.

## Design notes

### Revision and deduplicate state
The system stores caption store by segment and revision, thereby allowing newer revisions to replace older provisional text while also rejecting stale or duplicate updates. Additionally, once a caption has been already finalised, it is unable to be overwritten by a later update. On the browser side, each segment is associated with a single DOM element, and as such, revisions update the existing caption row rather than creating duplicates.

### Bounded Server History 
Our implementation ensures that the server maintains a bounded 90-second history of finalised captions in order to better support reconnect recovery and late joining without storing the entire lecture indefinitely or having an unbounded queue. All captions that fall outside of the time window are removed from both the finalised caption history and the revision manager, while the browser applies the same retention limit to its local caption state. 
- per-client buffering/backpressure policy;
- overload behavior and trade-offs.

### Reconnect and late-join recovery
When a client connects or reconnects, it sends the segment and revision of its latest finalised caption. With that, the server then uses this information in order to identify and return only the finalised captions in which the client has missed within the recovery window. Furthermore, the browser then merges the recovered captions using the same revision and dedpulication rules as live updates and displays a notice indicating whether captions were recovered or whether the client joined halfway through the lecture. 

### Per-client buffering and backpressure
Every browser client has an independent outgoing buffer and sender task, thereby ensuring that a slow reciever does not block the source or other connected clients. Finalised captions are then queued separately and delivered in order, while only the newest unsent provisional captions are retained as earlier drafts become obsolete as newer revisions arrive. Additionally, ping/pong measurements are used to estimate each client's responsiveness and determine whether provisional updates need to be delayed.

### Overload Behaviour and trade-offs. 
If a client is unable to keep up with the incoming stream, provisional captions might be delayed or discarded since newer drafts superseded the older ones, whereas finalised captions are then priortised and retained in the client's pending queue. This therefore preserves final-caption completes, although severly slow clients may experience delayed provisional updates. 
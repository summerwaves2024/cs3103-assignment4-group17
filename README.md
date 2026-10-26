# Team 17 - LiveRecall

## Team members

- Ong Li Min / A0309313M / responsibility
- <Name> / <Matric number> / <responsibility>

## Overview
LiveRecall is a real-time lecture captioning system. A deterministic caption source sends provisional and final caption revisions to a WebSocket server, which distributes them to multiple browser clients.
The system supports revision replacement, bounded history, reconnect recovery, late joining, local recall, per-client flow control and slow-client isolation.

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
Open http://127.0.0.1:8000

## Public deterministic tests

Run each test at least three times and report the median result.

### T1 - Basic correctness

```bash
python run_demo.py --clients 20 --duration 20 --runs 3
```

### T2 - Public Scale 
```bash
python run_demo.py --clients 100 --duration 40 --runs 3
```

### T3 - Slow Clients
```bash 
python run_demo.py --clients 100 --duration 60 --runs 3 --slow-percent 10 --slow-ms 500
```

### T4 - Reconnect 
```bash 
python run_demo.py --clients 100 --duration 60 --runs 3 --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5
```

### T5 - Network impairment 
Run the server and clients inside Ubuntu and apply a suitable tc/netem profile from network_profiles/.

Record P50/P95/P99 caption age, final-caption completeness, freshness, update coverage, recovery time and slow-client behavior.

## Mandatory deterministic application demo

Exact commands/steps for `caption_source.py`, browser clients, revision replacement, recall, late join, and reconnect/recovery.

## Design notes

- Caption state is stored by segment and revision. Newer revisions replace older revisions, while stale and duplicate updates are ignored.
- Finalized captions cannot be overwritten by older revisions.
- The server maintains bounded finalized-caption history for recovery and late-join requests.
- Clients send their latest finalized segment and revision during reconnect.
- Each client has independent buffering so slow clients do not block normal clients.
- Obsolete provisional revisions may be coalesced or discarded under pressure, while finalized captions are prioritized.
- The browser stores recent finalized captions locally for recall.

## Optional ASR bonus

Only if attempted. Keep these dependencies separate from the mandatory server path.

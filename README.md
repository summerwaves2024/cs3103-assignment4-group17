# Team 17 - LiveRecall

## Team members

- Ong Li Min / A0309313M / browser client, UI and documentation
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
Open `http://127.0.0.1:8000` in at least two browser windows.

## Public deterministic tests

Run each test at least three times and report the median result.

### T1 - Basic correctness

```bash
python run_demo.py --clients 20 --duration 20 --runs 3
```

### T2 - Public scale
```bash
python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t2
```

### T3 - Slow clients
```bash
python run_demo.py --clients 100 --duration 60 --runs 3 --slow-percent 10 --slow-ms 500 --output-stem t3
```

### T4 - Reconnect
```bash
python run_demo.py --clients 100 --duration 60 --runs 3 --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5 --output-stem t4
```

### T5 - Network impairment
Run the server and clients inside Ubuntu and apply a suitable `tc/netem` profile from `network_profiles/`. Reset the profile after testing.

Record P50/P95/P99 caption age, final-caption completeness, freshness, update coverage, recovery time and slow-client behavior.

## Mandatory deterministic application demo

Start the server and browser client as described above, then run:

```bash
python caption_source.py --duration 120
```

Open at least two browser windows and demonstrate:

- provisional captions being replaced by newer revisions;
- distinct provisional and final caption styles;
- the 30-second default recall view and the 15/30/60-second options;
- temporary disconnection, automatic reconnect and incremental recovery;
- no duplicate finalized captions after recovery;
- late joining with recent context and continued live updates;
- reconnect/late-join notices and recall-panel resizing.

## Design notes

- Caption state is stored by segment and revision. Newer revisions replace older revisions, while stale and duplicate updates are ignored.
- Finalized captions cannot be overwritten by older revisions.
- The server maintains bounded finalized-caption history for recovery and late-join requests.
- Clients send their latest finalized segment and revision during reconnect.
- Each client has independent buffering so slow clients do not block normal clients.
- Obsolete provisional revisions may be coalesced or discarded under pressure, while finalized captions are prioritized.
- The browser stores recent finalized captions locally for recall.
- Each client keeps pending finalized updates separately from the newest active provisional update.
- The browser stores one DOM element per segment so revisions update existing rows instead of creating duplicates.
- The recall panel supports oldest-first reading, newest-caption navigation and a persisted resizable width.
- Connection notices report reconnecting, recovered captions and late-join history.

## Public-test results

Add the median results from three independent runs for T1-T5 here, including P50/P95/P99 caption age, freshness, final-caption completeness, normal update coverage, recovery time and slow-client behavior.

## Submission files

The final submission includes `solution_server.py`, `requirements.txt`, `client/`, `README.md`, `AI_USAGE.md`, `CONTRIBUTIONS.md` and `report.pdf`.

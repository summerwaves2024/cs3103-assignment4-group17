# Package Contents

- `solution_server.py` - WebSocket server with revision handling, bounded history, recovery and per-client delivery.
- `caption_source.py` - deterministic revision source; exact-segment and long/demo modes.
- `load_clients.py` - scalable clients with role labels, finalized resume state, and history duplicate/stale checks.
- `run_demo.py` - independent public runs + median summary.
- `analyse_results.py` - correctness/freshness/coverage, slow-reader split, and recovery timing.
- `monitor_server.py` - optional Ubuntu RSS memory observation helper.
- `client/` - browser client with revision replacement, recall, reconnect notices and resizable recall layout.
- `revision_state_manager.py` - tracks accepted revisions and finalized segment state.
- `flow_control.py` - per-client buffering and backpressure logic.
- `tools/slow_proxy.py` - slow-network testing utility.
- `network_profiles/` - public `tc/netem` examples.
- `START_HERE.md`, `DEMO_GUIDE.md`, `PUBLIC_TESTS.md`, `PROTOCOL_EXAMPLES.md`, `NETWORKING_PRIMER.md`, `UBUNTU_SETUP.md` - setup, demo, protocol and testing guides.
- `submission_check.py` - startup/WebSocket compatibility checker; `--final` checks final deliverables.
- `AI_USAGE.md`, `CONTRIBUTIONS.md` and `report.pdf` - required final submission documentation.
- `AI_USAGE_TEMPLATE.md`, `CONTRIBUTIONS_TEMPLATE.md` - templates used to prepare the final documentation.
- `bonus_asr/` - optional-only bonus guidance.

#!/usr/bin/env python3
"""Run T1–T4 three times each and save per-case results and metric medians."""

import argparse
import json
import math
from pathlib import Path
import statistics
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
CASES = {
    'T1': ['--clients', '20', '--duration', '20'],
    'T2': ['--clients', '100', '--duration', '40'],
    'T3': ['--clients', '100', '--duration', '60',
           '--slow-percent', '10', '--slow-ms', '500'],
    'T4': ['--clients', '100', '--duration', '60',
           '--disconnect-percent', '20', '--disconnect-at', '15',
           '--disconnect-seconds', '5'],
}


def median_results(values):
    """Median of finite numeric observations, recursively including role metrics.

    Missing/non-finite observations are excluded; unavailable metrics become null.
    These are medians of run summaries, not percentiles of pooled caption logs.
    """
    dictionaries = [value for value in values if isinstance(value, dict)]
    if dictionaries:
        keys = dict.fromkeys(key for value in dictionaries for key in value)
        return {
            key: median_results([value.get(key) for value in dictionaries])
            for key in keys
        }
    numbers = [value for value in values
               if isinstance(value, (int, float))
               and not isinstance(value, bool) and math.isfinite(value)]
    return statistics.median(numbers) if numbers else None


def parse_args(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--results-dir', type=Path, default=ROOT / 'results')
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--quiet-server', action='store_true')
    return parser.parse_args()


def run_cases(cases, args):
    results_dir = args.results_dir.resolve()
    # Prevent mixing new partial results with summaries from an earlier suite.
    for case in cases:
        folder = results_dir / case
        outputs = [folder / f'{case}_run{run:02d}.{extension}'
                   for run in range(1, 4) for extension in ('json', 'csv')]
        outputs.append(folder / 'median.json')
        if any(path.exists() for path in outputs):
            raise SystemExit(f'Results already exist in {folder}; choose another --results-dir.')

    for case, options in cases.items():
        folder = results_dir / case
        folder.mkdir(parents=True, exist_ok=True)
        command = [sys.executable, str(ROOT / 'run_demo.py'), *options,
                   '--runs', '3', '--port', str(args.port),
                   '--output-stem', str(folder / case)]
        if args.quiet_server:
            command.append('--quiet-server')
        print(f'Running {case}: three independent runs → {folder}', flush=True)
        subprocess.run(command, cwd=ROOT, check=True)
        summaries = [json.loads((folder / f'{case}_run{run:02d}.json').read_text())
                     for run in range(1, 4)]
        (folder / 'median.json').write_text(
            json.dumps(median_results(summaries), indent=2, allow_nan=False) + '\n',
            encoding='utf-8',
        )
        print(f'Saved {folder / "median.json"}', flush=True)


if __name__ == '__main__':
    run_cases(CASES, parse_args(__doc__))

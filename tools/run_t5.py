#!/usr/bin/env python3
"""Run T5 three times after configuring the network profile externally."""

if __package__:
    from .run_t1_t4 import parse_args, run_cases
else:
    from run_t1_t4 import parse_args, run_cases


if __name__ == '__main__':
    args = parse_args(__doc__)
    print('T5 uses the currently configured network profile; it does not apply or reset it.',
          flush=True)
    run_cases({'T5': ['--clients', '100', '--duration', '40']}, args)

# Team Contributions

| Member | Main responsibilities | Experiments/report sections | Agreed contribution % |
|---|---|---|---:|
| Lim Jinle, Alexius | shared flow-control design and integration |  | 25 |
| Ong Li Min | client logic and UI | architectural and protocol, limitations, conclusion and future work | 25 |
| James Chin Jia Jun | revision replacement |  | 25 |
| Tey Kai Jie | reconnection |  | 25 |

Total contribution should be 100%.

Add brief notes on major shared/integration work if useful. Git history is supporting evidence, not a substitute for this statement.

## Shared integration work

Flow-control development was a shared effort across the team. The team
reviewed and integrated several implementations before selecting the final
approach. These included the baseline implementation on `main` and the
experimental versions on `experiment/gradual-reprobing`,
`experiment/combined-flow-control`, `experiment/finals-only-recovery`, and
`alexius/experiment-simplified-flow-control`.

The experiments were compared using the public test cases and then reviewed
for their effect on provisional-caption delivery, finalised-caption
completeness, slow-client isolation, and reconnect recovery. The final
flow-control behaviour and its integration with the server were therefore
treated as team work rather than as the work of one individual.

from enum import Enum

class ClientStatus(Enum):
    # Delivery decisions are made from the client's finalized-caption queue
    # in solution_server.py. NORMAL is retained as a diagnostic label so the
    # existing client logs and ClientState interface remain compatible.
    NORMAL = 1

PROBE_INTERVAL = 0.1   # at most one ping per client per 100 ms
HEALTHY_PROBE_INTERVAL = 0.5  # reduce probe overhead after calibration
LATE_MIN = 0.2         # a pong is never "late" below 200 ms, like TCP_RTO_MIN

class FlowControl:
    def __init__(self):
        self.sent = 0                           # messages sent to this client
        self.srtt : float | None = None         # smoothed RTT of on-time pongs
        self.rttvar = 0.0                       # and its variation
        self.probe_t : float | None = None      # send time of the outstanding ping
        self.last_probe_t = 0.0
        self.draft_hold_until = 0.0              # hysteresis after an overdue probe
        self.healthy_pongs = 0                   # consecutive on-time probes
        self.mode = ClientStatus.NORMAL         # state when last checked (for logs)

    def draft_delay(self, now : float) -> float | None:
        """Hold drafts while an outstanding probe is clearly overdue.

        The finalized-caption queue remains the primary signal. This small
        diagnostic fallback also catches clients whose socket buffer accepts
        writes even though the browser is reading them slowly.
        """
        if (self.probe_t is not None
                and now - self.probe_t > self._late_after()):
            return None
        if now < self.draft_hold_until:
            return None
        return 0.0

    def should_probe(self, now : float) -> bool:
        interval = (
            PROBE_INTERVAL
            if self.healthy_pongs < 3 or now < self.draft_hold_until
            else HEALTHY_PROBE_INTERVAL
        )
        return self.probe_t is None and now - self.last_probe_t >= interval

    def on_send(self) -> None:
        self.sent += 1

    def on_probe(self, now : float) -> None:
        self.probe_t = now
        self.last_probe_t = now

    def on_pong(self, now : float) -> None:
        if self.probe_t is None:
            return
        rtt = now - self.probe_t
        late = rtt > self._late_after()
        self.probe_t = None

        if not late:
            self.healthy_pongs += 1
            # Based on RFC 6298 retransmission timer
            if self.srtt is None:
                self.srtt, self.rttvar = rtt, rtt / 2
            else:
                self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - rtt)
                self.srtt = 0.875 * self.srtt + 0.125 * rtt
            # Do not resume drafts after a single lucky fast pong.  Requiring
            # a few healthy samples avoids oscillating between drafts and
            # finals-only behavior when the client is near its limit.
            if self.healthy_pongs >= 3:
                self.draft_hold_until = 0.0
            return

        self.healthy_pongs = 0
        # Keep drafts held briefly after a late pong.  The final queue remains
        # the primary backlog signal; this is only hysteresis for clients whose
        # socket accepts writes before the browser has consumed them.
        self.draft_hold_until = max(self.draft_hold_until, now + 1.0)

    def _late_after(self) -> float:
        """RTT above which a pong counts as late: TCP's RTO rule."""
        if self.srtt is None:
            return LATE_MIN
        return max(self.srtt + 4 * self.rttvar, LATE_MIN)

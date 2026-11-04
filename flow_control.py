from enum import Enum  # Provides named flow-control states.

class ClientStatus(Enum):
    # Normal delivery sends both final captions and drafts.
    NORMAL = 1       # every revision is sent
    BEHIND = 2       # a ping is overdue right now: drafts held
    FINALS_ONLY = 3  # Sustained lag holds drafts until the retry period ends.

PROBE_INTERVAL = 0.1   # at most one ping per client per 100 ms
LATE_MIN = 1.0         # Lower bound for a late pong threshold.
                       # Short browser or network hiccups stay below it.
LATE_MAX = 3.0         # Upper bound for a late pong threshold.
LAG_MIN = 2.0          # two late pongs 2-20 s apart = sustained lag -> finals only
LAG_MAX = 20.0
FINALS_ONLY_FOR = 60.0 # finals only for this long before trying drafts again

class FlowControl:
    def __init__(self):
        # Keep an RTT estimate from successful probes.
        self.srtt : float | None = None         # smoothed RTT of on-time pongs
        self.rttvar = 0.0                       # and its variation
        # Track the outstanding probe and the last probe time.
        self.probe_t : float | None = None      # send time of the outstanding ping
        self.last_probe_t = 0.0
        # Track repeated delayed responses.
        self.last_late_t : float | None = None  # time of the last late pong
        # Hold drafts until the temporary finals-only period ends.
        self.finals_only_until = 0.0            # drafts held until this time
        # Store the last reported state for transition logging.
        self.mode = ClientStatus.NORMAL         # state when last checked (for logs)

    def state(self, now : float) -> ClientStatus:
        """Which flow-control state the client is in right now."""
        # An unanswered probe means the client may be falling behind.
        if self.probe_t is not None and now - self.probe_t > self._late_after():
            return ClientStatus.BEHIND
        if now < self.finals_only_until:
            return ClientStatus.FINALS_ONLY
        return ClientStatus.NORMAL

    def update_mode(self, now : float) -> ClientStatus | None:
        """Refresh self.mode; return the previous state if it changed."""
        # Return a value only when the state changed.
        old, self.mode = self.mode, self.state(now)
        return old if old is not self.mode else None

    def draft_delay(self, now : float) -> float | None:
        """Seconds to wait before a draft may be sent: 0 = send now,
        None = wait until something wakes the sender (here, the late pong)."""
        state = self.state(now)
        if state is ClientStatus.BEHIND:
            return None
        if state is ClientStatus.FINALS_ONLY:
            return self.finals_only_until - now
        return 0.0

    def should_probe(self, now : float) -> bool:
        # Allow only one outstanding probe and respect the probe interval.
        return (self.probe_t is None and now - self.last_probe_t >= PROBE_INTERVAL
                and now >= self.finals_only_until)

    def on_probe(self, now : float) -> None:
        # Record when the probe was sent so its RTT can be measured later.
        self.probe_t = now
        self.last_probe_t = now

    def on_pong(self, now : float) -> None:
        # Ignore a pong that has no matching outstanding probe.
        if self.probe_t is None:
            return
        rtt = now - self.probe_t
        # Compare the measured RTT with the adaptive late-response threshold.
        late = rtt > self._late_after()
        self.probe_t = None

        if not late:
            # Use an RTT/RTO-style threshold so an isolated network spike does
            # not immediately classify the client as persistently slow.
            if self.srtt is None:
                self.srtt, self.rttvar = rtt, rtt / 2
            else:
                self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - rtt)
                self.srtt = 0.875 * self.srtt + 0.125 * rtt
            return

        # A second late pong within the configured interval indicates sustained
        # lag rather than a one-off spike.
        # Temporarily enter finals-only mode.
        sustained = self.last_late_t is not None and LAG_MIN <= now - self.last_late_t <= LAG_MAX
        if sustained:
            self.finals_only_until = now + FINALS_ONLY_FOR
        self.last_late_t = now

    def _late_after(self) -> float:
        """Return the RTT threshold above which a pong is considered late."""
        if self.srtt is None:
            # Use the conservative initial threshold before calibration.
            return LATE_MIN
        # Add variation headroom so normal jitter is not treated as sustained lag.
        return min(max(self.srtt + 4 * self.rttvar, LATE_MIN), LATE_MAX)
        # Final captions are never delayed by this policy.

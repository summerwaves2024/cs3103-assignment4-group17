"""Decide when to hold caption drafts and monitor each client's connection.

All durations are in seconds. Times come from monotonic(), a clock that is
unaffected by changes to the system date. RTT means ping round-trip time;
baseline means the estimated normal RTT; jitter means normal RTT variation.
"""

import asyncio
from time import monotonic
import websockets

from collections import deque
from math import ceil, isfinite
from statistics import median
from delivery_policy import DeliveryPolicy, NoFlowControl, empty_finals

from enum import Enum

class FlowControlManager:
    """Construct independent client policies without shared rate estimates."""

    def __init__(self, enabled: bool = True):
        self.enabled = enabled  # False selects delivery without custom probes.

    def record_update(self, now: float | None = None, final: bool = False) -> None:
        """Accept the common manager call; this version needs no shared rate."""
        # No shared measurements; per-client notifications go to the policy.
        pass

    def create_policy(self, ws, wake: asyncio.Event, client_id,
                      get_finals=empty_finals, get_send_started=None) -> DeliveryPolicy:
        """Create a separate policy for each client; callbacks read live state."""
        if not self.enabled:
            return NoFlowControl()
        return PingFlowControl(ws, wake, client_id, get_finals, get_send_started)

class ClientStatus(Enum):
    NORMAL = 1       # Permit drafts as well as finals.
    BEHIND = 2       # Hold drafts after sustained delay; check for recovery.
    FINALS_ONLY = 3  # Keep drafts held until recovery and the retry deadline.
    REPROBING = 4    # Resume drafts temporarily to check delivery under load.

# Experimental timings; validate them against caption freshness and coverage.
PROBE_INTERVAL = 0.1  # Minimum time between the starts of custom pings.
PROBE_CADENCE = 0.5   # Monitor waits this long after processing a pong to retry.
DELAY_AFTER = 2.0     # Delay must persist this long; also the delivery-age limit.
RECOVERY_AFTER = 3.0  # Healthy completed responses must span this long.
TRIAL_AFTER = 5.0     # Required healthy-response span and active traffic time.
INITIAL_RETRY = 5.0   # Initial wait before trying drafts from FINALS_ONLY.
MAX_RETRY = 30.0      # Maximum wait after repeatedly unsuccessful trials.

class FlowControl:
    """Store measurements and choose a mode without sending or awaiting data."""

    def __init__(self):
        # The first eight valid samples establish these estimates. This version
        # keeps them fixed afterward; later samples remain available for inspection.
        self.rtt_samples = deque(maxlen=32)  # Most recent completed RTT values.
        self.baseline = None  # Estimated normal RTT, or None while learning.
        self.jitter = None  # Typical distance of initial RTTs from their median.

        # Current probe
        self.probe_started_t: float | None = None  # Current ping operation's start.
        self.probe_threshold = None  # RTT limit saved at this ping's start.
        self.last_probe_t: float | None = None  # Most recent ping start, for spacing.

        # Completed observations
        self.last_rtt = None  # Latest library-measured RTT duration.
        self.last_rtt_t = None  # When the monitor processed that result.
        self.late_since = None  # First late completion since the last healthy one.
        self.last_late_t = None  # Latest late completion in that sequence.
        self.late_count = 0  # Consecutive completed responses above their limits.
        self.healthy_since = None  # First healthy response in the current sequence.
        self.healthy_count = 0  # Healthy completions since the sequence was reset.
        self.last_rtt_late = None  # None until a post-calibration result is judged.

        # Mode and recovery
        self.mode = ClientStatus.NORMAL  # Current permission to send drafts.
        self.mode_since = None  # When the latest mode change occurred.
        self.retry_t = None  # Earliest time FINALS_ONLY can try drafts again.
        self.retry_delay = INITIAL_RETRY  # Wait to use when setting that deadline.
        self.delivery_pressure = False  # Remember detected waiting/blocked delivery.
        self.trial_updates = 0  # Accepted source updates during REPROBING.
        self.trial_has_draft = False  # Whether any of those updates was a draft.
        self.trial_active_time = 0.0  # Time accumulated between source updates.
        self.trial_last_update = None  # Latest source-update time during the trial.

    def draft_delay(self) -> float | None:
        """Allow drafts now, or hold them until the monitor wakes the sender."""
        if self.mode in (
            ClientStatus.NORMAL,
            ClientStatus.REPROBING,
        ):
            return 0.0 # Send Drafts

        return None # Send only Finals

    def should_probe(self, now : float) -> bool:
        """Permit only one outstanding ping and enforce minimum ping spacing."""
        return self.probe_started_t is None and (
            self.last_probe_t is None
            or now - self.last_probe_t >= PROBE_INTERVAL
        )

    def on_probe(self, now : float) -> None:
        """Record a ping start and keep its late threshold fixed until completion."""
        self.probe_started_t = now
        self.last_probe_t = now

        if self.baseline is None:
            # No learned normal RTT yet, so do not judge this ping as late.
            self.probe_threshold = None
        else:
            self.probe_threshold = self.baseline + max(
                0.150,                  # At least 150ms of allowance
                0.5 * self.baseline,    # More allowance for higher-latency spikes
                4 * self.jitter,        # Allow for normal RTT variation
            )

    def on_rtt(self, now : float, rtt : float) -> None:
        """Learn initial RTT or record a healthy/late result; do not change mode."""

        # 1. Validate and record the RTT sample.
        if not isfinite(rtt) or rtt <= 0:
            raise ValueError("RTT must be positive and finite")

        threshold = self.probe_threshold  # Use the limit saved for this ping.

        # A new response after a long observation gap cannot extend an old
        # healthy streak. In particular, monitor stalls must not prove recovery.
        if (threshold is not None and self.last_rtt_t is not None
                and now - self.last_rtt_t > max(1.0, threshold + PROBE_CADENCE)):
            self._clear_healthy()

        self.probe_started_t = None
        self.probe_threshold = None
        self.last_rtt = rtt
        self.last_rtt_t = now
        self.rtt_samples.append(rtt)

        # Learn normal delay without rejecting higher RTTs.
        if self.baseline is None:
            if len(self.rtt_samples) >= 8:
                samples = sorted(self.rtt_samples)  # Low RTTs first.

                # Lower quartile: choose the value a quarter of the way through
                # the sorted samples, reducing the influence of high spikes.
                index = ceil(0.25 * len(samples)) - 1
                self.baseline = samples[index]

                # Median absolute deviation: find each sample's distance from
                # the middle RTT, then take the middle of those distances.
                middle = median(samples)
                self.jitter = median(
                    abs(sample - middle) for sample in samples
                )

            # Initial learning samples do not count toward sustained late delay.
            return

        # A probe launched before calibration had no threshold.
        if threshold is None:
            return

        self.last_rtt_late = rtt > threshold

        # Each result extends one sequence and clears the opposite sequence.
        if rtt > threshold:
            if self.late_since is None:
                self.late_since = now

            self.last_late_t = now
            self.late_count += 1

            self.healthy_since = None
            self.healthy_count = 0

        else:
            self.late_since = None
            self.last_late_t = None
            self.late_count = 0

            if self.healthy_since is None:
                self.healthy_since = now

            self.healthy_count += 1
        # tick() evaluates these observations together with queue waiting times.


    def record_update(self, now: float, final: bool) -> None:
        """Count accepted source updates while testing resumed draft delivery."""
        if self.mode is not ClientStatus.REPROBING:
            return
        if self.trial_last_update is not None:
            gap = now - self.trial_last_update  # Time between source updates.
            # Count at most one second per gap so a long idle period contributes
            # only limited time, rather than making an idle trial look successful.
            self.trial_active_time += min(max(gap, 0.0), 1.0)
            if gap > 2.0:
                # Require new healthy observations after a source interruption.
                self._clear_healthy()
        self.trial_last_update = now
        self.trial_updates += 1
        self.trial_has_draft |= not final

    def _clear_healthy(self) -> None:
        """Forget the healthy response sequence so recovery needs new samples."""
        self.healthy_since = None
        self.healthy_count = 0

    def tick(self, now: float, pending_count: int, oldest_final_age: float,
             current_send_duration: float = 0.0) -> bool:
        """Calculate conditions, then return whether _advance_mode changes mode.

        pending_count includes queued finals and the final currently in send().
        oldest_final_age is the longest wait since this client's final enqueue.
        current_send_duration measures the current history, draft, or final send;
        zero means no send is active. All durations are in seconds.
        Hard limits and connection closure are handled outside this method.
        """
        # Require multiple late completions spanning two seconds. Repeated tick
        # calls cannot turn two closely spaced old results into sustained delay.
        completed_delay = (
            self.late_count >= 2
            # There must be a delay between the oldest late probe and the most recent late probe
            and self.last_late_t - self.late_since >= DELAY_AFTER
        )
        overdue_for = 0.0  # Seconds beyond the outstanding ping's saved limit.
        if self.probe_started_t is not None and self.probe_threshold is not None:
            overdue_for = max(
                0.0, now - self.probe_started_t - self.probe_threshold
            )

        # Detect an old final or a long send, then remember that problem until
        # all finals drain and no send is active. One sent final is not enough.
        if oldest_final_age >= DELAY_AFTER or current_send_duration >= DELAY_AFTER:
            self.delivery_pressure = True
        elif pending_count == 0 and current_send_duration == 0.0:
            self.delivery_pressure = False
            
        # Healthy samples collected before current trouble cannot prove recovery.
        if self.delivery_pressure or overdue_for > 0.0:
            self._clear_healthy()

        # troubled means enough sustained delay or waiting work to hold drafts.
        troubled = (
            completed_delay or overdue_for >= DELAY_AFTER or self.delivery_pressure
        )
        # A late result or overdue ping is current delay, even before it has
        # lasted long enough to establish sustained trouble.
        delay_present = self.last_rtt_late is True or overdue_for > 0.0

        # Do not use an old healthy response to declare recovery now. Allow at
        # least one second of sample age, or longer for a higher-latency path.
        fresh = (
            self.last_rtt_t is not None
            and self.baseline is not None
            and now - self.last_rtt_t <= max(
                1.0, self.baseline + max(0.150, 0.5 * self.baseline, 4 * self.jitter)
                + PROBE_CADENCE
            )
        )
        healthy = (
            # A recent healthy result is useful only without unresolved pings,
            # remembered delivery problems, or an active send.
            fresh and self.last_rtt_late is False
            and self.probe_started_t is None
            and not self.delivery_pressure and current_send_duration == 0.0
            and self.healthy_since is not None
        )
        recovered = (
            # Count actual completions and their time span, not time spent ticking.
            healthy and self.healthy_count >= 3
            and self.last_rtt_t - self.healthy_since >= RECOVERY_AFTER
        )
        trial_complete = (
            # Test full delivery with healthy pongs AND actual source traffic.
            # This is evidence during a finite trial, not proof of client capacity.
            healthy and self.mode is ClientStatus.REPROBING
            and self.healthy_count >= 5
            and self.last_rtt_t - self.healthy_since >= TRIAL_AFTER
            and self.trial_active_time >= TRIAL_AFTER
            and self.trial_updates >= 10 and self.trial_has_draft
            and self.trial_last_update is not None
            and now - self.trial_last_update <= 2.0
        )
        return self._advance_mode(
            now, troubled, delay_present, recovered, trial_complete
        )

    def _advance_mode(self, now: float, troubled: bool, delay_present: bool,
                      recovered: bool, trial_complete: bool) -> bool:
        """Apply at most one mode change using tick's calculated conditions.

        troubled requests restriction; delay_present prevents premature recovery;
        recovered permits a draft trial; trial_complete permits normal delivery.
        """
        old = self.mode  # Keep the original mode to report whether it changed.
        if old is ClientStatus.NORMAL:
            if troubled:
                self.mode = ClientStatus.BEHIND

        elif old is ClientStatus.BEHIND:
            # Continuing trouble gets a longer restriction. If healthy responses
            # are accumulating instead, let the three-second recovery finish.
            if troubled or delay_present:
                if now - self.mode_since >= DELAY_AFTER:
                    self.mode = ClientStatus.FINALS_ONLY
                    self.retry_t = now + self.retry_delay
            elif recovered:
                self.mode = ClientStatus.REPROBING

        elif old is ClientStatus.FINALS_ONLY:
            # The retry deadline alone cannot resume drafts without recovery.
            if not troubled and recovered and now >= self.retry_t:
                self.mode = ClientStatus.REPROBING
                
        elif old is ClientStatus.REPROBING:
            if troubled:
                # A failed trial increases the next wait, up to the configured cap.
                self.mode = ClientStatus.FINALS_ONLY
                self.retry_delay = min(MAX_RETRY, 2 * self.retry_delay)
                self.retry_t = now + self.retry_delay
            elif not delay_present and trial_complete:
                # Successful full delivery resets the increasing retry wait.
                self.mode = ClientStatus.NORMAL
                self.retry_delay = INITIAL_RETRY
                self.retry_t = None

        if self.mode is old:
            return False
        self.mode_since = now
        # Measurements from the previous mode must not count as a successful
        # trial under the new mode's different amount of caption traffic.
        self._clear_healthy()
        if self.mode is ClientStatus.REPROBING:
            self.trial_updates = 0
            self.trial_has_draft = False
            self.trial_active_time = 0.0
            self.trial_last_update = None
        return True

class PingFlowControl:
    """Run network operations and monitoring for one client's FlowControl."""

    def __init__(self, ws, wake: asyncio.Event, client_id, get_finals=empty_finals,
                 get_send_started=None):
        self.ws = ws  # This client's WebSocket connection.
        self.wake = wake  # Event that tells the sender to recheck draft permission.
        self.client_id = client_id  # Used to identify this client in logs.
        self.get_finals = get_finals  # Returns current (pending finals, sending final).
        self.get_send_started = get_send_started  # Returns send start time or None.
        self.controller = FlowControl()  # Measurements and mode decisions.
        self._closed = False  # Prevent more monitoring or startup after shutdown.
        self._monitor_task = None  # Background asyncio task owned by this policy.

    @property
    def status(self) -> str:
        """Expose the current mode name through the shared policy interface."""
        return self.controller.mode.name

    def draft_delay(self) -> float | None:
        """Let the sender ask the controller whether drafts are permitted."""
        return self.controller.draft_delay()

    async def after_send(self) -> None:
        """Keep the shared sender hook; this version probes independently."""
        # Probing belongs to the monitor, not the caption sender.
        pass

    def record_update(self, now: float, final: bool) -> None:
        """Forward source activity even when this client did not receive it."""
        self.controller.record_update(now, final)

    def start(self) -> None:
        """Schedule one monitor after the client state is fully constructed."""
        if not self._closed and self._monitor_task is None:
            self._monitor_task = asyncio.create_task(self.client_monitor())

    def close(self) -> None:
        """Request cancellation; aclose() also waits for cancellation to finish."""
        self._closed = True
        if self._monitor_task is not None:
            self._monitor_task.cancel()

    async def aclose(self) -> None:
        """Stop the monitor and wait for its outstanding ping-task cleanup."""
        self.close()
        if self._monitor_task is not None:
            await asyncio.gather(self._monitor_task, return_exceptions=True)

    async def measure_ping(self):
        """Return library-measured RTT, without timing the earlier caption send.

        Buffered data can still increase RTT. A pong does not prove that the
        browser applied a caption. Sending the ping itself can also block.
        """
        pong_future = await self.ws.ping()
        # Shield prevents cancellation of this task from cancelling
        # the Future owned by the WebSocket library.
        # Consume errors even if this task is cancelled during shutdown.
        pong_future.add_done_callback(
            lambda future: None if future.cancelled() else future.exception()
        )
        return await asyncio.shield(pong_future)

    async def client_monitor(self):
        """Check delivery and ping deadlines while the caption sender can wait."""
        probe_task = None  # Separate task sends a ping and awaits its result.
        probe_started = None  # Includes time waiting for ping transmission.
        next_probe = monotonic()  # Earliest scheduled start of another ping.

        try:
            while not self._closed:
                now = monotonic()  # One consistent time for this iteration's checks.

                # Read a completed result before evaluating deadlines.
                if probe_task is not None and probe_task.done():
                    rtt = probe_task.result()  # Duration in seconds; may raise ConnectionClosed.
                    self.controller.on_rtt(now, rtt)
                    probe_task = None
                    probe_started = None
                    next_probe = now + PROBE_CADENCE

                # Close after 20 seconds without completion, including a blocked
                # ping send. This also applies before baseline learning finishes.
                if (
                    probe_started is not None
                    and now - probe_started >= 20.0
                ):
                    await self.ws.close(
                        code=1013,
                        reason="Probe did not complete",
                    )
                    return

                pending, inflight = self.get_finals()
                # inflight is the final currently inside ws.send(), not a final
                # waiting for application acknowledgement from the client.
                # With one sender and a FIFO queue, the sending final is oldest.
                if inflight is not None:
                    oldest = inflight
                elif pending:
                    oldest = pending[0]
                else:
                    oldest = None

                oldest_final_age = (
                    max(0.0, now - oldest.queue_ts)
                    if oldest is not None
                    else 0.0
                )
                send_started = (
                    self.get_send_started() if self.get_send_started else None
                )
                old = self.controller.mode
                # Include the sending final even though it has left the deque.
                changed = self.controller.tick(
                    now,
                    pending_count=len(pending) + int(inflight is not None),
                    oldest_final_age=oldest_final_age,
                    current_send_duration=(
                        max(0.0, now - send_started)
                        if send_started is not None else 0.0
                    ),
                )
                if changed:
                    print(f"client {self.client_id}: {old.name} -> {self.status}",
                          flush=True)
                    self.wake.set()  # Sender rechecks draft permission.

                if (probe_task is None and now >= next_probe
                        and self.controller.should_probe(now)):
                    # Store the start before creating the task. Do not await the
                    # ping here: the monitor must keep checking other deadlines.
                    probe_started = now
                    self.controller.on_probe(now)
                    probe_task = asyncio.create_task(
                        self.measure_ping()
                    )

                # Yield to other tasks; check again within about 50 ms.
                await asyncio.sleep(0.05)

        except websockets.ConnectionClosed:
            return
        finally:
            # Await cancellation so the child task cannot outlive this monitor.
            if probe_task is not None:
                probe_task.cancel()
                await asyncio.gather(
                    probe_task, return_exceptions=True
                )

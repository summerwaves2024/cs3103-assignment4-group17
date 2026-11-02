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

# Technically this class is not needed but its here so that we can easily swap out
# the old flow control code with the new one if necessary
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
    FINALS_ONLY = 3  # Hold drafts until recovery and the draft-retry wait has elapsed.
    REPROBING = 4    # Resume drafts temporarily to check delivery under load.

# Tuning parameters: all durations are seconds. Defaults are experimental;
# compare caption freshness, final coverage, and state changes when tuning.

# RTT calibration. Estimates remain fixed after this initial sample set.
RTT_CALIBRATION_SAMPLES = 8  # More samples delay calibration; fewer are noisier.
RTT_SAMPLE_HISTORY = 32     # Inspection buffer; keep >= RTT_CALIBRATION_SAMPLES.
RTT_BASELINE_QUANTILE = 0.25  # Must be in (0, 1]; lower selects a lower baseline.

# RTT sensitivity: smaller allowances classify more probes as late.
RTT_MIN_ALLOWANCE = 0.150      # Floor for tolerance of transient delay.
RTT_BASELINE_ALLOWANCE = 0.5   # Multiplier applied to baseline RTT.
RTT_JITTER_ALLOWANCE = 4.0     # Multiplier applied to calibration jitter.

# Draft withholding: lower times react sooner, but tolerate fewer brief spikes.
LATE_PROBES_REQUIRED = 2       # Minimum consecutive late completed probes.
LATE_PROBE_SPAN = 2.0          # Required time between first/latest late completions.
PROBE_OVERDUE_AFTER = 2.0      # Time beyond a pending ping's RTT threshold.
FINAL_AGE_LIMIT = 2.0          # Oldest final's time since per-client enqueue.
SEND_DURATION_LIMIT = 2.0      # Maximum send duration before delivery pressure.
BEHIND_ESCALATE_AFTER = 2.0    # Time in BEHIND before ongoing delay escalates.

# Probe scheduling and observation freshness.
PROBE_INTERVAL = 0.1          # Minimum spacing between custom ping starts.
PROBE_CADENCE = 0.5           # Wait after processing a pong before another ping.
MONITOR_INTERVAL = 0.05       # Check frequency; smaller values increase CPU work.
PROBE_TIMEOUT = 20.0          # Close if a probe doesn't complete within this time.
RTT_FRESHNESS_MIN = 1.0       # Freshness limit is max(this, RTT limit + cadence).

# Recovery: smaller requirements resume drafts sooner, with less evidence.
RECOVERY_AFTER = 3.0          # Required span of healthy completed responses.
RECOVERY_PROBES_REQUIRED = 3

# Trial is another name for Reprobe, reprobe probe sounds weird
TRIAL_AFTER = 5.0             # Required healthy-response span during a draft trial.
TRIAL_PROBES_REQUIRED = 5
TRIAL_SENDS_REQUIRED = 10     # Completed caption sends; finals and drafts both count.

# Retry backoff: smaller waits retry drafts sooner after restriction/failure.
INITIAL_RETRY = 5.0
MAX_RETRY = 30.0
RETRY_BACKOFF_MULTIPLIER = 2.0

class FlowControl:
    """Store measurements and choose a mode without sending or awaiting data."""

    def __init__(self):
        # The initial calibration samples establish these estimates. This version
        # keeps them fixed afterward; later samples remain available for inspection.
        self.rtt_samples = deque(maxlen=RTT_SAMPLE_HISTORY)  # Recent completed RTTs.
        self.baseline = None  # Estimated normal RTT, or None while learning.
        self.jitter = None  # Typical distance of initial RTTs from their median.
        self.rtt_threshold = None  # Learned RTT limit, or None during calibration.

        # Current probe
        self.probe_started_t: float | None = None  # Current ping operation's start.
        self.probe_threshold = None  # RTT limit saved at this ping's start.
        self.last_probe_t: float | None = None  # Most recent ping start, for spacing.

        # Completed observations
        self.last_rtt = None  # Latest library-measured RTT duration.
        self.last_rtt_t = None  # When the monitor processed that result.
        self.late_since = None  # First late completion since the last healthy one.
        self.last_late_t = None  # Oldest late completion in a sequence of lates.
        self.late_count = 0  # Consecutive pongs which were outdated.

        self.healthy_since = None  # First healthy response in the current sequence of healthys.
        self.healthy_count = 0  # Healthy completions since the sequence was reset.
        self.last_rtt_late = None  # None until a post-calibration result is judged.

        # Mode and recovery
        self.mode = ClientStatus.NORMAL  # Current permission to send drafts.
        self.mode_since = None  # When the latest mode change occurred.
        self.retry_t = None  # Earliest time FINALS_ONLY can try drafts again.
        self.retry_delay = INITIAL_RETRY  # Seconds to wait before another draft trial.
        self.delivery_pressure = False  # Remember detected waiting/blocked delivery.
        self.trial_sends = 0  # Completed caption sends during REPROBING.
        self.trial_traffic_ready_t = None  # When sufficient trial traffic was first sent.

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

        # None during calibration; otherwise snapshot the learned RTT limit.
        self.probe_threshold = self.rtt_threshold

    # Use RTT value to calculate 
    def on_rtt(self, now : float, rtt : float) -> None:
        """Learn initial RTT or record a healthy/late result; do not change mode."""

        # 1. Validate and record the RTT sample.
        # A fast localhost pong can round to zero on coarse-resolution clocks.
        if not isfinite(rtt) or rtt < 0:
            raise ValueError("RTT must be nonnegative and finite")

        threshold = self.probe_threshold  # Use the limit saved for this ping.

        # A new response after a long observation gap cannot extend an old
        # healthy streak. In particular, monitor stalls must not prove recovery.
        if (threshold is not None and self.last_rtt_t is not None
                and now - self.last_rtt_t > max(RTT_FRESHNESS_MIN, threshold + PROBE_CADENCE)):
            self._clear_healthy()

        self.probe_started_t = None
        self.probe_threshold = None
        self.last_rtt = rtt
        self.last_rtt_t = now
        self.rtt_samples.append(rtt)

        # Learn normal delay without rejecting higher RTTs.
        # NOTE: this is only done for the initial calibration sample set;
        # if the client starts off lagging, the baseline will remain
        # inflated for the rest of the client lifespan
        if self.baseline is None:
            if len(self.rtt_samples) >= RTT_CALIBRATION_SAMPLES:
                samples = sorted(self.rtt_samples)  # Low RTTs first.

                # Choose a low quantile to reduce the influence of high spikes.
                index = ceil(RTT_BASELINE_QUANTILE * len(samples)) - 1
                self.baseline = samples[index]

                # Median absolute deviation: find each sample's distance from
                # the middle RTT, then take the middle of those distances.
                middle = median(samples)
                self.jitter = median(
                    abs(sample - middle) for sample in samples
                )
                self.rtt_threshold = self.baseline + max(
                    RTT_MIN_ALLOWANCE,
                    RTT_BASELINE_ALLOWANCE * self.baseline,
                    RTT_JITTER_ALLOWANCE * self.jitter,
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
        # tick() from monitor will use these measurements together with queue waiting times.

    # Check and measure if we can transition from REPROBE to NROMAL
    def record_send(self, now: float) -> None:
        """Count completed caption sends while testing resumed delivery.

        A completed send isn't an application acknowledgement. Healthy pongs
        and the delivery-pressure checks must also support trial success.
        """
        if self.mode is not ClientStatus.REPROBING:
            return
        self.trial_sends += 1
        # Keep the first readiness time: sparse or continuous later sends must
        # not restart the healthy-probe confirmation requirement.
        if (self.trial_traffic_ready_t is None
                and self.trial_sends >= TRIAL_SENDS_REQUIRED):
            self.trial_traffic_ready_t = now

    def _clear_healthy(self) -> None:
        """Forget the healthy response sequence so recovery needs new samples."""
        self.healthy_since = None
        self.healthy_count = 0

    # Takes measurements from on_RTT, then performs more calculations to gauge connection state
    # using those calculations assign a state to the client
    def tick(self, now: float, pending_count: int, oldest_final_age: float,
             current_send_duration: float = 0.0) -> bool:
        """Calculate conditions, then return whether _advance_mode changes mode.

        pending_count includes queued finals and the final currently in send().
        oldest_final_age is the longest wait since this client's final enqueue.
        current_send_duration measures the current history, draft, or final send;
        zero means no send is active. All durations are in seconds.
        Hard limits and connection closure are handled outside this method.
        """

        # 1. SUSTAINED DELAY
        # Require consecutive late completions spanning LATE_PROBE_SPAN. Repeated tick
        # calls cannot turn two closely spaced old results into sustained delay.
        completed_delay = (
            self.late_count >= LATE_PROBES_REQUIRED
            # There must be a delay between the oldest late probe and the most recent late probe
            and self.last_late_t - self.late_since >= LATE_PROBE_SPAN
        )

        # 2. OVERDUE FOR
        # How long has the current ping waited for its pong (past its expected time, i.e. the threshold) 
        overdue_for = 0.0  # Seconds beyond the outstanding ping's saved limit.
        if self.probe_started_t is not None and self.probe_threshold is not None:
            overdue_for = max(
                0.0, now - self.probe_started_t - self.probe_threshold
            )

        # 3. DELIVER PRESSURE
        # Detect an old final or a long send, then remember that problem until
        # all finals drain and no send is active. One sent final is not enough.
        if oldest_final_age >= FINAL_AGE_LIMIT or current_send_duration >= SEND_DURATION_LIMIT:
            self.delivery_pressure = True
        elif pending_count == 0 and current_send_duration == 0.0:
            self.delivery_pressure = False

        # If we have evidence of a slowing connection, reset the "healthy sequence"
        # Healthy samples collected before current trouble cannot prove recovery.
        if self.delivery_pressure or overdue_for > 0.0:
            self._clear_healthy()

        # troubled means enough sustained delay or waiting work to hold drafts.
        troubled = (
            completed_delay or overdue_for >= PROBE_OVERDUE_AFTER or self.delivery_pressure
        )

        # A late result or overdue ping means delay_present, even before it has
        # lasted long enough to establish sustained trouble.
        delay_present = self.last_rtt_late is True or overdue_for > 0.0

        # Do not use an old healthy response to declare recovery now. Allow at
        # least RTT_FRESHNESS_MIN of sample age, or longer for a higher-latency path.
        fresh = (
            self.last_rtt_t is not None
            and self.rtt_threshold is not None
            and now - self.last_rtt_t <= max(
                RTT_FRESHNESS_MIN, self.rtt_threshold + PROBE_CADENCE
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
            healthy and self.healthy_count >= RECOVERY_PROBES_REQUIRED
            and self.last_rtt_t - self.healthy_since >= RECOVERY_AFTER
        )
        trial_complete = (
            # Test resumed delivery with healthy pongs AND completed caption sends.
            # This is evidence during a finite trial, not proof of client capacity.
            healthy and self.mode is ClientStatus.REPROBING
            and self.healthy_count >= TRIAL_PROBES_REQUIRED
            and self.last_rtt_t - self.healthy_since >= TRIAL_AFTER
            and self.trial_sends >= TRIAL_SENDS_REQUIRED
            # Require a healthy pong from a ping started after sufficient trial
            # traffic was sent. Later sends needn't stop for the trial to pass,
            # and no new sends are required once that traffic has drained.
            and self.trial_traffic_ready_t is not None
            and self.last_probe_t is not None
            and self.last_probe_t >= self.trial_traffic_ready_t
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
            # are accumulating instead, let the healthy recovery span finish.
            if troubled or delay_present:
                if now - self.mode_since >= BEHIND_ESCALATE_AFTER:
                    self.mode = ClientStatus.FINALS_ONLY
                    self.retry_t = now + self.retry_delay
            elif recovered:
                self.mode = ClientStatus.REPROBING

        elif old is ClientStatus.FINALS_ONLY:
            # Elapsing the draft-retry wait alone cannot resume drafts without recovery.
            if not troubled and recovered and now >= self.retry_t:
                self.mode = ClientStatus.REPROBING

        elif old is ClientStatus.REPROBING:
            if troubled:
                # A failed trial increases the next wait, up to the configured cap.
                self.mode = ClientStatus.FINALS_ONLY
                self.retry_delay = min(MAX_RETRY, RETRY_BACKOFF_MULTIPLIER * self.retry_delay)
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
            self.trial_sends = 0
            self.trial_traffic_ready_t = None
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
        """Record a completed caption send; probing remains in the monitor."""
        self.controller.record_send(monotonic())

    def record_update(self, now: float, final: bool) -> None:
        """Keep interface compatibility; incoming updates don't validate a trial."""
        pass

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
        """Return library-measured gauge of RTT, without including any 
        caption send or ping-call setup time (which would occur if time b4 ws.ping).

        Buffered data can still increase RTT. A pong does not prove that the
        browser applied a caption. Sending the ping itself can also block, inflating RTT.
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
        """Monitor delivery pressure and ping timing concurrently with the sender."""
        probe_task = None  # Task sends a ping and awaits its result.
        probe_started = None  # Includes time waiting for ping transmission.
        next_probe = monotonic()  # Earliest scheduled start of another ping.

        try:
            while not self._closed:
                now = monotonic()  # One consistent time for this iteration's checks.

                # Read a completed result before checking the probe completion timeout.
                if probe_task is not None and probe_task.done():
                    rtt = probe_task.result()  # Duration in seconds; may raise ConnectionClosed.

                    self.controller.on_rtt(now, rtt)
                    probe_task = None
                    probe_started = None
                    next_probe = now + PROBE_CADENCE

                # Close websocket after PROBE_TIMEOUT without completion, including a blocked
                # ping send. This also applies before baseline learning finishes.
                # Forces reconnection
                if (
                    probe_started is not None
                    and now - probe_started >= PROBE_TIMEOUT
                ):
                    await self.ws.close(
                        code=1013,
                        reason="Probe did not complete",
                    )
                    return

                pending, inflight = self.get_finals()
                # inflight is the final currently inside ws.send(), not a final
                # waiting for application acknowledgement from the client.
                # With one sender and a FIFO queue, the "nearest to be sent" final is oldest.
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

                # Send out probe, asynchronously
                if (probe_task is None and now >= next_probe
                        and self.controller.should_probe(now)):
                    # Store the start before creating the task. Do not await the
                    # ping here: the monitor must keep checking final age,
                    # send duration, overdue pings, and draft-retry eligibility.
                    probe_started = now
                    self.controller.on_probe(now)
                    probe_task = asyncio.create_task(
                        self.measure_ping()
                    )

                # Yield to other tasks before the next monitoring iteration.
                await asyncio.sleep(MONITOR_INTERVAL)

        except websockets.ConnectionClosed:
            return
        finally:
            # Await cancellation so the child task cannot outlive this monitor.
            if probe_task is not None:
                probe_task.cancel()
                await asyncio.gather(
                    probe_task, return_exceptions=True
                )

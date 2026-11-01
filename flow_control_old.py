"""Delivery policies and ping-based flow control, independent of caption queues."""

import asyncio
from time import monotonic
from collections import deque
from enum import Enum
from typing import Callable
from delivery_policy import DeliveryPolicy, NoFlowControl, empty_finals


class StreamRate:
    def __init__(self, window: float = 10.0):
        self.window = window
        self.updates: deque[float] = deque()

    def rate(self, now: float) -> float:
        while self.updates and self.updates[0] < now - self.window:
            self.updates.popleft()
        return len(self.updates) / self.window

    def record(self, now: float) -> None:
        self.rate(now)
        self.updates.append(now)


class FlowControlManager:
    """Share source-rate measurements and construct independent client policies."""

    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.stream = StreamRate()

    def record_update(self, now: float | None = None, final: bool = False) -> None:
        if self.enabled:
            self.stream.record(monotonic() if now is None else now)

    def create_policy(self, ws, wake: asyncio.Event, client_id,
                      get_finals=empty_finals, get_send_started=None) -> DeliveryPolicy:
        if not self.enabled:
            return NoFlowControl()
        return PingFlowControl(ws, wake, client_id, self.stream.rate,
                               get_finals, get_send_started)

class ClientStatus(Enum):
    NORMAL = 1       # every revision is sent
    BEHIND = 2       # a ping is overdue right now: drafts held
    FINALS_ONLY = 3  # too slow for the full stream: drafts held until a re-probe
    REPROBING = 4    # drafts resumed to test whether the client got faster

PROBE_INTERVAL = 0.1   # at most one ping per client per 100 ms
LATE_MIN = 0.2         # a pong is never "late" below 200 ms, like TCP_RTO_MIN
MEASURE_MIN = 2.0      # R is measured from two late pongs 2-20 s apart
MEASURE_MAX = 20.0
HEADROOM = 1.1         # R must exceed the stream rate by 10% to take it all
REPROBE_AFTER = 60.0   # finals only for this long before trying drafts again
RECOVERED_AFTER = 20.0 # no late pong this long after a re-probe -> recovered

class FlowControl:
    def __init__(self):
        self.sent = 0                           # messages sent to this client
        self.srtt : float | None = None         # smoothed RTT of on-time pongs
        self.rttvar = 0.0                       # and its variation
        self.probe_t : float | None = None      # send time of the outstanding ping
        self.probe_sent = 0                     # self.sent when that ping went out
        self.last_probe_t = 0.0
        self.last_late_t : float | None = None  # time and self.sent of the
        self.last_late_sent = 0                 # last late pong
        self.read_rate : float | None = None    # R, msgs/s the client reads
        self.reprobe_at : float | None = None   # finals only until then (None = not)
        self.mode = ClientStatus.NORMAL         # state when last checked (for logs)

    def state(self, now : float, stream_rate : float) -> ClientStatus:
        """Which flow-control state the client is in right now."""
        if self.probe_t is not None and now - self.probe_t > self._late_after():
            return ClientStatus.BEHIND
        if self.reprobe_at is None or self._can_keep_up(stream_rate):
            return ClientStatus.NORMAL
        if now < self.reprobe_at:
            return ClientStatus.FINALS_ONLY
        return ClientStatus.REPROBING

    def update_mode(self, now : float, stream_rate : float) -> ClientStatus | None:
        """Refresh self.mode; return the previous state if it changed."""
        old, self.mode = self.mode, self.state(now, stream_rate)
        return old if old is not self.mode else None

    def draft_delay(self, now : float, stream_rate : float) -> float | None:
        """Seconds to wait before a draft may be sent: 0 = send now,
        None = wait until something wakes the sender (here, the late pong)."""
        state = self.state(now, stream_rate)
        if state is ClientStatus.BEHIND:
            return None
        if state is ClientStatus.FINALS_ONLY:
            return self.reprobe_at - now
        return 0.0

    def should_probe(self, now : float) -> bool:
        return self.probe_t is None and now - self.last_probe_t >= PROBE_INTERVAL

    def on_send(self) -> None:
        self.sent += 1

    def on_probe(self, now : float) -> None:
        self.probe_t = now
        self.last_probe_t = now
        self.probe_sent = self.sent

    def on_pong(self, now : float, stream_rate : float) -> None:
        if self.probe_t is None:
            return
        rtt = now - self.probe_t
        late = rtt > self._late_after()
        self.probe_t = None

        if not late:
            # Based on RFC 6298 retransmission timer
            if self.srtt is None:
                self.srtt, self.rttvar = rtt, rtt / 2
            else:
                self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - rtt)
                self.srtt = 0.875 * self.srtt + 0.125 * rtt
            # Drafts resumed at a re-probe and it has stayed on time, then we recover
            if (self.reprobe_at is not None and now >= self.reprobe_at
                    and now - self.last_late_t >= RECOVERED_AFTER):
                self.reprobe_at = None
            return

        if self.last_late_t is not None and MEASURE_MIN <= now - self.last_late_t <= MEASURE_MAX:
            self.read_rate = (self.probe_sent - self.last_late_sent) / (now - self.last_late_t)
            finals_only = self.reprobe_at is not None and now < self.reprobe_at
            if not self._can_keep_up(stream_rate) and not finals_only:
                self.reprobe_at = now + REPROBE_AFTER
        self.last_late_t, self.last_late_sent = now, self.probe_sent

    def _can_keep_up(self, stream_rate : float) -> bool:
        return self.read_rate is not None and self.read_rate >= HEADROOM * stream_rate

    def _late_after(self) -> float:
        """RTT above which a pong counts as late: TCP's RTO rule."""
        if self.srtt is None:
            return LATE_MIN
        return max(self.srtt + 4 * self.rttvar, LATE_MIN)


class PingFlowControl:
    """Adapt the state machine to WebSockets, including probing and wakeups."""

    def __init__(self, ws, wake: asyncio.Event, client_id,
                 stream_rate: Callable[[float], float],
                 get_finals=empty_finals, get_send_started=None):
        self.ws = ws
        self.wake = wake
        self.client_id = client_id
        self.stream_rate = stream_rate
        self.controller = FlowControl()
        self._pong: asyncio.Future | None = None
        self._closed = False

    @property
    def status(self) -> str:
        return self.controller.mode.name

    def _log_mode(self, now: float) -> None:
        old = self.controller.update_mode(now, self.stream_rate(now))
        if old is not None:
            print(f"client {self.client_id}: {old.name} -> {self.status}", flush=True)

    def draft_delay(self) -> float | None:
        now = monotonic()
        self._log_mode(now)
        return self.controller.draft_delay(now, self.stream_rate(now))

    async def after_send(self) -> None:
        """Count a completed caption send and optionally launch a legacy probe.

        Keep the same no-argument hook as the newer send-based trial policy;
        both finals and drafts count without extra metadata from the server.
        """
        self.controller.on_send()
        now = monotonic()
        if not self._closed and self.controller.should_probe(now):
            self.controller.on_probe(now)
            self._pong = await self.ws.ping()
            self._pong.add_done_callback(self._on_pong)

    def start(self) -> None:
        # This implementation probes after sends; it has no monitor task.
        pass

    def record_update(self, now: float, final: bool) -> None:
        # Source rate is already recorded once by FlowControlManager.
        pass

    def _on_pong(self, future: asyncio.Future) -> None:
        self._pong = None
        if future.cancelled() or future.exception() is not None or self._closed:
            return
        now = monotonic()
        self.controller.on_pong(now, self.stream_rate(now))
        self._log_mode(now)
        self.wake.set()

    def close(self) -> None:
        # The WebSocket owns its pong future; leave it to finish, consuming any
        # exception in the callback without waking an already disconnected client.
        self._closed = True

    async def aclose(self) -> None:
        self.close()

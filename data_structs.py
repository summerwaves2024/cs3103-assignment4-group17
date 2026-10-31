import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Literal, NotRequired, TypedDict
from delivery_policy import DeliveryPolicy

class SourceHello(TypedDict):
    type : Literal["SOURCE_HELLO"]
    session : str

class ClientHello(TypedDict):
    type : Literal["CLIENT_HELLO"]
    client_id : int
    session : str
    last_segment : int
    last_revision : int
    history_seconds : NotRequired[int]   # server defaults to 30

@dataclass
class Caption:
    session : str
    segment : int
    revision : int
    source_ts : float
    queue_ts : float
    final : bool
    text : str

    @classmethod
    def from_dict(cls, caption_dict : dict) -> "Caption":
        session = caption_dict.get("session")
        segment = caption_dict.get("segment")
        revision = caption_dict.get("revision")
        source_ts = caption_dict.get("source_ts")
        queue_ts = caption_dict.get("queue_ts")
        final = caption_dict.get("final")
        text = caption_dict.get("text")
        return cls(session, segment, revision,
                   source_ts, queue_ts, final, text)


@dataclass
class BaseClientState:
  """Normal caption delivery state, usable without a flow-control component."""
  ws : object # WebSocket Object
  pending_finals : deque[Caption]
  pending_active : Caption | None
  last_final : tuple[int, int]  # (segment, revision)
  inflight_final : Caption | None = None
  send_started_t : float | None = None
  client_id : object = None # from CLIENT_HELLO, for logs
  wake : asyncio.Event = field(default_factory=asyncio.Event) # tell the sender task there is work to do
                                                              # so we can individually send rather than broadcast
  sender : asyncio.Task | None = None
  drafts_dropped : int = 0 # drafts overwritten before they could be sent

  def draft_delay(self) -> float | None:
    return 0.0

  async def after_send(self) -> None:
    pass

  def start(self) -> None:
    pass

  def record_update(self, now: float, final: bool) -> None:
    pass

  def close(self) -> None:
    pass

  async def aclose(self) -> None:
    self.close()

  @property
  def delivery_status(self) -> str:
    return "DISABLED"


@dataclass
class ClientState(BaseClientState):
  """Extend normal client delivery with a replaceable flow-control policy."""

  flow: DeliveryPolicy = field(kw_only=True)

  def draft_delay(self) -> float | None:
    return self.flow.draft_delay()

  async def after_send(self) -> None:
    await self.flow.after_send()

  def start(self) -> None:
    self.flow.start()

  def record_update(self, now: float, final: bool) -> None:
    self.flow.record_update(now, final)

  def close(self) -> None:
    self.flow.close()

  async def aclose(self) -> None:
    await self.flow.aclose()

  @property
  def delivery_status(self) -> str:
    return self.flow.status

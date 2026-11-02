import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Literal, NotRequired, TypedDict

from flow_control import FlowControl

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
    final : bool
    text : str

    @classmethod
    def from_dict(cls, caption_dict : dict) -> "Caption":
        session = caption_dict.get("session")
        segment = caption_dict.get("segment")
        revision = caption_dict.get("revision")
        source_ts = caption_dict.get("source_ts")
        final = caption_dict.get("final")
        text = caption_dict.get("text")
        return cls(session, segment, revision,
                   source_ts, final, text)


@dataclass
class ClientState:
  ws : object # WebSocket Object
  pending_finals : deque[Caption]
  pending_active : Caption | None
  last_final : tuple[int, int]  # (segment, revision)
  client_id : object = None # from CLIENT_HELLO, for logs
  wake : asyncio.Event = field(default_factory=asyncio.Event) # tell the sender task there is work to do
                                                              # so we can individually send rather than broadcast
  sender : asyncio.Task | None = None
  terminal_sync_sent : bool = False
  drafts_dropped : int = 0 # drafts overwritten before they could be sent
  final_pressure : bool = False # this client has a growing final backlog
  flow : FlowControl = field(default_factory=FlowControl)

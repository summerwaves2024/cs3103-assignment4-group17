import asyncio  # Provides the event type and task type used by each client.
from collections import deque  # Provides the queue used for final captions.
from dataclasses import dataclass, field  # Provides compact state containers.
from typing import Literal, NotRequired, TypedDict  # Describes protocol messages.

from flow_control import FlowControl  # Gives each client its own flow policy.

class SourceHello(TypedDict):
    # Identifies a connection from the caption source.
    type : Literal["SOURCE_HELLO"]
    session : str

class ClientHello(TypedDict):
    # Identifies a browser client and its recovery checkpoint.
    type : Literal["CLIENT_HELLO"]
    client_id : int
    session : str
    last_segment : int
    last_revision : int
    history_seconds : NotRequired[int]   # server defaults to 30

@dataclass
class Caption:
    # Stores one provisional or finalised caption revision.
    session : str
    segment : int
    revision : int
    source_ts : float
    final : bool
    text : str

    @classmethod
    def from_dict(cls, caption_dict : dict) -> "Caption":
        # Read protocol fields without changing their values.
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
  # Stores all state that belongs to one browser connection.
  ws : object # WebSocket Object
  pending_finals : deque[Caption]  # Final captions waiting for delivery.
  pending_active : Caption | None  # Newest provisional caption waiting for delivery.
  client_id : object = None # Identifier received in CLIENT_HELLO.
  wake : asyncio.Event = field(default_factory=asyncio.Event) # Notifies the sender about new work.
  sender : asyncio.Task | None = None  # Task that writes to this client's socket.
  drafts_dropped : int = 0 # Count of replaced provisional captions.
  flow : FlowControl = field(default_factory=FlowControl)  # Per-client flow policy.

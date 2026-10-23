import asyncio
from collections import deque
from dataclasses import dataclass
from enum import Enum

class ClientStatus(Enum):
    NORMAL = 1
    LAGGING = 2
    COMPLETED = 3
    RECONNECTING = 4

@dataclass
class Caption:
    session : str
    segment : int
    revision : int
    source_ts : float
    final : bool
    text : str


@dataclass
class ClientState:
  ws : object # WebSocket Object
  pending_finals : deque[Caption]
  pending_active : Caption | None
  mode : ClientStatus # True -> “normal” or False -> “lagging”
  last_final : tuple[int, int]  # (segment, revision)
  wake : asyncio.Event # tell the sender task there is work to do 
                       # so we can individually send rather than broadcast
  sender: asyncio.Task 
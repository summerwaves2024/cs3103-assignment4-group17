from dataclasses import dataclass
import asyncio
from collections import deque
from dataclasses import dataclass

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
  mode : bool # True -> “normal” or False -> “lagging”
  last_final : tuple[int, int]  # (segment, revision)
  wake : asyncio.Event # tell the sender task there is work to do 
                       # so we can individually send rather than broadcast
  sender: asyncio.Task 
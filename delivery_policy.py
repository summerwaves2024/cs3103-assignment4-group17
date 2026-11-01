"""Common server interface for interchangeable caption delivery policies."""

from typing import Protocol


class DeliveryPolicy(Protocol):
    @property
    def status(self) -> str: ...

    def start(self) -> None:
        """Start background work after client state and callbacks are ready."""
        ...

    def draft_delay(self) -> float | None:
        """Zero permits a draft; a delay retries later; None awaits a wakeup."""
        ...

    async def after_send(self) -> None: ...

    def record_update(self, now: float, final: bool) -> None:
        """Observe an accepted source update, including updates not delivered."""
        ...

    def close(self) -> None:
        """Request shutdown without waiting. Use aclose to finish cleanup."""
        ...

    async def aclose(self) -> None: ...


class NoFlowControl:
    """Always permit drafts, with no custom probes or background tasks."""

    status = "DISABLED"

    def start(self) -> None:
        pass

    def draft_delay(self) -> float:
        return 0.0

    async def after_send(self) -> None:
        pass

    def record_update(self, now: float, final: bool) -> None:
        pass

    def close(self) -> None:
        pass

    async def aclose(self) -> None:
        self.close()


def empty_finals():
    """Default for standalone policies without a server queue to observe."""
    return (), None

"""Transport protocol used by concrete WebSocket or message adapters."""

from __future__ import annotations

from typing import Any
from typing import Protocol

from object_streams.events import StreamEvent
from object_streams.subscriptions import SubscriptionRequest


__all__ = ("Transport",)


class Transport(Protocol):
    async def send_subscribed(self, subscription: SubscriptionRequest) -> None:
        """Send a subscription acknowledgement."""

    async def send_event(self, event: StreamEvent) -> None:
        """Send a stream event."""

    async def send_error(self, code: str, message: str, *, details: Any = None) -> None:
        """Send a protocol error."""

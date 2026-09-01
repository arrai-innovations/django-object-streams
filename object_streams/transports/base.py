"""Transport protocol used by concrete WebSocket or message adapters."""

from __future__ import annotations

from typing import Any
from typing import Protocol

from object_streams.events import StreamEvent
from object_streams.subscriptions import ResyncRequired
from object_streams.subscriptions import SubscriptionRequest


__all__ = ("Transport",)


class Transport(Protocol):
    async def prepare_subscription(self, subscription: SubscriptionRequest) -> None:
        """Prepare transport routing before the subscription is acknowledged."""

    async def send_subscribed(self, subscription: SubscriptionRequest) -> None:
        """Send a subscription acknowledgement."""

    async def send_unsubscribed(self, subscription_id: str) -> None:
        """Send an unsubscribe acknowledgement."""

    async def send_event(self, event: StreamEvent) -> None:
        """Send a stream event."""

    async def send_resync(self, resync: ResyncRequired) -> None:
        """Send a subscription-level resync instruction."""

    async def send_error(self, code: str, message: str, *, details: Any = None) -> None:
        """Send a protocol error."""

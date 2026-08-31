"""Channels transport helpers."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncJsonWebsocketConsumer

from object_streams.events import ObjectRef
from object_streams.events import StreamEvent
from object_streams.models import ObjectStreamEvent
from object_streams.registry import ObjectStreamRegistry
from object_streams.registry import registry as default_registry
from object_streams.sessions import AsyncSubscriptionSession
from object_streams.subscriptions import ResyncRequired
from object_streams.subscriptions import SubscriptionRequest


__all__ = (
    "ObjectStreamConsumer",
    "model_group_name",
    "object_group_name",
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def object_group_name(ref: ObjectRef) -> str:
    """Return a stable Channels group name for an object subject."""

    return f"object_streams.object.{_digest(f'{ref.model}:{ref.pk}')}"


def model_group_name(model_label: str) -> str:
    """Return a stable Channels group name for model-level fanout."""

    return f"object_streams.model.{_digest(model_label)}"


class ObjectStreamConsumer(AsyncJsonWebsocketConsumer):
    """Minimal Channels consumer for JSON object stream subscriptions."""

    registry: ObjectStreamRegistry = default_registry
    session_class = AsyncSubscriptionSession

    def __init__(
        self,
        *args: Any,
        registry: ObjectStreamRegistry | None = None,
        session_class: type[AsyncSubscriptionSession] | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        if registry is not None:
            self.registry = registry
        if session_class is not None:
            self.session_class = session_class

    async def connect(self) -> None:
        self.session = self.session_class(
            user=self.scope.get("user"),
            request=self.scope,
            transport=self,
            registry=self.registry,
        )
        await self.accept()

    async def receive_json(self, content: Any, **kwargs: Any) -> None:
        if not isinstance(content, Mapping):
            await self.send_error("invalid_request", "Messages must be JSON objects.")
            return
        await self.session.handle_message(content)

    async def object_stream_event(self, message: Mapping[str, Any]) -> None:
        event_id = message.get("id") or message.get("outbox_id")
        if event_id is None:
            await self.send_error("invalid_event", "Object stream events require an outbox id.")
            return

        try:
            row = await database_sync_to_async(self._get_outbox_event)(event_id)
        except (ObjectStreamEvent.DoesNotExist, TypeError, ValueError):
            await self.send_error("event_not_found", "Object stream event does not exist.")
            return

        await self.session.publish(row)

    async def send_subscribed(self, subscription: SubscriptionRequest) -> None:
        payload = subscription.as_dict()
        payload.pop("op", None)
        payload["type"] = "subscribed"
        await self.send_json(payload)

    async def send_unsubscribed(self, subscription_id: str) -> None:
        await self.send_json(
            {
                "type": "unsubscribed",
                "subscription_id": subscription_id,
            }
        )

    async def send_event(self, event: StreamEvent) -> None:
        await self.send_json(event.as_dict())

    async def send_resync(self, resync: ResyncRequired) -> None:
        await self.send_json(resync.as_dict())

    async def send_error(self, code: str, message: str, *, details: Any = None) -> None:
        payload = {
            "type": "error",
            "code": code,
            "message": message,
        }
        if details is not None:
            payload["details"] = details
        await self.send_json(payload)

    def _get_outbox_event(self, event_id: Any) -> ObjectStreamEvent:
        return ObjectStreamEvent.objects.select_related(
            "subject_content_type",
            "source_content_type",
            "source_history_content_type",
        ).get(pk=int(event_id))

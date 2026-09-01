"""Channels transport helpers."""

from __future__ import annotations

import hashlib
from collections import deque
from collections.abc import Mapping
from typing import Any

from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncJsonWebsocketConsumer
from channels.layers import get_channel_layer

from object_streams.events import ObjectRef
from object_streams.events import StreamEvent
from object_streams.models import ObjectStreamEvent
from object_streams.registry import ObjectStreamRegistry
from object_streams.registry import registry as default_registry
from object_streams.sessions import AsyncSubscriptionSession
from object_streams.subscriptions import ResyncRequired
from object_streams.subscriptions import SubscriptionKind
from object_streams.subscriptions import SubscriptionRequest


__all__ = (
    "ObjectStreamConsumer",
    "broadcast_outbox_event",
    "broadcast_outbox_event_sync",
    "model_group_name",
    "object_group_name",
    "outbox_event_group_names",
    "subscription_group_names",
)


OUTBOX_EVENT_MESSAGE_TYPE = "object.stream.event"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def object_group_name(ref: ObjectRef) -> str:
    """Return a stable Channels group name for an object subject."""

    return f"object_streams.object.{_digest(f'{ref.model}:{ref.pk}')}"


def model_group_name(model_label: str) -> str:
    """Return a stable Channels group name for model-level fanout."""

    return f"object_streams.model.{_digest(model_label)}"


def subscription_group_names(subscription: SubscriptionRequest) -> tuple[str, ...]:
    """Return Channels groups needed to wake a subscription."""

    if subscription.kind == SubscriptionKind.OBJECT:
        if subscription.pk is None:
            msg = "Object subscriptions require a primary key."
            raise ValueError(msg)
        return (object_group_name(ObjectRef(model=subscription.model, pk=subscription.pk)),)
    return (model_group_name(subscription.model),)


def outbox_event_group_names(event: StreamEvent) -> tuple[str, ...]:
    """Return Channels groups that should receive an outbox event."""

    return (
        model_group_name(event.subject.model),
        object_group_name(event.subject),
    )


async def broadcast_outbox_event(event_or_id: ObjectStreamEvent | int) -> None:
    """Fan out an outbox event id through the configured Channels layer."""

    channel_layer = get_channel_layer()
    if channel_layer is None:
        msg = "No Channels channel layer is configured."
        raise RuntimeError(msg)

    row = await database_sync_to_async(_coerce_outbox_event)(event_or_id)
    event = await database_sync_to_async(row.to_stream_event)()
    message = {
        "type": OUTBOX_EVENT_MESSAGE_TYPE,
        "id": row.pk,
    }
    for group_name in outbox_event_group_names(event):
        await channel_layer.group_send(group_name, message)


def broadcast_outbox_event_sync(event_or_id: ObjectStreamEvent | int) -> None:
    """Synchronous wrapper for management commands and other Django call sites."""

    async_to_sync(broadcast_outbox_event)(event_or_id)


def _coerce_outbox_event(event_or_id: ObjectStreamEvent | int) -> ObjectStreamEvent:
    if isinstance(event_or_id, ObjectStreamEvent):
        return event_or_id
    return _get_outbox_event(event_or_id)


def _get_outbox_event(event_id: Any) -> ObjectStreamEvent:
    return ObjectStreamEvent.objects.select_related(
        "subject_content_type",
        "source_content_type",
        "source_history_content_type",
    ).get(pk=int(event_id))


class ObjectStreamConsumer(AsyncJsonWebsocketConsumer):
    """Minimal Channels consumer for JSON object stream subscriptions."""

    registry: ObjectStreamRegistry = default_registry
    session_class = AsyncSubscriptionSession
    event_dedupe_size = 1024

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
        self._subscription_groups: dict[str, tuple[str, ...]] = {}
        self._group_ref_counts: dict[str, int] = {}
        self._seen_outbox_ids: set[int] = set()
        self._seen_outbox_order: deque[int] = deque()

    async def connect(self) -> None:
        self.session = self.session_class(
            user=self.scope.get("user"),
            request=self.scope,
            transport=self,
            registry=self.registry,
        )
        await self.accept()

    async def disconnect(self, code: int) -> None:
        await self._discard_all_groups()

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
            outbox_id = int(event_id)
        except (TypeError, ValueError):
            await self.send_error("event_not_found", "Object stream event does not exist.")
            return

        if not self._remember_outbox_id(outbox_id):
            return

        try:
            row = await database_sync_to_async(_get_outbox_event)(outbox_id)
        except ObjectStreamEvent.DoesNotExist:
            await self.send_error("event_not_found", "Object stream event does not exist.")
            return

        await self.session.publish(row)

    async def send_subscribed(self, subscription: SubscriptionRequest) -> None:
        payload = subscription.as_dict()
        payload.pop("op", None)
        payload["type"] = "subscribed"
        await self.send_json(payload)

    async def prepare_subscription(self, subscription: SubscriptionRequest) -> None:
        await self._add_subscription_groups(subscription)

    async def send_unsubscribed(self, subscription_id: str) -> None:
        await self._discard_subscription_groups(subscription_id)
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

    async def _add_subscription_groups(self, subscription: SubscriptionRequest) -> None:
        if subscription.subscription_id is None:
            return
        groups = subscription_group_names(subscription)
        self._subscription_groups[subscription.subscription_id] = groups
        for group_name in groups:
            await self._add_group(group_name)

    async def _discard_subscription_groups(self, subscription_id: str) -> None:
        groups = self._subscription_groups.pop(subscription_id, ())
        for group_name in groups:
            await self._discard_group(group_name)

    async def _discard_all_groups(self) -> None:
        for group_name in tuple(self._group_ref_counts):
            self._group_ref_counts[group_name] = 1
            await self._discard_group(group_name)
        self._subscription_groups.clear()

    async def _add_group(self, group_name: str) -> None:
        count = self._group_ref_counts.get(group_name, 0)
        self._group_ref_counts[group_name] = count + 1
        if count == 0 and self.channel_layer is not None:
            await self.channel_layer.group_add(group_name, self.channel_name)

    async def _discard_group(self, group_name: str) -> None:
        count = self._group_ref_counts.get(group_name, 0)
        if count <= 1:
            self._group_ref_counts.pop(group_name, None)
            if self.channel_layer is not None:
                await self.channel_layer.group_discard(group_name, self.channel_name)
            return
        self._group_ref_counts[group_name] = count - 1

    def _remember_outbox_id(self, outbox_id: int) -> bool:
        if outbox_id in self._seen_outbox_ids:
            return False
        self._seen_outbox_ids.add(outbox_id)
        self._seen_outbox_order.append(outbox_id)
        while len(self._seen_outbox_order) > self.event_dedupe_size:
            expired_id = self._seen_outbox_order.popleft()
            self._seen_outbox_ids.discard(expired_id)
        return True

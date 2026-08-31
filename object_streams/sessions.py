"""Connection-local subscription coordination."""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import replace
from itertools import count
from typing import Any

from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async
from django.db import models

from object_streams.events import EventOperation
from object_streams.events import ListAction
from object_streams.events import ObjectRef
from object_streams.events import StreamEvent
from object_streams.exceptions import FilterValidationError
from object_streams.exceptions import NotRegistered
from object_streams.models import ObjectStreamEvent
from object_streams.outbox import latest_outbox_cursor
from object_streams.outbox import outbox_events_after
from object_streams.registry import ObjectStreamRegistration
from object_streams.registry import ObjectStreamRegistry
from object_streams.registry import registry as default_registry
from object_streams.subscriptions import ResyncRequired
from object_streams.subscriptions import SubscriptionKind
from object_streams.subscriptions import SubscriptionRequest
from object_streams.transports.base import Transport


__all__ = ("ActiveSubscription", "AsyncSubscriptionSession", "SubscriptionSession")


@dataclass(slots=True)
class ActiveSubscription:
    """Connection-local state for one active subscription."""

    request: SubscriptionRequest
    registration: ObjectStreamRegistration
    member_pks: set[str]

    @property
    def subscription_id(self) -> str:
        if self.request.subscription_id is None:
            msg = "Active subscriptions require a subscription id."
            raise ValueError(msg)
        return self.request.subscription_id


class _BaseSubscriptionSession:
    def __init__(
        self,
        *,
        user: Any,
        transport: Transport,
        registry: ObjectStreamRegistry = default_registry,
        request: Any = None,
        subscription_id_factory: Callable[[], str] | None = None,
        replay_limit: int = 1000,
    ):
        self.user = user
        self.transport = transport
        self.registry = registry
        self.request = request
        self.replay_limit = replay_limit
        self._subscriptions: dict[str, ActiveSubscription] = {}
        self._counter = count(1)
        self._subscription_id_factory = subscription_id_factory

    @property
    def subscriptions(self) -> tuple[ActiveSubscription, ...]:
        return tuple(self._subscriptions.values())

    def _coerce_subscription_request(self, message: Mapping[str, Any] | SubscriptionRequest) -> SubscriptionRequest:
        if isinstance(message, SubscriptionRequest):
            return message
        return SubscriptionRequest.from_message(message)

    def _next_subscription_id(self) -> str:
        if self._subscription_id_factory is not None:
            return self._subscription_id_factory()
        return f"sub_{next(self._counter)}"

    def _subscription_queryset(
        self,
        registration: ObjectStreamRegistration,
        request: SubscriptionRequest,
    ) -> models.QuerySet:
        filters = request.filters if request.kind == SubscriptionKind.FILTER else None
        queryset = registration.get_queryset(
            self.user,
            filters,
            request=self.request,
        )
        if request.kind == SubscriptionKind.OBJECT:
            queryset = queryset.filter(pk=request.pk)
        return queryset

    def _current_member_pks(
        self,
        registration: ObjectStreamRegistration,
        request: SubscriptionRequest,
    ) -> set[str]:
        return {str(pk) for pk in self._subscription_queryset(registration, request).values_list("pk", flat=True)}

    def _is_current_member(self, active: ActiveSubscription, event: StreamEvent) -> bool:
        if event.op == str(EventOperation.DELETED):
            return False
        return self._subscription_queryset(active.registration, active.request).filter(pk=event.subject.pk).exists()

    def _list_action(
        self,
        event: StreamEvent,
        *,
        before_member: bool,
        after_member: bool,
    ) -> ListAction | None:
        if before_member and after_member:
            return ListAction.CHANGED
        if not before_member and after_member:
            return ListAction.ADDED
        if before_member and not after_member:
            if event.op == str(EventOperation.DELETED):
                return ListAction.DELETED
            return ListAction.REMOVED
        return None

    def _has_collection_replay_events(
        self,
        active: ActiveSubscription,
        requested_cursor: int,
        through_cursor: int,
    ) -> bool:
        return outbox_events_after(
            requested_cursor,
            model=active.registration.model,
            through_cursor=through_cursor,
        ).exists()

    def _object_replay_events(
        self,
        active: ActiveSubscription,
        requested_cursor: int,
        through_cursor: int,
    ) -> tuple[StreamEvent, ...] | None:
        subject = ObjectRef(model=active.request.model, pk=active.request.pk)
        rows = list(
            outbox_events_after(
                requested_cursor,
                subject=subject,
                through_cursor=through_cursor,
                limit=self.replay_limit + 1,
            )
        )
        if len(rows) > self.replay_limit:
            return None

        events = []
        for row in rows:
            event = row.to_stream_event()
            list_action = ListAction.DELETED if event.op == str(EventOperation.DELETED) else ListAction.CHANGED
            events.append(
                replace(
                    event,
                    subscription_id=active.subscription_id,
                    list_action=list_action,
                )
            )
        return tuple(events)


class SubscriptionSession(_BaseSubscriptionSession):
    """Coordinate subscriptions and event delivery for one connection."""

    def handle_message(self, message: Mapping[str, Any]) -> SubscriptionRequest | bool | None:
        """Handle a subscribe or unsubscribe protocol message."""

        op = message.get("op")
        if op == "subscribe":
            return self.subscribe(message)
        if op == "unsubscribe":
            subscription_id = message.get("subscription_id")
            if subscription_id is None:
                self._send_error("invalid_request", "Unsubscribe messages require a subscription_id.")
                return None
            return self.unsubscribe(str(subscription_id))

        self._send_error("invalid_request", "Messages require a supported op.")
        return None

    def subscribe(self, message: Mapping[str, Any] | SubscriptionRequest) -> SubscriptionRequest | None:
        """Register a subscription and send its acknowledgement."""

        try:
            requested = self._coerce_subscription_request(message)
            registration = self.registry.get(requested.model)
        except (LookupError, NotRegistered, ValueError) as exc:
            self._send_error("invalid_request", str(exc))
            return None

        if requested.search is not None:
            self._send_error("unsupported_search", "Search subscriptions are not supported yet.")
            return None

        snapshot_cursor = latest_outbox_cursor()
        if requested.cursor is not None and requested.cursor > snapshot_cursor:
            self._send_error("invalid_cursor", "Subscription cursor is newer than the outbox.")
            return None

        subscription_id = requested.subscription_id or self._next_subscription_id()
        acknowledged = replace(requested, subscription_id=subscription_id, cursor=snapshot_cursor)

        try:
            member_pks = self._current_member_pks(registration, acknowledged)
        except FilterValidationError as exc:
            self._send_error("invalid_filter", "Subscription filters are invalid.", details=exc.errors)
            return None

        if acknowledged.kind == SubscriptionKind.OBJECT and not member_pks:
            self._send_error("not_found", "Object does not exist or is not visible.")
            return None

        active = ActiveSubscription(
            request=acknowledged,
            registration=registration,
            member_pks=member_pks,
        )
        self._subscriptions[subscription_id] = active
        self._send_subscribed(acknowledged)
        self._replay_requested_cursor(active, requested.cursor, through_cursor=snapshot_cursor)
        return acknowledged

    def unsubscribe(self, subscription_id: str) -> bool:
        """Remove a subscription and send its acknowledgement."""

        if subscription_id not in self._subscriptions:
            self._send_error("not_subscribed", "Subscription is not active.")
            return False

        del self._subscriptions[subscription_id]
        self._send_unsubscribed(subscription_id)
        return True

    def publish(self, event_or_row: StreamEvent | ObjectStreamEvent) -> list[StreamEvent]:
        """Evaluate and deliver one outbox event to active subscriptions."""

        event = event_or_row.to_stream_event() if isinstance(event_or_row, ObjectStreamEvent) else event_or_row
        delivered = []
        for active in tuple(self._subscriptions.values()):
            subscription_event = self.evaluate(active, event)
            if subscription_event is None:
                continue
            self._send_event(subscription_event)
            delivered.append(subscription_event)
        return delivered

    def evaluate(self, active: ActiveSubscription, event: StreamEvent) -> StreamEvent | None:
        """Return the subscription-relative event, or None when it has no effect."""

        if event.subject.model != active.request.model:
            return None
        if event.facet not in active.registration.facets:
            return None
        if active.request.kind == SubscriptionKind.OBJECT and event.subject.pk != active.request.pk:
            return None

        before_member = event.subject.pk in active.member_pks
        after_member = self._is_current_member(active, event)
        list_action = self._list_action(event, before_member=before_member, after_member=after_member)
        if list_action is None:
            return None

        if after_member:
            active.member_pks.add(event.subject.pk)
        else:
            active.member_pks.discard(event.subject.pk)

        return replace(
            event,
            subscription_id=active.subscription_id,
            list_action=list_action,
        )

    def _replay_requested_cursor(
        self,
        active: ActiveSubscription,
        requested_cursor: int | None,
        *,
        through_cursor: int,
    ) -> None:
        if requested_cursor is None or requested_cursor == through_cursor:
            return
        if active.request.kind == SubscriptionKind.OBJECT:
            self._replay_object_subscription(active, requested_cursor, through_cursor=through_cursor)
            return

        if self._has_collection_replay_events(active, requested_cursor, through_cursor):
            self._send_resync(
                ResyncRequired(
                    subscription_id=active.subscription_id,
                    cursor=through_cursor,
                )
            )

    def _replay_object_subscription(
        self,
        active: ActiveSubscription,
        requested_cursor: int,
        *,
        through_cursor: int,
    ) -> None:
        replay_events = self._object_replay_events(active, requested_cursor, through_cursor)
        if replay_events is None:
            self._send_resync(
                ResyncRequired(
                    subscription_id=active.subscription_id,
                    cursor=through_cursor,
                    reason="object_replay_limit_exceeded",
                )
            )
            return

        for event in replay_events:
            self._send_event(event)

    def _send_subscribed(self, subscription: SubscriptionRequest) -> None:
        async_to_sync(self.transport.send_subscribed)(subscription)

    def _send_unsubscribed(self, subscription_id: str) -> None:
        async_to_sync(self.transport.send_unsubscribed)(subscription_id)

    def _send_event(self, event: StreamEvent) -> None:
        async_to_sync(self.transport.send_event)(event)

    def _send_resync(self, resync: ResyncRequired) -> None:
        async_to_sync(self.transport.send_resync)(resync)

    def _send_error(self, code: str, message: str, *, details: Any = None) -> None:
        async_to_sync(self.transport.send_error)(code, message, details=details)


class AsyncSubscriptionSession(_BaseSubscriptionSession):
    """Async subscription coordinator for ASGI consumers."""

    async def handle_message(self, message: Mapping[str, Any]) -> SubscriptionRequest | bool | None:
        """Handle a subscribe or unsubscribe protocol message."""

        op = message.get("op")
        if op == "subscribe":
            return await self.subscribe(message)
        if op == "unsubscribe":
            subscription_id = message.get("subscription_id")
            if subscription_id is None:
                await self.transport.send_error("invalid_request", "Unsubscribe messages require a subscription_id.")
                return None
            return await self.unsubscribe(str(subscription_id))

        await self.transport.send_error("invalid_request", "Messages require a supported op.")
        return None

    async def subscribe(self, message: Mapping[str, Any] | SubscriptionRequest) -> SubscriptionRequest | None:
        """Register a subscription and send its acknowledgement."""

        try:
            requested = self._coerce_subscription_request(message)
            registration = self.registry.get(requested.model)
        except (LookupError, NotRegistered, ValueError) as exc:
            await self.transport.send_error("invalid_request", str(exc))
            return None

        if requested.search is not None:
            await self.transport.send_error("unsupported_search", "Search subscriptions are not supported yet.")
            return None

        snapshot_cursor = await database_sync_to_async(latest_outbox_cursor)()
        if requested.cursor is not None and requested.cursor > snapshot_cursor:
            await self.transport.send_error("invalid_cursor", "Subscription cursor is newer than the outbox.")
            return None

        subscription_id = requested.subscription_id or self._next_subscription_id()
        acknowledged = replace(requested, subscription_id=subscription_id, cursor=snapshot_cursor)

        try:
            member_pks = await database_sync_to_async(self._current_member_pks)(registration, acknowledged)
        except FilterValidationError as exc:
            await self.transport.send_error("invalid_filter", "Subscription filters are invalid.", details=exc.errors)
            return None

        if acknowledged.kind == SubscriptionKind.OBJECT and not member_pks:
            await self.transport.send_error("not_found", "Object does not exist or is not visible.")
            return None

        active = ActiveSubscription(
            request=acknowledged,
            registration=registration,
            member_pks=member_pks,
        )
        self._subscriptions[subscription_id] = active
        await self.transport.send_subscribed(acknowledged)
        await self._replay_requested_cursor(active, requested.cursor, through_cursor=snapshot_cursor)
        return acknowledged

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Remove a subscription and send its acknowledgement."""

        if subscription_id not in self._subscriptions:
            await self.transport.send_error("not_subscribed", "Subscription is not active.")
            return False

        del self._subscriptions[subscription_id]
        await self.transport.send_unsubscribed(subscription_id)
        return True

    async def publish(self, event_or_row: StreamEvent | ObjectStreamEvent) -> list[StreamEvent]:
        """Evaluate and deliver one outbox event to active subscriptions."""

        if isinstance(event_or_row, ObjectStreamEvent):
            event = await database_sync_to_async(event_or_row.to_stream_event)()
        else:
            event = event_or_row

        delivered = []
        for active in tuple(self._subscriptions.values()):
            subscription_event = await self.evaluate(active, event)
            if subscription_event is None:
                continue
            await self.transport.send_event(subscription_event)
            delivered.append(subscription_event)
        return delivered

    async def evaluate(self, active: ActiveSubscription, event: StreamEvent) -> StreamEvent | None:
        """Return the subscription-relative event, or None when it has no effect."""

        if event.subject.model != active.request.model:
            return None
        if event.facet not in active.registration.facets:
            return None
        if active.request.kind == SubscriptionKind.OBJECT and event.subject.pk != active.request.pk:
            return None

        before_member = event.subject.pk in active.member_pks
        after_member = await database_sync_to_async(self._is_current_member)(active, event)
        list_action = self._list_action(event, before_member=before_member, after_member=after_member)
        if list_action is None:
            return None

        if after_member:
            active.member_pks.add(event.subject.pk)
        else:
            active.member_pks.discard(event.subject.pk)

        return replace(
            event,
            subscription_id=active.subscription_id,
            list_action=list_action,
        )

    async def _replay_requested_cursor(
        self,
        active: ActiveSubscription,
        requested_cursor: int | None,
        *,
        through_cursor: int,
    ) -> None:
        if requested_cursor is None or requested_cursor == through_cursor:
            return
        if active.request.kind == SubscriptionKind.OBJECT:
            await self._replay_object_subscription(active, requested_cursor, through_cursor=through_cursor)
            return

        has_events = await database_sync_to_async(self._has_collection_replay_events)(
            active,
            requested_cursor,
            through_cursor,
        )
        if has_events:
            await self.transport.send_resync(
                ResyncRequired(
                    subscription_id=active.subscription_id,
                    cursor=through_cursor,
                )
            )

    async def _replay_object_subscription(
        self,
        active: ActiveSubscription,
        requested_cursor: int,
        *,
        through_cursor: int,
    ) -> None:
        replay_events = await database_sync_to_async(self._object_replay_events)(
            active,
            requested_cursor,
            through_cursor,
        )
        if replay_events is None:
            await self.transport.send_resync(
                ResyncRequired(
                    subscription_id=active.subscription_id,
                    cursor=through_cursor,
                    reason="object_replay_limit_exceeded",
                )
            )
            return

        for event in replay_events:
            await self.transport.send_event(event)

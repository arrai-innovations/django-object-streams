"""Lightweight object and queryset subscription primitives for Django.

Only names that are safe to import before the app registry is ready are
exported here. Modules that touch database models, the outbox, or a transport
stay behind their own imports: `object_streams.models`,
`object_streams.outbox`, `object_streams.producers`,
`object_streams.retention`, `object_streams.sessions`, and
`object_streams.transports`.
"""

from object_streams.events import EventOperation
from object_streams.events import ListAction
from object_streams.events import ObjectRef
from object_streams.events import SourceRef
from object_streams.events import StreamEvent
from object_streams.exceptions import AlreadyRegistered
from object_streams.exceptions import FilterValidationError
from object_streams.exceptions import NotRegistered
from object_streams.exceptions import ObjectStreamsError
from object_streams.exceptions import RegistrationError
from object_streams.registry import ObjectStreamRegistration
from object_streams.registry import ObjectStreamRegistry
from object_streams.registry import register
from object_streams.registry import registry
from object_streams.sources import ModelSource
from object_streams.sources import Source
from object_streams.subscriptions import ResyncRequired
from object_streams.subscriptions import SubscriptionKind
from object_streams.subscriptions import SubscriptionRequest
from object_streams.visibility import AllowAllVisibilityPolicy
from object_streams.visibility import DenyAllVisibilityPolicy
from object_streams.visibility import VisibilityPolicy


__all__ = (
    "AllowAllVisibilityPolicy",
    "AlreadyRegistered",
    "DenyAllVisibilityPolicy",
    "EventOperation",
    "FilterValidationError",
    "ListAction",
    "ModelSource",
    "NotRegistered",
    "ObjectRef",
    "ObjectStreamRegistration",
    "ObjectStreamRegistry",
    "ObjectStreamsError",
    "RegistrationError",
    "ResyncRequired",
    "Source",
    "SourceRef",
    "StreamEvent",
    "SubscriptionKind",
    "SubscriptionRequest",
    "VisibilityPolicy",
    "__version__",
    "register",
    "registry",
)

__version__ = "0.1.0a0"

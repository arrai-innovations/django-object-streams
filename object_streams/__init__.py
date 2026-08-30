"""Lightweight object and queryset subscription primitives for Django."""

from object_streams.registry import ObjectStreamRegistration
from object_streams.registry import ObjectStreamRegistry
from object_streams.registry import register
from object_streams.registry import registry


__all__ = (
    "ObjectStreamRegistration",
    "ObjectStreamRegistry",
    "__version__",
    "register",
    "registry",
)

__version__ = "0.1.0a0"

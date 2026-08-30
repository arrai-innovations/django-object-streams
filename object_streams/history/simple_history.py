"""Reserved extension point for django-simple-history integration."""

from __future__ import annotations

from typing import Any

from object_streams.events import SourceRef


__all__ = ("SimpleHistoryAdapter",)


class SimpleHistoryAdapter:
    """Adapter shell for django-simple-history records."""

    def source_ref_for_history(self, history_record: Any) -> SourceRef:
        msg = "django-simple-history source mapping is not implemented yet."
        raise NotImplementedError(msg)

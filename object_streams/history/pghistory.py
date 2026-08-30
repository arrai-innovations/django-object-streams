"""Reserved extension point for django-pghistory integration."""

from __future__ import annotations

from typing import Any

from object_streams.events import SourceRef


__all__ = ("PgHistoryAdapter",)


class PgHistoryAdapter:
    """Adapter shell for django-pghistory records."""

    def source_ref_for_history(self, history_record: Any) -> SourceRef:
        msg = "django-pghistory source mapping is not implemented yet."
        raise NotImplementedError(msg)

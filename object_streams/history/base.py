"""Base protocols for history-backed source adapters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import Protocol

from object_streams.events import SourceRef


__all__ = ("HistoryAdapter", "HistoryRecordRef")


@dataclass(frozen=True, slots=True)
class HistoryRecordRef:
    """Reference to an application history record that caused a stream event."""

    model: str
    pk: str
    history_model: str
    history_id: str

    def as_source_ref(self) -> SourceRef:
        return SourceRef(
            model=self.model,
            pk=self.pk,
            history_model=self.history_model,
            history_id=self.history_id,
        )


class HistoryAdapter(Protocol):
    def source_ref_for_history(self, history_record: Any) -> SourceRef:
        """Return a source reference for a history record."""

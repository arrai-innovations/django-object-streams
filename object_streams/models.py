"""Database models for replayable object stream events."""

from __future__ import annotations

from django.contrib.contenttypes.models import ContentType
from django.db import models

from object_streams.events import ObjectRef
from object_streams.events import SourceRef
from object_streams.events import StreamEvent


__all__ = ("ObjectStreamEvent",)


def _content_type_model_label(content_type: ContentType) -> str:
    model_class = content_type.model_class()
    if model_class is not None:
        return model_class._meta.label
    return f"{content_type.app_label}.{content_type.model}"


class ObjectStreamEvent(models.Model):
    """Outbox row that gives object streams a replayable global cursor."""

    subject_content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE, related_name="+")
    subject_object_id = models.TextField()
    source_content_type = models.ForeignKey(
        ContentType,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="+",
    )
    source_object_id = models.TextField(blank=True, default="")
    source_history_content_type = models.ForeignKey(
        ContentType,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="+",
    )
    source_history_id = models.TextField(blank=True, default="")
    facet = models.CharField(max_length=64, db_index=True)
    op = models.CharField(max_length=32)
    changed_fields = models.JSONField(default=list, blank=True)
    before = models.JSONField(null=True, blank=True)
    after = models.JSONField(null=True, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ["id"]
        indexes = [
            models.Index(fields=["subject_content_type", "subject_object_id", "id"]),
            models.Index(fields=["facet", "id"]),
        ]

    def __str__(self):
        return f"{self.subject_content_type}:{self.subject_object_id}:{self.id}"

    def to_stream_event(self) -> StreamEvent:
        source = None
        if self.source_content_type_id or self.source_history_content_type_id:
            source = SourceRef(
                model=_content_type_model_label(self.source_content_type) if self.source_content_type_id else None,
                pk=self.source_object_id or None,
                history_model=(
                    _content_type_model_label(self.source_history_content_type)
                    if self.source_history_content_type_id
                    else None
                ),
                history_id=self.source_history_id or None,
            )

        return StreamEvent(
            cursor=self.pk,
            subject=ObjectRef(
                model=_content_type_model_label(self.subject_content_type),
                pk=self.subject_object_id,
            ),
            facet=self.facet,
            op=self.op,
            changed_fields=tuple(self.changed_fields or ()),
            source=source,
            before=self.before,
            after=self.after,
            metadata=self.metadata or {},
        )

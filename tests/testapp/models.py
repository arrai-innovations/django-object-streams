from django.db import models

from object_streams.triggers import ObjectStreamTrigger


class Note(models.Model):
    title = models.CharField(max_length=100)


class TriggeredNote(models.Model):
    """Capture declared in model state, the way an integrator declares it."""

    title = models.CharField(max_length=100)
    body = models.TextField(blank=True, default="")

    class Meta:
        triggers = [ObjectStreamTrigger(name="triggered_note_stream")]


class ProxyTriggerTarget(models.Model):
    """Concrete model standing in for a model owned by another application."""

    title = models.CharField(max_length=100)


class ProxyTriggeredNote(ProxyTriggerTarget):
    """Attach capture without changing the concrete model's declaration."""

    class Meta:
        proxy = True
        triggers = [ObjectStreamTrigger(name="proxy_triggered_note_stream")]


class CompositeTriggerTarget(models.Model):
    """Unmanaged model used to exercise unsupported trigger compilation."""

    tenant_id = models.IntegerField()
    object_id = models.IntegerField()
    pk = models.CompositePrimaryKey("tenant_id", "object_id")

    class Meta:
        managed = False


class ProxyCompositeTriggerTarget(CompositeTriggerTarget):
    """Proxy coverage for concrete composite-key validation."""

    class Meta:
        proxy = True

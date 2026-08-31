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

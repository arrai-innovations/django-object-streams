import pgtrigger
from django.contrib.contenttypes.models import ContentType
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


class StateDefinition(models.Model):
    """Names the model that a generic state record governs."""

    # tests.testapp has no migrations, so its tables are created before the
    # contenttypes migration runs and cannot carry a constraint on that table.
    content_type = models.ForeignKey(ContentType, null=True, on_delete=models.CASCADE, db_constraint=False)
    name = models.CharField(max_length=100)


class StateRecord(models.Model):
    """Generic state row whose subject is the governed object, not the row itself."""

    definition = models.ForeignKey(StateDefinition, on_delete=models.CASCADE)
    object_id = models.CharField(max_length=64)
    state = models.CharField(max_length=100)

    class Meta:
        triggers = [
            ObjectStreamTrigger(
                name="state_record_stream",
                facet="workflow_state",
                subject_content_type="definition__content_type",
                subject_pk="object_id",
            )
        ]


class NoteComment(models.Model):
    """Child row whose subject is its parent note."""

    note = models.ForeignKey(Note, null=True, on_delete=models.CASCADE)
    body = models.TextField(blank=True, default="")

    class Meta:
        triggers = [ObjectStreamTrigger(name="note_comment_stream", facet="comments", subject="note")]


class SluggedNote(models.Model):
    slug = models.SlugField(unique=True)


class SlugComment(models.Model):
    """Child row whose foreign key targets a unique column rather than the primary key."""

    note = models.ForeignKey(SluggedNote, to_field="slug", on_delete=models.CASCADE)

    class Meta:
        triggers = [ObjectStreamTrigger(name="slug_comment_stream", subject="note")]


class ProtectedNote(models.Model):
    """Carries a pgtrigger trigger that is not a capture trigger."""

    title = models.CharField(max_length=100)

    class Meta:
        triggers = [pgtrigger.Protect(name="protected_note_delete", operation=pgtrigger.Delete)]


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

import pytest
from django.db import connection
from django.db import transaction

from object_streams.models import ObjectStreamEvent
from object_streams.registry import ObjectStreamRegistry
from tests.testapp.models import CompositeTriggerTarget
from tests.testapp.models import ProxyCompositeTriggerTarget
from tests.testapp.models import ProxyTriggeredNote
from tests.testapp.models import ProxyTriggerTarget
from tests.testapp.models import TriggeredNote


def outbox_rows():
    return list(ObjectStreamEvent.objects.order_by("id"))


@pytest.mark.django_db
def test_insert_writes_a_created_event():
    note = TriggeredNote.objects.create(title="Open")

    [row] = outbox_rows()
    assert row.op == "created"
    assert row.facet == "object"
    assert row.subject_object_id == str(note.pk)
    assert row.subject_content_type.model_class() is TriggeredNote
    assert row.changed_fields == []


@pytest.mark.django_db
def test_update_records_the_columns_that_actually_changed():
    note = TriggeredNote.objects.create(title="Open")
    note.title = "Closed"
    note.save()

    row = outbox_rows()[-1]
    assert row.op == "updated"
    assert row.changed_fields == ["title"]


@pytest.mark.django_db
def test_changed_fields_are_recorded_without_update_fields():
    """The signal producers depend on update_fields; the trigger compares the rows."""
    note = TriggeredNote.objects.create(title="Open", body="first")
    note.title = "Closed"
    note.body = "second"
    note.save()

    row = outbox_rows()[-1]
    assert row.changed_fields == ["body", "title"]


@pytest.mark.django_db
def test_a_write_that_changes_nothing_records_no_changed_fields():
    note = TriggeredNote.objects.create(title="Open")
    note.save()

    row = outbox_rows()[-1]
    assert row.op == "updated"
    assert row.changed_fields == []


@pytest.mark.django_db
def test_delete_writes_a_deleted_event():
    """Deletes are captured, which the post_save producers cannot do."""
    note = TriggeredNote.objects.create(title="Open")
    pk = note.pk
    note.delete()

    row = outbox_rows()[-1]
    assert row.op == "deleted"
    assert row.subject_object_id == str(pk)


@pytest.mark.django_db
def test_the_source_reference_is_the_row_itself():
    note = TriggeredNote.objects.create(title="Open")

    event = outbox_rows()[-1].to_stream_event()
    assert event.subject.model == "testapp.TriggeredNote"
    assert event.source.model == "testapp.TriggeredNote"
    assert event.source.pk == str(note.pk)
    assert event.source.history_model is None


@pytest.mark.django_db
def test_events_from_one_transaction_share_a_transaction_id():
    with transaction.atomic():
        TriggeredNote.objects.create(title="One")
        TriggeredNote.objects.create(title="Two")

    transaction_ids = {row.metadata["transaction_id"] for row in outbox_rows()}
    assert len(transaction_ids) == 1


@pytest.mark.django_db(transaction=True)
def test_separate_transactions_get_separate_transaction_ids():
    """Needs real transactions: nested atomic() blocks are savepoints sharing one xid."""
    with transaction.atomic():
        TriggeredNote.objects.create(title="One")
    with transaction.atomic():
        TriggeredNote.objects.create(title="Two")

    transaction_ids = [row.metadata["transaction_id"] for row in outbox_rows()]
    assert len(set(transaction_ids)) == len(transaction_ids)


@pytest.mark.django_db
def test_a_bulk_update_writes_one_event_per_row():
    """Bulk writes bypass save() and signals entirely, but not the trigger."""
    TriggeredNote.objects.create(title="One")
    TriggeredNote.objects.create(title="Two")
    before = ObjectStreamEvent.objects.count()

    TriggeredNote.objects.update(title="Renamed")

    written = ObjectStreamEvent.objects.order_by("id")[before:]
    assert [row.op for row in written] == ["updated", "updated"]
    assert {row.metadata["transaction_id"] for row in written} == {written[0].metadata["transaction_id"]}


@pytest.mark.django_db(transaction=True)
def test_a_rolled_back_write_leaves_no_event():
    with pytest.raises(RuntimeError), transaction.atomic():
        TriggeredNote.objects.create(title="Doomed")
        msg = "rollback"
        raise RuntimeError(msg)

    assert outbox_rows() == []


@pytest.mark.django_db(transaction=True)
def test_the_trigger_notifies_the_listener_on_commit():
    notifications = []

    with connection.cursor() as cursor:
        cursor.execute("LISTEN object_streams_events")

    note = TriggeredNote.objects.create(title="Open")

    raw = connection.connection
    raw.execute("SELECT 1")
    for notification in raw.notifies(timeout=5, stop_after=1):
        notifications.append(notification)

    with connection.cursor() as cursor:
        cursor.execute("UNLISTEN object_streams_events")

    assert notifications, "expected a notification for the committed outbox row"
    row = ObjectStreamEvent.objects.get(subject_object_id=str(note.pk))
    assert int(notifications[0].payload) == row.pk


@pytest.mark.django_db
def test_the_trigger_is_installed_for_the_declared_model():
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT tgname FROM pg_trigger WHERE tgrelid = %s::regclass AND NOT tgisinternal",
            [TriggeredNote._meta.db_table],
        )
        names = [name for (name,) in cursor.fetchall()]

    assert any("triggered_note_stream" in name for name in names), names


@pytest.mark.django_db
def test_a_trigger_declared_on_a_proxy_emits_the_concrete_model():
    registry = ObjectStreamRegistry()
    registration = registry.register(ProxyTriggerTarget)

    note = ProxyTriggerTarget.objects.create(title="Open")

    row = ObjectStreamEvent.objects.get(subject_object_id=str(note.pk))
    event = row.to_stream_event()
    assert event.subject.model == registration.model_label
    assert row.subject_content_type.model_class() is ProxyTriggerTarget
    assert event.source.model == registration.model_label


def test_a_proxy_trigger_compiles_with_the_concrete_model_identity():
    from object_streams.triggers import ObjectStreamTrigger

    compiled = ObjectStreamTrigger(name="proxy_probe").compile(ProxyTriggeredNote)

    assert "app_label = 'testapp' AND model = 'proxytriggertarget'" in compiled.sql
    assert "model = 'proxytriggerednote'" not in compiled.sql


@pytest.mark.parametrize("model", [CompositeTriggerTarget, ProxyCompositeTriggerTarget])
def test_a_trigger_rejects_a_composite_primary_key(model):
    from object_streams.triggers import ObjectStreamTrigger

    with pytest.raises(
        ValueError,
        match=r"does not support composite primary keys \(testapp.CompositeTriggerTarget\)",
    ):
        ObjectStreamTrigger(name="composite_probe").compile(model)


def test_a_changed_facet_changes_the_compiled_trigger():
    """Migrations serialize the compiled SQL, so options must reach the trigger body."""
    from object_streams.triggers import ObjectStreamTrigger

    default = ObjectStreamTrigger(name="probe").compile(TriggeredNote)
    other_facet = ObjectStreamTrigger(name="probe", facet="workflow_state").compile(TriggeredNote)

    assert "'object'" in default.sql
    assert "'workflow_state'" in other_facet.sql
    assert default.sql != other_facet.sql


def test_the_trigger_rejects_an_invalid_notify_channel():
    from object_streams.triggers import ObjectStreamTrigger

    with pytest.raises(ValueError, match="identifier"):
        ObjectStreamTrigger(name="probe", channel="not a channel")

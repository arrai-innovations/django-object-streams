import pytest
from django.apps import apps
from django.contrib.contenttypes.models import ContentType
from django.db import connection
from django.db import transaction
from django.db.migrations.state import ProjectState

from object_streams.events import ObjectRef
from object_streams.models import ObjectStreamEvent
from object_streams.triggers import ObjectStreamTrigger
from tests.testapp.models import Note
from tests.testapp.models import NoteComment
from tests.testapp.models import SlugComment
from tests.testapp.models import SluggedNote
from tests.testapp.models import StateDefinition
from tests.testapp.models import StateRecord
from tests.testapp.models import TriggeredNote


def outbox_rows():
    return list(ObjectStreamEvent.objects.order_by("id"))


def new_rows(before):
    return list(ObjectStreamEvent.objects.filter(id__gt=before).order_by("id"))


def last_id():
    row = ObjectStreamEvent.objects.order_by("-id").first()
    return row.pk if row is not None else 0


def subjects(rows):
    return [row.to_stream_event().subject for row in rows]


def note_definition():
    return StateDefinition.objects.create(content_type=ContentType.objects.get_for_model(Note), name="review")


@pytest.mark.django_db
def test_a_state_record_insert_writes_an_event_for_the_governed_object():
    note = Note.objects.create(title="Open")
    record = StateRecord.objects.create(definition=note_definition(), object_id=str(note.pk), state="draft")

    [row] = outbox_rows()
    event = row.to_stream_event()
    assert event.subject == ObjectRef.from_instance(note)
    assert event.source.model == "testapp.StateRecord"
    assert event.source.pk == str(record.pk)
    assert event.facet == "workflow_state"
    assert event.op == "updated"
    assert event.changed_fields == ()
    assert event.transaction_id is not None


@pytest.mark.django_db
def test_a_state_record_update_reports_its_changed_columns():
    note = Note.objects.create(title="Open")
    record = StateRecord.objects.create(definition=note_definition(), object_id=str(note.pk), state="draft")
    before = last_id()

    record.state = "approved"
    record.save()

    [row] = new_rows(before)
    assert row.to_stream_event().subject == ObjectRef.from_instance(note)
    assert row.op == "updated"
    assert row.changed_fields == ["state"]


@pytest.mark.django_db
def test_a_state_record_delete_maps_through_the_old_row():
    note = Note.objects.create(title="Open")
    record = StateRecord.objects.create(definition=note_definition(), object_id=str(note.pk), state="draft")
    record_pk = record.pk
    before = last_id()

    record.delete()

    [row] = new_rows(before)
    assert row.to_stream_event().subject == ObjectRef.from_instance(note)
    assert row.source_object_id == str(record_pk)
    assert row.op == "updated"


@pytest.mark.django_db
def test_moving_a_state_record_to_another_object_writes_for_both_subjects():
    first = Note.objects.create(title="First")
    second = Note.objects.create(title="Second")
    record = StateRecord.objects.create(definition=note_definition(), object_id=str(first.pk), state="draft")
    before = last_id()

    record.object_id = str(second.pk)
    record.save()

    rows = new_rows(before)
    assert subjects(rows) == [ObjectRef.from_instance(first), ObjectRef.from_instance(second)]
    assert [row.changed_fields for row in rows] == [["object_id"], ["object_id"]]
    assert len({row.metadata["transaction_id"] for row in rows}) == 1


@pytest.mark.django_db
def test_moving_a_state_record_to_another_model_writes_for_both_subjects():
    note = Note.objects.create(title="Open")
    other = TriggeredNote.objects.create(title="Other")
    other_definition = StateDefinition.objects.create(
        content_type=ContentType.objects.get_for_model(TriggeredNote),
        name="review",
    )
    record = StateRecord.objects.create(definition=note_definition(), object_id=str(note.pk), state="draft")
    before = last_id()

    record.definition = other_definition
    record.object_id = str(other.pk)
    record.save()

    rows = new_rows(before)
    assert subjects(rows) == [ObjectRef.from_instance(note), ObjectRef.from_instance(other)]
    assert rows[0].changed_fields == ["definition_id", "object_id"]


@pytest.mark.django_db
def test_a_state_record_whose_content_type_does_not_resolve_is_skipped():
    definition = StateDefinition.objects.create(content_type=None, name="unbound")

    StateRecord.objects.create(definition=definition, object_id="1", state="draft")

    assert outbox_rows() == []


@pytest.mark.django_db
def test_a_child_row_writes_an_event_for_its_parent():
    note = Note.objects.create(title="Open")
    comment = NoteComment.objects.create(note=note, body="first")

    [row] = outbox_rows()
    event = row.to_stream_event()
    assert event.subject == ObjectRef.from_instance(note)
    assert event.source.model == "testapp.NoteComment"
    assert event.source.pk == str(comment.pk)
    assert event.facet == "comments"


@pytest.mark.django_db
def test_a_child_row_moved_to_another_parent_writes_for_both_parents():
    first = Note.objects.create(title="First")
    second = Note.objects.create(title="Second")
    comment = NoteComment.objects.create(note=first)
    before = last_id()

    NoteComment.objects.filter(pk=comment.pk).update(note=second)

    rows = new_rows(before)
    assert subjects(rows) == [ObjectRef.from_instance(first), ObjectRef.from_instance(second)]
    assert rows[0].changed_fields == ["note_id"]


@pytest.mark.django_db
def test_a_child_row_detached_from_its_parent_writes_only_for_the_old_parent():
    note = Note.objects.create(title="Open")
    comment = NoteComment.objects.create(note=note)
    before = last_id()

    comment.note = None
    comment.save()

    assert subjects(new_rows(before)) == [ObjectRef.from_instance(note)]


@pytest.mark.django_db
def test_a_child_row_without_a_parent_is_skipped():
    NoteComment.objects.create(note=None)

    assert outbox_rows() == []


@pytest.mark.django_db
def test_a_child_row_delete_cascading_from_its_parent_maps_through_the_old_row():
    note = Note.objects.create(title="Open")
    NoteComment.objects.create(note=note)
    note_pk = note.pk
    before = last_id()

    with transaction.atomic():
        note.delete()

    [row] = new_rows(before)
    assert row.subject_object_id == str(note_pk)


@pytest.mark.django_db
def test_an_orm_cascade_through_a_multi_hop_path_is_captured():
    note = Note.objects.create(title="Open")
    definition = note_definition()
    StateRecord.objects.create(definition=definition, object_id=str(note.pk), state="draft")
    before = last_id()

    definition.delete()

    [row] = new_rows(before)
    assert row.to_stream_event().subject == ObjectRef.from_instance(note)


@pytest.mark.django_db
def test_a_multi_hop_path_through_an_already_deleted_row_is_skipped():
    """A later hop reads the current row, so a parent deleted first leaves no subject."""
    note = Note.objects.create(title="Open")
    definition = note_definition()
    StateRecord.objects.create(definition=definition, object_id=str(note.pk), state="draft")
    before = last_id()

    with transaction.atomic(), connection.cursor() as cursor:
        cursor.execute(f"DELETE FROM {StateDefinition._meta.db_table} WHERE id = %s", [definition.pk])
        cursor.execute(f"DELETE FROM {StateRecord._meta.db_table} WHERE definition_id = %s", [definition.pk])

    assert new_rows(before) == []


def test_a_schema_qualified_table_is_quoted_as_django_quotes_it(monkeypatch):
    monkeypatch.setattr(StateDefinition._meta, "db_table", 'workflow"."statedefinition')

    func = StateRecord._meta.triggers[0].compile(StateRecord).sql.kwargs["func"]

    assert 'FROM "workflow"."statedefinition" WHERE' in func


@pytest.mark.django_db
def test_a_foreign_key_to_a_unique_column_maps_to_the_parent_primary_key():
    note = SluggedNote.objects.create(slug="intro")

    SlugComment.objects.create(note=note)

    [row] = outbox_rows()
    assert row.to_stream_event().subject == ObjectRef.from_instance(note)


def test_placeholder_text_in_a_facet_stays_literal():
    trigger = ObjectStreamTrigger(name="probe", facet="a__TRANSACTION_ID_KEY__b")

    assert "'a__TRANSACTION_ID_KEY__b'" in trigger.compile(TriggeredNote).sql.kwargs["func"]


def test_the_default_trigger_body_is_not_mapped():
    trigger = ObjectStreamTrigger(name="probe")

    assert not trigger.is_mapped
    assert "subject_row := to_jsonb(NEW);" in trigger.compile(TriggeredNote).sql.kwargs["func"]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"subject": "note", "subject_pk": "object_id"}, "either subject or"),
        ({"subject_content_type": "definition__content_type"}, "together"),
        ({"subject_pk": "object_id"}, "together"),
    ],
)
def test_subject_options_must_be_combined_correctly(kwargs, message):
    with pytest.raises(ValueError, match=message):
        ObjectStreamTrigger(name="probe", **kwargs)


@pytest.mark.parametrize(
    ("model", "kwargs", "message"),
    [
        (NoteComment, {"subject": "missing"}, "has no field 'missing'"),
        (NoteComment, {"subject": "body"}, "must end in a foreign key"),
        (NoteComment, {"subject": "body__note"}, "is not a foreign key"),
        (StateRecord, {"subject_content_type": "definition", "subject_pk": "object_id"}, "foreign key to ContentType"),
        (
            StateRecord,
            {"subject_content_type": "definition__content_type", "subject_pk": "nope"},
            "has no field 'nope'",
        ),
        (Note, {"subject": "notecomment"}, "is not a column of its table"),
    ],
)
def test_an_invalid_subject_path_is_rejected_at_compile_time(model, kwargs, message):
    with pytest.raises(ValueError, match=message):
        ObjectStreamTrigger(name="probe", **kwargs).compile(model)


@pytest.mark.parametrize("model", [StateRecord, NoteComment, SlugComment, TriggeredNote])
def test_migration_state_compiles_the_same_trigger_as_the_runtime_model(model):
    """makemigrations compiles historical models; a different hash would make the trigger outdated."""
    historical = ProjectState.from_apps(apps).apps.get_model(model._meta.label)
    [trigger] = model._meta.triggers

    assert trigger.compile(historical).hash == trigger.compile(model).hash

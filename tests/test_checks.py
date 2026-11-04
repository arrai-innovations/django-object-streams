import pytest
from django.apps import apps
from django.core import checks
from django.db import connection
from django.db import router

from object_streams.checks import check_capture_triggers
from tests.testapp.models import ProtectedNote
from tests.testapp.models import TriggeredNote


def declared_trigger(model, name):
    return next(trigger for trigger in model._meta.triggers if trigger.name == name)


def capture_trigger():
    return declared_trigger(TriggeredNote, "triggered_note_stream")


def check_ids(messages):
    return [(message.id, message.obj) for message in messages]


def test_the_check_does_nothing_unless_databases_are_requested():
    assert check_capture_triggers() == []
    assert check_capture_triggers(databases=[]) == []


@pytest.mark.django_db
def test_installed_capture_triggers_pass():
    assert check_capture_triggers(databases=["default"]) == []


@pytest.mark.django_db
def test_an_uninstalled_capture_trigger_is_reported():
    capture_trigger().uninstall(TriggeredNote)

    messages = check_capture_triggers(databases=["default"])

    assert check_ids(messages) == [("object_streams.W001", TriggeredNote)]
    assert (
        messages[0].msg
        == "Capture trigger testapp.TriggeredNote:triggered_note_stream is not installed in database 'default'."
    )


@pytest.mark.django_db
def test_an_outdated_capture_trigger_is_reported():
    trigger = capture_trigger()
    with connection.cursor() as cursor:
        cursor.execute(
            f"COMMENT ON TRIGGER {trigger.get_pgid(TriggeredNote)} ON {TriggeredNote._meta.db_table} IS 'stale'"
        )

    messages = check_capture_triggers(databases=["default"])

    assert check_ids(messages) == [("object_streams.W002", TriggeredNote)]


@pytest.mark.django_db
def test_other_pgtrigger_triggers_are_not_reported():
    declared_trigger(ProtectedNote, "protected_note_delete").uninstall(ProtectedNote)

    assert check_capture_triggers(databases=["default"]) == []


@pytest.mark.django_db
def test_the_check_is_limited_to_the_requested_apps():
    capture_trigger().uninstall(TriggeredNote)

    assert check_capture_triggers(app_configs=[apps.get_app_config("auth")], databases=["default"]) == []


class NoTestappRouter:
    def allow_migrate(self, db, app_label, **hints):
        return app_label != "testapp"


@pytest.mark.django_db
def test_a_model_the_router_keeps_off_the_database_is_not_reported(monkeypatch):
    """pgtrigger reports such a trigger as unallowed rather than uninstalled."""
    capture_trigger().uninstall(TriggeredNote)
    monkeypatch.setattr(router, "routers", [NoTestappRouter()])

    assert check_capture_triggers(databases=["default"]) == []


@pytest.mark.django_db
def test_the_check_waits_while_migrations_are_unapplied(monkeypatch):
    """A pending migration may be the one that installs the trigger."""
    capture_trigger().uninstall(TriggeredNote)
    monkeypatch.setattr("object_streams.checks._has_unapplied_migrations", lambda connection: True)

    assert check_capture_triggers(databases=["default"]) == []


@pytest.mark.django_db
def test_the_check_is_registered_as_a_database_check():
    capture_trigger().uninstall(TriggeredNote)

    assert "object_streams.W001" not in [message.id for message in checks.run_checks()]
    messages = checks.run_checks(tags=[checks.Tags.database], databases=["default"])
    assert ("object_streams.W001", TriggeredNote) in check_ids(messages)

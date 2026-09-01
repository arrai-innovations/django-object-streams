from object_streams.outbox import assign_outbox_cursors
from object_streams.outbox import create_outbox_event


def create_deliverable_outbox_event(event, *, notify=False):
    """Create a row and simulate post-commit cursor assignment in transactional tests."""

    row = create_outbox_event(event, notify=notify)
    if row.cursor is None:
        assign_outbox_cursors()
        row.refresh_from_db(fields=["cursor"])
    return row

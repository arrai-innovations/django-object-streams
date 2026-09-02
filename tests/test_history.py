import pytest

from object_streams.events import SourceRef
from object_streams.history import HistoryRecordRef
from object_streams.history.pghistory import PgHistoryAdapter
from object_streams.history.simple_history import SimpleHistoryAdapter


def test_history_record_ref_maps_onto_a_source_ref():
    record = HistoryRecordRef(
        model="testapp.Note",
        pk=7,
        history_model="testapp.HistoricalNote",
        history_id=42,
    )

    assert record.as_source_ref() == SourceRef(
        model="testapp.Note",
        pk="7",
        history_model="testapp.HistoricalNote",
        history_id="42",
    )


def test_history_record_ref_carries_no_source_metadata():
    record = HistoryRecordRef(
        model="testapp.Note",
        pk="7",
        history_model="testapp.HistoricalNote",
        history_id="42",
    )

    assert record.as_source_ref().metadata == {}


@pytest.mark.parametrize("adapter_class", [PgHistoryAdapter, SimpleHistoryAdapter])
def test_history_adapter_shells_report_they_are_unimplemented(adapter_class):
    with pytest.raises(NotImplementedError, match="not implemented yet"):
        adapter_class().source_ref_for_history(object())

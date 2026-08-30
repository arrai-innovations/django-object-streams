import django_filters
import pytest

from object_streams import ObjectStreamRegistry
from object_streams.exceptions import AlreadyRegistered
from object_streams.sources import ModelSource
from tests.testapp.models import Note


class NoteFilter(django_filters.FilterSet):
    class Meta:
        model = Note
        fields = ["title"]


def test_registry_registers_model_with_filterset_and_sources():
    registry = ObjectStreamRegistry()

    registration = registry.register(
        Note,
        filterset=NoteFilter,
        sources=[ModelSource(Note, facet="object")],
    )

    assert registration.model is Note
    assert registration.model_label == "testapp.Note"
    assert registration.filterset is NoteFilter
    assert registration.facets == frozenset({"object"})
    assert registry.get(Note) is registration


def test_registry_rejects_duplicate_registration():
    registry = ObjectStreamRegistry()
    registry.register(Note)

    with pytest.raises(AlreadyRegistered):
        registry.register(Note)


def test_registration_applies_filterset_without_evaluating_queryset():
    registry = ObjectStreamRegistry()
    registration = registry.register(Note, filterset=NoteFilter)

    queryset = registration.get_queryset(user=None, filters={"title": "Draft"})

    assert queryset.model is Note
    assert "Draft" in str(queryset.query)

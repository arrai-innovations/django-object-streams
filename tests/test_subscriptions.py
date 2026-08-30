import pytest

from object_streams.subscriptions import SubscriptionKind
from object_streams.subscriptions import SubscriptionRequest


SUBSCRIPTION_CURSOR = 120000


def test_subscription_request_from_filter_message():
    request = SubscriptionRequest.from_message(
        {
            "op": "subscribe",
            "kind": "filter",
            "model": "store.CustomerOrder",
            "filter": {"status": "open"},
            "ordering": ["-updated_at", "id"],
            "cursor": SUBSCRIPTION_CURSOR,
        }
    )

    assert request.kind == SubscriptionKind.FILTER
    assert request.model == "store.CustomerOrder"
    assert request.filters == {"status": "open"}
    assert request.ordering == ("-updated_at", "id")
    assert request.cursor == SUBSCRIPTION_CURSOR
    assert request.as_dict()["filter"] == {"status": "open"}


def test_object_subscription_requires_pk():
    with pytest.raises(ValueError, match="primary key"):
        SubscriptionRequest(kind="object", model="store.CustomerOrder")

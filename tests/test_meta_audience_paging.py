"""
End to end test for the blob-native audience paging used by the Meta egress.

The test drives the real index and page reading code against an in memory
stand-in for Blob Storage, then asserts that paging the audience back out
reproduces the original MAID list exactly.

It replaces what the Synapse ``OPENROWSET`` queries used to do:
``COUNT(DISTINCT ...)`` for the batch count and an ``ORDER BY ... OFFSET ...
FETCH NEXT`` window for the pages.
"""

import sys
import types

import pytest

try:  # pragma: no cover - exercised only when the SDK is not installed
    import azure.durable_functions  # noqa: F401
except ImportError:  # pragma: no cover
    _stub = types.ModuleType("azure.durable_functions")

    class _Blueprint:
        def __init__(self, *args, **kwargs):
            pass

        def _identity(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

        activity_trigger = _identity
        orchestration_trigger = _identity
        route = _identity
        durable_client_input = _identity
        timer_trigger = _identity

    _stub.Blueprint = _Blueprint
    sys.modules["azure.durable_functions"] = _stub

from libs.azure.functions.blueprints.esquire.audiences.egress.meta.blobReader import (
    fetch_users_page,
)
from libs.azure.functions.blueprints.esquire.audiences.utils.activities import (
    indexAudienceBlobs,
)
from libs.azure.functions.blueprints.esquire.audiences.utils.audienceBlobs import (
    MAID_HEADER_BYTES,
    MAID_RECORD_BYTES,
    plan_ranges,
)

PREFIX = "audiences/aud-123/2026-11-04"


def maid(n):
    """Build a deterministic 36 character MAID."""
    return "{:08x}-0000-4000-8000-{:012x}".format(n, n)


def make_blob(ids, header=True):
    payload = b"deviceid\n" if header else b""
    for value in ids:
        payload += value.encode("utf-8") + b"\n"
    return payload


class FakeDownloader:
    def __init__(self, data):
        self._data = data

    def readall(self):
        return self._data


class FakeBlobClient:
    def __init__(self, store, name):
        self._store = store
        self._name = name

    def download_blob(self, offset=0, length=None):
        data = self._store[self._name]
        end = len(data) if length is None else offset + length
        return FakeDownloader(data[offset:end])


class FakeBlobProperties:
    def __init__(self, name, size):
        self.name = name
        self.size = size


class FakeContainerClient:
    def __init__(self, store):
        self._store = store

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def list_blobs(self, name_starts_with=None):
        for name in sorted(self._store):
            if name_starts_with and not name.startswith(name_starts_with):
                continue
            yield FakeBlobProperties(name, len(self._store[name]))


@pytest.fixture
def blob_store(monkeypatch):
    store = {}

    monkeypatch.setattr(
        indexAudienceBlobs,
        "get_container_client",
        lambda **kwargs: FakeContainerClient(store),
    )
    monkeypatch.setattr(
        indexAudienceBlobs,
        "init_blob_client",
        lambda **kwargs: FakeBlobClient(store, kwargs["blob_name"]),
    )
    monkeypatch.setattr(
        "libs.azure.functions.blueprints.esquire.audiences.egress.meta.blobReader.init_blob_client",
        lambda **kwargs: FakeBlobClient(store, kwargs["blob_name"]),
    )
    return store


def page_through(index, batch_size, total):
    """Page the index the way the orchestrator does."""
    pages = []
    for sequence in range(0, total, batch_size):
        count = min(batch_size, total - sequence)
        ranges = plan_ranges(index, sequence, count)
        users, error, _, _ = fetch_users_page(
            {
                "destination": {
                    "conn_str": "AzureWebJobsStorage",
                    "container_name": "general",
                },
                "batch": {"batch_seq": sequence // batch_size + 1},
                "batch_size": batch_size,
                "ranges": ranges,
            }
        )
        assert error is None
        pages.append(users)
    return pages


def test_index_and_page_a_single_blob(blob_store):
    ids = [maid(i) for i in range(25)]
    blob_store[PREFIX + "/final_00000.csv"] = make_blob(ids)

    index = indexAudienceBlobs.build_audience_index(
        conn_str="AzureWebJobsStorage",
        container_name="general",
        blob_prefix=PREFIX,
    )

    assert index["total"] == 25
    assert len(index["blobs"]) == 1
    assert index["blobs"][0]["data_offset"] == MAID_HEADER_BYTES

    pages = page_through(index, batch_size=10, total=index["total"])
    assert [maid for page in pages for maid in page] == ids


def test_index_and_page_multiple_blobs(blob_store):
    ids = [maid(i) for i in range(50)]
    split = 20
    blob_store[PREFIX + "/final_00000.csv"] = make_blob(ids[:split])
    blob_store[PREFIX + "/final_00001.csv"] = make_blob(ids[split:])

    index = indexAudienceBlobs.build_audience_index(
        conn_str="AzureWebJobsStorage",
        container_name="general",
        blob_prefix=PREFIX,
    )

    assert index["total"] == 50
    assert len(index["blobs"]) == 2

    # A batch size that forces pages to span the blob boundary.
    pages = page_through(index, batch_size=7, total=index["total"])
    assert [maid for page in pages for maid in page] == ids


def test_index_ignores_non_audience_blobs(blob_store):
    ids = [maid(i) for i in range(5)]
    blob_store[PREFIX + "/final_00000.csv"] = make_blob(ids)
    blob_store[PREFIX + "/scratch.csv"] = make_blob([maid(99)] * 3)

    index = indexAudienceBlobs.build_audience_index(
        conn_str="AzureWebJobsStorage",
        container_name="general",
        blob_prefix=PREFIX,
    )

    assert index["total"] == 5
    assert [entry["name"] for entry in index["blobs"]] == [PREFIX + "/final_00000.csv"]


def test_index_on_an_empty_folder(blob_store):
    index = indexAudienceBlobs.build_audience_index(
        conn_str="AzureWebJobsStorage",
        container_name="general",
        blob_prefix=PREFIX,
    )

    assert index["total"] == 0
    assert index["blobs"] == []


def test_index_rejects_an_inconsistent_record_grid(blob_store):
    blob_store[PREFIX + "/final_00000.csv"] = make_blob([maid(1)])
    # A blob whose size is not a whole number of 37 byte records.
    blob_store[PREFIX + "/final_00001.csv"] = b"deviceid\n" + b"x" * 40

    with pytest.raises(ValueError):
        indexAudienceBlobs.build_audience_index(
            conn_str="AzureWebJobsStorage",
            container_name="general",
            blob_prefix=PREFIX,
        )


def test_page_read_is_bounded_to_the_requested_range(blob_store):
    """The reader must only pull the bytes belonging to the requested page."""
    ids = [maid(i) for i in range(100)]
    blob_store[PREFIX + "/final_00000.csv"] = make_blob(ids)

    index = indexAudienceBlobs.build_audience_index(
        conn_str="AzureWebJobsStorage",
        container_name="general",
        blob_prefix=PREFIX,
    )

    ranges = plan_ranges(index, 10, 5)
    assert ranges[0]["length"] == 5 * MAID_RECORD_BYTES

    users, error, offset, limit = fetch_users_page(
        {
            "destination": {
                "conn_str": "AzureWebJobsStorage",
                "container_name": "general",
            },
            "batch": {"batch_seq": 2},
            "batch_size": 5,
            "ranges": ranges,
        }
    )

    assert error is None
    assert users == ids[10:15]
    assert offset == 5
    assert limit == 5


def test_batch_count_and_paged_records_stay_in_agreement(blob_store):
    """The count used for the batch loop must match the records actually read.

    If they disagree the final batch never carries ``last_batch_flag`` and the
    Meta replace session never closes, so this invariant is asserted directly.
    """
    import math

    batch_size = 10
    ids = [maid(i) for i in range(25007 % 1000 + 97)]
    blob_store[PREFIX + "/final_00000.csv"] = make_blob(ids)

    index = indexAudienceBlobs.build_audience_index(
        conn_str="AzureWebJobsStorage",
        container_name="general",
        blob_prefix=PREFIX,
    )
    total = index["total"]
    assert total == len(ids)

    seen = []
    last_flags = []
    for sequence, _ in enumerate(range(0, total, batch_size)):
        is_last = (sequence + 1) == math.ceil(total / batch_size)
        last_flags.append(is_last)

        count = min(batch_size, total - (sequence * batch_size))
        ranges = plan_ranges(index, sequence * batch_size, count)
        users, error, _, _ = fetch_users_page(
            {
                "destination": {
                    "conn_str": "AzureWebJobsStorage",
                    "container_name": "general",
                },
                "batch": {"batch_seq": sequence + 1},
                "batch_size": batch_size,
                "ranges": ranges,
            }
        )
        assert error is None
        seen.extend(users)

    # Every record read exactly once, in order.
    assert seen == ids
    # Exactly one batch is flagged as the last one, and it is the final batch.
    assert last_flags[-1] is True
    assert sum(last_flags) == 1
    assert all(flag is False for flag in last_flags[:-1])

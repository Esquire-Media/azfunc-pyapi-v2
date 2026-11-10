"""
Tests for the device observation DISTINCT activity.

The activity replaces the Synapse ``CREATE EXTERNAL TABLE ... AS SELECT
DISTINCT`` that used to extract the unique device ids from the observation
files. These tests cover the CSV parsing, which is where the behaviour could
silently diverge from the Synapse query.
"""

from libs.azure.functions.blueprints.esquire.reporting.location_insights.activities import (
    unique_device_ids,
)


class FakeDownloader:
    def __init__(self, data: bytes):
        self._data = data

    def chunks(self):
        # Deliberately awkward chunk boundaries, to prove the reader handles a
        # chunk landing in the middle of a row.
        size = 7
        for index in range(0, len(self._data), size):
            yield self._data[index : index + size]

    def readall(self):
        return self._data


class FakeBlob:
    def __init__(self, data: bytes):
        self._data = data

    def download_blob(self, offset=0, length=None):
        if length is None:
            return FakeDownloader(self._data[offset:])
        return FakeDownloader(self._data[offset : offset + length])


def test_parses_rows_from_a_chunked_stream():
    payload = b"deviceid,timestamp,lat,lng\naaa,1,2.0,3.0\nbbb,2,4.0,5.0\n"

    rows = list(unique_device_ids._iter_rows(FakeDownloader(payload).chunks()))

    assert rows == [
        ["deviceid", "timestamp", "lat", "lng"],
        ["aaa", "1", "2.0", "3.0"],
        ["bbb", "2", "4.0", "5.0"],
    ]


def test_extracts_the_device_id_column():
    payload = b"deviceid,timestamp,lat,lng\naaa,1,2.0,3.0\nbbb,2,4.0,5.0\n"

    ids = list(unique_device_ids._iter_device_ids(FakeBlob(payload)))

    assert ids == ["aaa", "bbb"]


def test_finds_the_device_id_column_when_it_is_not_first():
    payload = b"timestamp,deviceid,lat,lng\n1,aaa,2.0,3.0\n"

    ids = list(unique_device_ids._iter_device_ids(FakeBlob(payload)))

    assert ids == ["aaa"]


def test_skips_rows_without_a_device_id():
    payload = b"deviceid,timestamp\n,1\naaa,2\n"

    ids = list(unique_device_ids._iter_device_ids(FakeBlob(payload)))

    assert ids == ["aaa"]


def test_handles_an_empty_blob():
    assert list(unique_device_ids._iter_device_ids(FakeBlob(b""))) == []


def test_handles_a_header_only_blob():
    payload = b"deviceid,timestamp,lat,lng\n"

    assert list(unique_device_ids._iter_device_ids(FakeBlob(payload))) == []


def test_detects_a_header_row():
    blob = FakeBlob(b"deviceid,timestamp,lat,lng\naaa,1,2.0,3.0\n")

    assert unique_device_ids._blob_has_header(blob) is True


def test_detects_a_missing_header_row():
    blob = FakeBlob(b"aaa,1,2.0,3.0\nbbb,2,4.0,5.0\n")

    assert unique_device_ids._blob_has_header(blob) is False


def test_parses_quoted_fields():
    payload = b'deviceid,note\naaa,"a, b"\n'

    rows = list(unique_device_ids._iter_rows(FakeDownloader(payload).chunks()))

    assert rows == [["deviceid", "note"], ["aaa", "a, b"]]

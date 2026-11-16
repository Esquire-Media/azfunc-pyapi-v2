"""
Tests for the blob-native audience paging helpers.

These cover the pure logic that replaced the Synapse ``OPENROWSET`` queries in
the Meta audience egress: the record grid arithmetic, the mapping from global
record indices onto blob byte ranges, and the guard that proves a read range
is correctly aligned.

The helpers are deliberately dependency free (stdlib only) so they can be
tested without the Azure SDK installed.
"""

import pytest

from libs.azure.functions.blueprints.esquire.audiences.utils.audienceBlobs import (
    MAID_HEADER_BYTES,
    MAID_LENGTH,
    MAID_RECORD_BYTES,
    choose_data_offset,
    detect_data_offset,
    parse_records,
    plan_ranges,
)

MAID_A = "01234567-89ab-cdef-0123-456789abcdef"
MAID_B = "11111111-2222-3333-4444-555555555555"
MAID_C = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def make_blob(ids, header=True):
    """Build the byte layout the audience builder produces."""
    payload = b"deviceid\n" if header else b""
    for maid in ids:
        payload += maid.encode("utf-8") + b"\n"
    return payload


def make_index(blobs):
    """Build an index the same way the index activity does."""
    entries = []
    total = 0
    for name, size in blobs:
        record_count = (size - MAID_HEADER_BYTES) // MAID_RECORD_BYTES
        entries.append(
            {
                "name": name,
                "size": size,
                "data_offset": MAID_HEADER_BYTES,
                "record_count": record_count,
            }
        )
        total += record_count
    return {"blobs": entries, "total": total, "record_bytes": MAID_RECORD_BYTES}


# --------------------------------------------------------------------------
# detect_data_offset
# --------------------------------------------------------------------------


def test_detect_data_offset_with_header():
    assert detect_data_offset(make_blob([MAID_A])) == MAID_HEADER_BYTES


def test_detect_data_offset_without_header():
    assert detect_data_offset(make_blob([MAID_A], header=False)) == 0


def test_detect_data_offset_empty_blob():
    assert detect_data_offset(b"") == 0


def test_detect_data_offset_headerless_first_line_is_a_maid():
    # A MAID is a UUID and can never contain "deviceid".
    assert detect_data_offset(MAID_A.encode() + b"\n") == 0


# --------------------------------------------------------------------------
# choose_data_offset
# --------------------------------------------------------------------------


def test_choose_data_offset_prefers_the_probed_value():
    sizes = [MAID_HEADER_BYTES + MAID_RECORD_BYTES * 3]
    assert choose_data_offset(sizes, MAID_HEADER_BYTES) == MAID_HEADER_BYTES


def test_choose_data_offset_falls_back_to_zero():
    sizes = [MAID_RECORD_BYTES * 3]
    assert choose_data_offset(sizes, None) == 0


def test_choose_data_offset_returns_none_for_an_inconsistent_grid():
    sizes = [MAID_HEADER_BYTES + MAID_RECORD_BYTES, MAID_RECORD_BYTES * 2]
    assert choose_data_offset(sizes, MAID_HEADER_BYTES) is None


def test_choose_data_offset_with_no_blobs():
    assert choose_data_offset([], None) == 0


# --------------------------------------------------------------------------
# plan_ranges
# --------------------------------------------------------------------------


def test_plan_ranges_within_a_single_blob():
    index = make_index(
        [("final_00000.csv", MAID_HEADER_BYTES + MAID_RECORD_BYTES * 10)]
    )

    ranges = plan_ranges(index, 0, 3)

    assert ranges == [
        {
            "blob_name": "final_00000.csv",
            "offset": MAID_HEADER_BYTES,
            "length": 3 * MAID_RECORD_BYTES,
            "records": 3,
        }
    ]


def test_plan_ranges_offsets_by_record_index():
    index = make_index(
        [("final_00000.csv", MAID_HEADER_BYTES + MAID_RECORD_BYTES * 10)]
    )

    ranges = plan_ranges(index, 4, 2)

    assert ranges[0]["offset"] == MAID_HEADER_BYTES + 4 * MAID_RECORD_BYTES
    assert ranges[0]["records"] == 2


def test_plan_ranges_spanning_two_blobs():
    index = make_index(
        [
            ("final_00000.csv", MAID_HEADER_BYTES + MAID_RECORD_BYTES * 5),
            ("final_00001.csv", MAID_HEADER_BYTES + MAID_RECORD_BYTES * 5),
        ]
    )

    ranges = plan_ranges(index, 3, 4)

    assert len(ranges) == 2
    assert ranges[0]["blob_name"] == "final_00000.csv"
    assert ranges[0]["records"] == 2
    assert ranges[0]["offset"] == MAID_HEADER_BYTES + 3 * MAID_RECORD_BYTES
    assert ranges[1]["blob_name"] == "final_00001.csv"
    assert ranges[1]["records"] == 2
    assert ranges[1]["offset"] == MAID_HEADER_BYTES


def test_plan_ranges_on_an_exact_blob_boundary():
    index = make_index(
        [
            ("final_00000.csv", MAID_HEADER_BYTES + MAID_RECORD_BYTES * 5),
            ("final_00001.csv", MAID_HEADER_BYTES + MAID_RECORD_BYTES * 5),
        ]
    )

    ranges = plan_ranges(index, 5, 3)

    assert len(ranges) == 1
    assert ranges[0]["blob_name"] == "final_00001.csv"
    assert ranges[0]["offset"] == MAID_HEADER_BYTES
    assert ranges[0]["records"] == 3


def test_plan_ranges_covers_every_record_exactly_once():
    blob_a = make_blob([MAID_A] * 4)
    blob_b = make_blob([MAID_B] * 6)
    index = make_index(
        [("final_00000.csv", len(blob_a)), ("final_00001.csv", len(blob_b))]
    )

    seen = []
    for start in range(0, index["total"], 2):
        for entry in plan_ranges(index, start, 2):
            seen.append((entry["blob_name"], entry["offset"], entry["records"]))

    # Every record of every blob is claimed exactly once.
    assert sum(entry[2] for entry in seen) == index["total"] == 10
    assert len(seen) == 5


def test_plan_ranges_beyond_the_end_raises():
    index = make_index([("final_00000.csv", MAID_HEADER_BYTES + MAID_RECORD_BYTES * 2)])

    with pytest.raises(ValueError):
        plan_ranges(index, 0, 5)


def test_plan_ranges_with_zero_records_returns_nothing():
    index = make_index([("final_00000.csv", MAID_HEADER_BYTES + MAID_RECORD_BYTES * 2)])

    assert plan_ranges(index, 0, 0) == []


# --------------------------------------------------------------------------
# parse_records
# --------------------------------------------------------------------------


def test_parse_records_round_trip():
    payload = make_blob([MAID_A, MAID_B, MAID_C])[MAID_HEADER_BYTES:]

    assert parse_records(payload, 3) == [MAID_A, MAID_B, MAID_C]


def test_parse_records_with_no_expected_records():
    assert parse_records(b"", 0) == []


def test_parse_records_rejects_unexpected_bytes():
    payload = make_blob([MAID_A, MAID_B])[MAID_HEADER_BYTES:]

    with pytest.raises(ValueError):
        parse_records(payload, 3)


def test_parse_records_rejects_a_short_payload():
    payload = make_blob([MAID_A, MAID_B])[MAID_HEADER_BYTES:]

    with pytest.raises(ValueError):
        parse_records(payload, 1)


def test_parse_records_rejects_a_misaligned_grid():
    # Shift the grid by one byte: the records no longer split cleanly.
    payload = make_blob([MAID_A, MAID_B])[MAID_HEADER_BYTES:]

    with pytest.raises(ValueError):
        parse_records(payload[1:], 2)


def test_parse_records_rejects_a_record_of_the_wrong_length():
    payload = b"short\n"

    with pytest.raises(ValueError):
        parse_records(payload, 1)


def test_parse_records_accepts_any_36_character_identifier():
    # The builder only guarantees a length of 36, not a specific charset.
    value = "z" * MAID_LENGTH
    payload = value.encode("utf-8") + b"\n"

    assert parse_records(payload, 1) == [value]


def test_parse_records_rejects_a_payload_not_ending_on_a_boundary():
    payload = make_blob([MAID_A, MAID_B])[MAID_HEADER_BYTES:]

    with pytest.raises(ValueError):
        parse_records(payload[:-1], 2)

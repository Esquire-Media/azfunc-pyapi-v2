"""
Blob-native paging helpers for the Meta audience egress.

The audience builder (``activity_esquireAudienceBuilder_finalize``) writes one
MAID per line, already normalized to exactly 36 characters, preceded by a
``deviceid`` header row::

    deviceid\\n
    01234567-89ab-cdef-0123-456789abcdef\\n
    ...

Because every MAID is exactly 36 characters, every record is exactly
``MAID_RECORD_BYTES`` (37) bytes on disk. That makes the record grid
byte-addressable: record ``k`` of a blob lives at byte
``data_offset + k * MAID_RECORD_BYTES``. Paging therefore reduces to a plain
Azure Blob range read, which keeps the Function App's memory footprint flat
(O(batch_size)) regardless of how large the audience is.

This module is intentionally dependency-free (stdlib only) so that importing
it does not pull pandas, sqlalchemy or any database driver into the worker.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

# Number of characters in a normalized MAID.
MAID_LENGTH = 36

# Bytes per record on disk: the MAID plus its trailing newline.
MAID_RECORD_BYTES = MAID_LENGTH + 1

# Bytes taken by the builder's header row ("deviceid\n").
MAID_HEADER_BYTES = len(b"deviceid\n")

# Column names the builder (or its predecessors) may have used for the header.
_HEADER_HINTS = (b"deviceid", b"device_id")


def detect_data_offset(head: bytes) -> int:
    """
    Determine the byte offset at which MAID records begin within a blob.

    Parameters
    ----------
    head : bytes
        The first bytes of the blob (a few hundred bytes is plenty).

    Returns
    -------
    int
        The number of bytes to skip before the first MAID record. ``0`` when
        the blob has no recognizable header row.

    Notes
    -----
    A MAID is a UUID, so it can never contain the substring ``deviceid``; the
    header detection is therefore unambiguous.
    """
    newline = head.find(b"\n")
    if newline == -1:
        # No complete line in the probe window; treat the blob as headerless.
        return 0

    first_line = head[:newline].lower()
    if any(hint in first_line for hint in _HEADER_HINTS):
        return newline + 1

    return 0


def choose_data_offset(
    sizes: Sequence[int],
    probed_offset: Optional[int] = None,
) -> Optional[int]:
    """
    Pick the header size that yields a consistent record grid across blobs.

    Parameters
    ----------
    sizes : Sequence[int]
        Size (in bytes) of each candidate blob.
    probed_offset : Optional[int]
        Offset detected by :func:`detect_data_offset` from the first blob.

    Returns
    -------
    Optional[int]
        The offset to use, or ``None`` when no candidate divides every blob
        evenly into 37 byte records.

    Notes
    -----
    At most one candidate can satisfy ``(size - offset) % 37 == 0`` for a
    given size, because ``37`` does not divide ``8``. A candidate therefore
    either works for every blob or the grid is inconsistent.
    """
    if not sizes:
        return 0

    candidates: List[int] = []
    if probed_offset is not None:
        candidates.append(probed_offset)
    candidates.append(0)
    candidates.append(MAID_HEADER_BYTES)

    seen = set()
    for offset in candidates:
        if offset in seen:
            continue
        seen.add(offset)
        if all(
            size >= offset and (size - offset) % MAID_RECORD_BYTES == 0
            for size in sizes
        ):
            return offset

    return None


def plan_ranges(
    index: Dict[str, Any],
    record_start: int,
    record_count: int,
) -> List[Dict[str, Any]]:
    """
    Map a range of global record indices onto blob byte ranges.

    Parameters
    ----------
    index : Dict[str, Any]
        The audience index produced by the index activity. Its ``blobs`` list
        must be ordered and each entry must carry ``name``, ``data_offset``
        and ``record_count``.
    record_start : int
        Zero-based index of the first record to read.
    record_count : int
        Number of records to read.

    Returns
    -------
    List[Dict[str, Any]]
        One entry per blob touched, with ``blob_name``, ``offset``, ``length``
        and ``records``.

    Notes
    -----
    The function is pure, which keeps it safe to call from a Durable
    orchestrator: replaying the orchestration reproduces the same ranges.
    """
    if record_count <= 0:
        return []

    ranges: List[Dict[str, Any]] = []
    remaining = record_count
    cursor = record_start

    records_before = 0
    for blob in index.get("blobs", []):
        count = int(blob["record_count"])
        blob_end = records_before + count

        if cursor >= blob_end:
            records_before = blob_end
            continue

        take = min(remaining, blob_end - cursor)
        local = cursor - records_before
        ranges.append(
            {
                "blob_name": blob["name"],
                "offset": int(blob["data_offset"]) + local * MAID_RECORD_BYTES,
                "length": take * MAID_RECORD_BYTES,
                "records": take,
            }
        )

        cursor += take
        remaining -= take
        records_before = blob_end

        if remaining <= 0:
            break

    if remaining > 0:
        raise ValueError(
            "Requested {requested} records starting at {start}, but the "
            "audience index only holds {available} records.".format(
                requested=record_count,
                start=record_start,
                available=record_start + remaining,
            )
        )

    return ranges


def parse_records(payload: bytes, expected: int) -> List[str]:
    """
    Decode a range of MAID records into a list of MAID strings.

    Parameters
    ----------
    payload : bytes
        Raw bytes read from a blob range.
    expected : int
        Number of records the range is expected to contain.

    Returns
    -------
    List[str]
        The MAIDs, in file order.

    Raises
    ------
    ValueError
        When the payload is not an exact, record-aligned multiple of
        ``MAID_RECORD_BYTES`` or when any record is not exactly
        ``MAID_LENGTH`` characters.

    Notes
    -----
    The per-record length check doubles as a proof that the byte grid is
    aligned: shifting the grid by any amount other than a whole record
    necessarily splits at least one record into pieces that are not 36
    characters long.
    """
    if expected == 0:
        if payload:
            raise ValueError(
                "Expected no records but received {n} bytes.".format(n=len(payload))
            )
        return []

    if len(payload) != expected * MAID_RECORD_BYTES:
        raise ValueError(
            "Expected {expected} records ({expected_bytes} bytes) but received "
            "{received} bytes; the audience record grid is not aligned.".format(
                expected=expected,
                expected_bytes=expected * MAID_RECORD_BYTES,
                received=len(payload),
            )
        )

    text = payload.decode("utf-8")
    if not text.endswith("\n"):
        raise ValueError("Audience range does not end on a record boundary.")

    lines = text.split("\n")
    # ``split`` on a trailing newline yields a final empty element.
    lines.pop()

    if len(lines) != expected:
        raise ValueError(
            "Expected {expected} records but decoded {decoded}.".format(
                expected=expected, decoded=len(lines)
            )
        )

    for line in lines:
        if len(line) != MAID_LENGTH:
            raise ValueError(
                "Audience record has length {length}, expected {expected}. "
                "The audience record grid is not aligned.".format(
                    length=len(line), expected=MAID_LENGTH
                )
            )

    return lines

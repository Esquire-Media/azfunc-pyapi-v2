"""
Bounded-memory external sort helpers.

Used where a value has to be de-duplicated from a stream that is too large to
hold in memory. Lines are accumulated into fixed size chunks, each chunk is
sorted and spilled to a temporary file, and the runs are then merged with a
bounded fan-in so that the number of open file handles stays small.

The peak memory cost is governed by ``chunk_size`` and is independent of the
size of the input.
"""

from __future__ import annotations

import heapq
import os
import shutil
import tempfile
from typing import Iterable, Iterator, List, Optional

# Number of lines held in memory before a chunk is sorted and spilled.
DEFAULT_CHUNK_SIZE = int(os.environ.get("EXTERNAL_SORT_CHUNK_SIZE", "200000"))

# Maximum number of run files merged in one pass.
DEFAULT_MAX_OPEN_RUNS = int(os.environ.get("EXTERNAL_SORT_MAX_OPEN_RUNS", "64"))


def _write_run(lines: List[str], path: str) -> None:
    """Write a sorted chunk of lines to a run file."""
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(lines))
        handle.write("\n")


def _read_run(path: str) -> Iterator[str]:
    """Yield the lines of a run file, without their trailing newlines."""
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            yield line.rstrip("\n")


def _merge_to_file(runs: List[str], path: str) -> None:
    """Merge sorted run files into a single sorted file."""
    with open(path, "w", encoding="utf-8", newline="\n") as out:
        for line in heapq.merge(*[_read_run(run) for run in runs]):
            out.write(line)
            out.write("\n")


def _reduce_runs(
    runs: List[str], temp_dir: str, max_open_runs: int, counter: int
) -> "tuple[List[str], int]":
    """
    Merge runs until few enough remain to merge in a single pass.

    Merging every run at once would need one open file handle per run, which
    does not scale. Merging groups of ``max_open_runs`` at a time keeps the
    handle count bounded regardless of how large the input was.

    The counter keeps the merged file names unique across passes, so a merged
    file can never overwrite one of its own inputs.
    """
    while len(runs) > max_open_runs:
        merged: List[str] = []
        for index in range(0, len(runs), max_open_runs):
            group = runs[index : index + max_open_runs]
            if len(group) == 1:
                merged.append(group[0])
                continue

            path = os.path.join(temp_dir, "merge_{0}".format(counter))
            counter += 1
            _merge_to_file(group, path)
            for run in group:
                os.remove(run)
            merged.append(path)
        runs = merged
    return runs, counter


def unique_sorted_lines(
    lines: Iterable[str],
    temp_dir: Optional[str] = None,
    chunk_size: Optional[int] = None,
    max_open_runs: Optional[int] = None,
) -> Iterator[str]:
    """
    Yield the sorted, de-duplicated, non-empty lines of an iterable.

    Parameters
    ----------
    lines : Iterable[str]
        The lines to sort and de-duplicate. Trailing newlines are stripped.
    temp_dir : Optional[str]
        Directory used for the intermediate run files. A temporary directory
        is created and removed automatically when omitted.
    chunk_size : Optional[int]
        Number of lines held in memory at once. This is the knob that bounds
        the memory footprint.
    max_open_runs : Optional[int]
        Maximum number of run files merged in a single pass.

    Yields
    ------
    str
        Each distinct line, in ascending order.

    Notes
    -----
    The output is sorted, which is what makes the de-duplication possible
    without holding every distinct value in memory.
    """
    chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
    max_open_runs = max_open_runs or DEFAULT_MAX_OPEN_RUNS

    owns_temp_dir = temp_dir is None
    if owns_temp_dir:
        temp_dir = tempfile.mkdtemp(prefix="external_sort_")

    created: List[str] = []

    try:
        runs: List[str] = []
        chunk: List[str] = []
        counter = 0

        for line in lines:
            line = line.rstrip("\n")
            if not line:
                continue
            chunk.append(line)
            if len(chunk) >= chunk_size:
                chunk.sort()
                path = os.path.join(temp_dir, "run_{0}".format(counter))
                counter += 1
                _write_run(chunk, path)
                created.append(path)
                runs.append(path)
                chunk = []

        if chunk:
            chunk.sort()
            path = os.path.join(temp_dir, "run_{0}".format(counter))
            counter += 1
            _write_run(chunk, path)
            created.append(path)
            runs.append(path)

        # Release the last chunk before the merge allocates its own buffers.
        chunk = None  # type: ignore[assignment]

        if not runs:
            return

        runs, counter = _reduce_runs(runs, temp_dir, max_open_runs, counter)
        created = [run for run in created if os.path.exists(run)]

        previous = None
        for line in heapq.merge(*[_read_run(run) for run in runs]):
            if line != previous:
                yield line
                previous = line
    finally:
        if owns_temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)
        else:
            for path in created:
                try:
                    os.remove(path)
                except OSError:
                    pass

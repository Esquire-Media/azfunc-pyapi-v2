"""
Tests for the bounded-memory external sort.

The sort is what lets the device observation DISTINCT run without holding
every distinct value in memory, so these tests pin the ordering, the
de-duplication and the chunk spilling behaviour.
"""

import os

from libs.utils.external_sort import unique_sorted_lines


def test_sorts_and_deduplicates():
    lines = ["c", "a", "b", "a", "c", "d"]

    assert list(unique_sorted_lines(lines)) == ["a", "b", "c", "d"]


def test_preserves_every_distinct_value():
    lines = ["device-{}".format(i % 500) for i in range(10_000)]

    result = list(unique_sorted_lines(lines))

    assert len(result) == 500
    assert result == sorted(set(lines))


def test_strips_trailing_newlines_and_drops_empty_lines():
    lines = ["b\n", "a\n", "", "\n", "c"]

    assert list(unique_sorted_lines(lines)) == ["a", "b", "c"]


def test_handles_an_empty_input():
    assert list(unique_sorted_lines([])) == []


def test_spills_to_multiple_runs_and_still_sorts():
    """A small chunk forces many runs, which is where the merge matters."""
    lines = ["value-{:05d}".format(i % 997) for i in range(5_000)]

    result = list(unique_sorted_lines(lines, chunk_size=100))

    assert result == sorted(set(lines))
    assert len(result) == 997


def test_merge_is_bounded_when_there_are_many_runs():
    """Runs are merged in groups so the open file handle count stays bounded."""
    lines = ["v-{:05d}".format(i) for i in range(2_000)]

    result = list(unique_sorted_lines(lines, chunk_size=10, max_open_runs=4))

    assert result == sorted(lines)


def test_deduplicates_across_run_boundaries():
    """The same value in two different runs must collapse to one."""
    lines = ["dup"] * 100 + ["other"]

    result = list(unique_sorted_lines(lines, chunk_size=10))

    assert result == ["dup", "other"]


def test_uses_the_supplied_temp_directory_and_cleans_up():
    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
        result = list(unique_sorted_lines(["b", "a"], temp_dir=temp_dir))

        assert result == ["a", "b"]
        # The run files are removed, the caller owned directory is kept.
        assert os.listdir(temp_dir) == []

"""The resume decision is a pure function, so it is easy to test."""

import pytest

from hopsync.core.resume import make_meta, meta_path, part_path, resume_offset

EXPECTED = make_meta("hpc", "data/x.bin", 1000, 1700000000)


def test_names():
    assert part_path("a/b.bin") == "a/b.bin.hopsync-part"
    assert meta_path("a/b.bin") == "a/b.bin.hopsync-part.json"


@pytest.mark.parametrize(
    "saved, part_size, result",
    [
        (EXPECTED, 400, 300),  # match: resume, one chunk (100) back
        (EXPECTED, 1000, 900),  # complete part: just re-copies the tail
        (EXPECTED, 100, 0),  # part no bigger than the rewind
        (EXPECTED, 50, 0),  # part smaller than the rewind: clamps to 0
        (EXPECTED, 0, 0),
        (EXPECTED, 1001, 0),  # part bigger than source
        (EXPECTED, None, 0),  # no part file
        (None, 400, 0),  # no sidecar / bad JSON
        (make_meta("other", "data/x.bin", 1000, 1700000000), 400, 0),
        (make_meta("hpc", "data/y.bin", 1000, 1700000000), 400, 0),
        (make_meta("hpc", "data/x.bin", 999, 1700000000), 400, 0),
        (make_meta("hpc", "data/x.bin", 1000, 1700000001), 400, 0),
        ({**EXPECTED, "version": 2}, 400, 0),
    ],
)
def test_resume_offset(saved, part_size, result):
    assert resume_offset(saved, EXPECTED, part_size, rewind=100) == result

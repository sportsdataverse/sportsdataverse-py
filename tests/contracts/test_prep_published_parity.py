import polars as pl

from tools.validation.checks import prep_published_parity
from tools.validation.findings import Severity


def test_matching_frames_yield_no_findings():
    prep = pl.DataFrame({"game_id": [1, 2], "epa": [0.1, 0.2]})
    published = pl.DataFrame({"game_id": [1, 2], "epa": [0.1, 0.2]})
    assert prep_published_parity.run("d", prep, published, ("game_id",), "nfl") == []


def test_dropped_row_is_error():
    prep = pl.DataFrame({"game_id": [1, 2], "epa": [0.1, 0.2]})
    published = pl.DataFrame({"game_id": [1], "epa": [0.1]})  # game 2 dropped
    findings = prep_published_parity.run("d", prep, published, ("game_id",), "nfl")
    assert any(f.severity is Severity.ERROR and "dropped" in f.message for f in findings)


def test_value_divergence_is_error():
    prep = pl.DataFrame({"game_id": [1, 2], "epa": [0.1, 0.2]})
    published = pl.DataFrame({"game_id": [1, 2], "epa": [0.1, 0.9]})  # game 2 epa diverges
    findings = prep_published_parity.run("d", prep, published, ("game_id",), "nfl")
    assert any(f.severity is Severity.ERROR and "epa" in f.message for f in findings)


def test_null_in_one_side_is_divergence():
    prep = pl.DataFrame({"game_id": [1, 2], "epa": [0.1, 0.2]})
    published = pl.DataFrame({"game_id": [1, 2], "epa": [0.1, None]})  # game 2 epa dropped to null
    findings = prep_published_parity.run("d", prep, published, ("game_id",), "nfl")
    assert any(f.severity is Severity.ERROR and "epa" in f.message for f in findings)


def test_within_tolerance_yields_no_findings():
    # diff of 5e-7 is below the default tolerance of 1e-6 — must not be reported
    prep = pl.DataFrame({"game_id": [1, 2], "epa": [0.1, 0.2]})
    published = pl.DataFrame({"game_id": [1, 2], "epa": [0.1, 0.2 + 5e-7]})
    assert prep_published_parity.run("d", prep, published, ("game_id",), "nfl", tolerance=1e-6) == []


def test_suffix_collision_still_reports_divergence():
    # published already carries "epa__pub__" (the join suffix).  Without the
    # collision guard this raises DuplicateError; with it, divergence in "epa"
    # must still be detected correctly.
    prep = pl.DataFrame({"game_id": [1, 2], "epa": [0.1, 0.2]})
    published = pl.DataFrame({"game_id": [1, 2], "epa": [0.1, 0.9], "epa__pub__": [999.0, 999.0]})
    findings = prep_published_parity.run("d", prep, published, ("game_id",), "nfl")
    assert any(f.severity is Severity.ERROR and "epa" in f.message for f in findings)

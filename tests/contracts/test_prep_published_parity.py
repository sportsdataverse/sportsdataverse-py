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

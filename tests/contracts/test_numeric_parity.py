import polars as pl

from tools.validation.checks import numeric_parity
from tools.validation.findings import CheckContext, Severity


def test_prob_group_not_summing_to_one_is_error():
    frame = pl.DataFrame({"p_a": [0.5, 0.5], "p_b": [0.4, 0.5]})  # row 0 sums to 0.9
    ctx = CheckContext(domain="nfl", dataset="nfl_pbp", schema={}, prob_groups=(("p_a", "p_b"),))
    findings = numeric_parity.run("nfl_pbp", frame, ctx)
    assert any(f.severity is Severity.ERROR and "sum != 1" in f.message for f in findings)


def test_out_of_range_is_error():
    frame = pl.DataFrame({"wp": [0.5, 1.4]})  # wp must be in [0,1]
    ctx = CheckContext(domain="nfl", dataset="nfl_pbp", schema={}, range_constraints={"wp": (0.0, 1.0)})
    findings = numeric_parity.run("nfl_pbp", frame, ctx)
    assert any(f.severity is Severity.ERROR and "out of range" in f.message for f in findings)


def test_clean_frame_yields_no_findings():
    frame = pl.DataFrame({"p_a": [0.5, 0.5], "p_b": [0.5, 0.5], "wp": [0.2, 0.8]})
    ctx = CheckContext(
        domain="nfl", dataset="nfl_pbp", schema={}, prob_groups=(("p_a", "p_b"),), range_constraints={"wp": (0.0, 1.0)}
    )
    assert numeric_parity.run("nfl_pbp", frame, ctx) == []

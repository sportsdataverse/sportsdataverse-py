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


def test_oracle_corr_resolves_suffixed_column_under_asymmetric_map():
    class _FakeOracle:
        domain = "nfl"
        column_map = {"qb_epa": "epa"}
        thresholds = {"qb_epa": 0.99}

        def reference_frame(self, dataset, keys):
            return pl.DataFrame({"game_id": [1, 2, 3], "epa": [1.0, 2.0, 3.0]})

    frame = pl.DataFrame(
        {
            "game_id": [1, 2, 3],
            "qb_epa": [1.0, 2.0, 3.0],  # == oracle epa -> corr 1.0 -> NO warn (when resolved correctly)
            "epa": [3.0, 2.0, 1.0],  # left column; corr(qb_epa, left epa) = -1.0 -> WOULD warn if mis-resolved
        }
    )
    ctx = CheckContext(domain="nfl", dataset="nfl_pbp", schema={}, join_keys=("game_id",), oracle=_FakeOracle())
    findings = numeric_parity.run("nfl_pbp", frame, ctx)
    assert [f for f in findings if f.locator.get("column") == "qb_epa"] == []

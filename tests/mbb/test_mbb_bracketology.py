"""Tests for bracketology (``mbb_bracketology``)."""

import polars as pl

from sportsdataverse.mbb.mbb_bracketology import _conference_auto_bids, project_bracket


def _resume(n: int = 70) -> pl.DataFrame:
    # strictly descending quality: t00 is the best resume
    return pl.DataFrame(
        {
            "season": [2024] * n,
            "team_id": [f"t{i:02d}" for i in range(n)],
            "adj_em_z": [3.0 - 0.06 * i for i in range(n)],
            "sos": [10.0 - 0.2 * i for i in range(n)],
            "wab": [8.0 - 0.2 * i for i in range(n)],
            "quad1_w": [float(max(0, 12 - i // 3)) for i in range(n)],
        }
    )


def test_exactly_68_bids():
    out = project_bracket(_resume(), auto_bids={"t69"})
    assert out.filter(pl.col("bid") == True).height == 68  # noqa: E712


def test_auto_bid_always_in_even_when_weak():
    out = project_bracket(_resume(), auto_bids={"t69"})
    worst = out.filter(pl.col("team_id") == "t69").row(0, named=True)
    assert worst["auto_bid"] is True
    assert worst["bid"] is True


def test_top_four_resumes_get_seed_one():
    out = project_bracket(_resume(), auto_bids={"t69"})
    top4 = out.sort("resume_score", descending=True).head(4)
    assert top4["projected_seed"].to_list() == [1, 1, 1, 1]


def test_seeds_capped_at_16_and_null_outside_field():
    out = project_bracket(_resume(), auto_bids={"t69"})
    in_field = out.filter(pl.col("bid") == True)  # noqa: E712
    assert in_field["projected_seed"].max() == 16
    assert out.filter(pl.col("bid") == False)["projected_seed"].null_count() == 2  # noqa: E712


def test_at_large_prob_splits_field_at_half():
    out = project_bracket(_resume(), auto_bids={"t69"})
    at_large_in = out.filter((pl.col("bid") == True) & (pl.col("auto_bid") == False))  # noqa: E712
    left_out = out.filter(pl.col("bid") == False)  # noqa: E712
    assert (at_large_in["at_large_prob"] > 0.5).all()
    assert (left_out["at_large_prob"] < 0.5).all()


def test_resume_score_monotone_in_inputs():
    out = project_bracket(_resume(), auto_bids=set())
    ranked = out.sort("resume_score", descending=True)["team_id"].to_list()
    assert ranked[0] == "t00"
    assert ranked[-1] == "t69"


def _standings() -> pl.DataFrame:
    # long-form load_mbb_standings shape: one row per (group, team, stat)
    rows = [
        (2024, "2", "ACC", "1", "vsConf_winPercent", 0.90),
        (2024, "2", "ACC", "2", "vsConf_winPercent", 0.55),
        (2024, "8", "SEC", "3", "vsConf_winPercent", 0.75),
        (2024, "8", "SEC", "4", "vsConf_winPercent", 0.80),
    ]
    return pl.DataFrame(
        rows,
        schema={
            "season": pl.Int64,
            "group_id": pl.Utf8,
            "group_name": pl.Utf8,
            "team_id": pl.Utf8,
            "stat_name": pl.Utf8,
            "value": pl.Float64,
        },
        orient="row",
    )


def test_conference_auto_bids_one_per_conference():
    bids = _conference_auto_bids(_standings())
    assert bids == {"1", "4"}

"""Same-game cross-source gate: official box vs pbp-derived player logs.

The real two-surface reconciliation the contract stack called for — the
committed engine fixtures carry the SAME games from two independent
pipelines (the league's aggregated ``boxscoretraditionalv3`` and our
classifier's pbp-derived logs). Floors sit at the observed truth: EXACT
agreement on all seven stats across all five games (three NBA + two
G-League). This gate caught three real extraction bugs on first contact —
FT points omitted from pts (the v3 empty-shotResult quirk), the G-League
single-FT point value, and assist credits dropped for initialed
same-surname teammates — so a red here is a real regression, not noise.
"""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from sportsdataverse.modeling.integrity import agreement_summary, key_coverage, reconcile
from sportsdataverse.nba.nba_possession_sim import (
    player_box_from_boxscorev3,
    player_game_logs_from_pbp,
)

PAIRS = [
    ("nba_engine", "0022100001"),
    ("nba_engine", "0022200001"),
    ("nba_engine", "0022300001"),
    ("nbagl_engine", "2022400003"),
    ("nbagl_engine", "2022400009"),
]
STATS = ["pts", "fga", "fg3a", "fta", "reb", "ast", "tov"]


def _pair(root: str, gid: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    payload = json.loads(pathlib.Path(f"tests/fixtures/{root}/{gid}/playbyplayv3.json").read_text(encoding="utf-8"))
    acts = payload.get("game", {}).get("actions") or payload["actions"]
    raw = pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id"))
    box_payload = json.loads(
        pathlib.Path(f"tests/fixtures/{root}/{gid}/boxscoretraditionalv3.json").read_text(encoding="utf-8")
    )
    return player_game_logs_from_pbp(raw), player_box_from_boxscorev3(box_payload)


@pytest.mark.parametrize("root, gid", PAIRS)
def test_pbp_logs_reconcile_exactly_with_official_box(root: str, gid: str) -> None:
    pbp, box = _pair(root, gid)
    assert box.height >= pbp.height > 0
    # every pbp actor appears in the official box (the box also lists DNPs)
    coverage = key_coverage(pbp, box, keys=["game_id", "player_id"])
    assert coverage["only_left"] == 0, f"pbp actors missing from the box: {coverage}"
    recon = reconcile(pbp, box, keys=["game_id", "player_id"], compare=STATS)
    summary = agreement_summary(recon)
    rates = {row["column"]: row["agree_rate"] for row in summary.to_dicts()}
    # observed floors: EXACT on every stat, every game
    for stat in STATS:
        assert rates[stat] == 1.0, (
            stat,
            recon.filter((pl.col("column") == stat) & (pl.col("agree") == False)).to_dicts(),  # noqa: E712
        )


@pytest.mark.parametrize("root, gid", PAIRS)
def test_team_scoring_totals_match_the_box(root: str, gid: str) -> None:
    pbp, box = _pair(root, gid)
    pbp_totals = pbp.group_by("team_id").agg(pl.col("pts").sum()).sort("team_id")
    box_totals = (
        box.join(pbp.select("player_id").unique(), on="player_id", how="inner")
        .group_by("team_id")
        .agg(pl.col("pts").sum())
        .sort("team_id")
    )
    assert pbp_totals.equals(box_totals)
    # and the box's full team totals equal the pbp actors' totals (DNPs score 0)
    assert int(box["pts"].sum()) == int(pbp["pts"].sum())

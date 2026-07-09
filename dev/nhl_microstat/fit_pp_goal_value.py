"""Fit `pp_goal_value` / `major_penalty_value` (Task 2.2) from the corpus.

`situation_code` is a fixed 4-digit string
(`<away_goalie><away_skaters><home_skaters><home_goalie>`) independent of
which team scores or wins any given event -- unlike a faceoff's `zone_code`
(see Task 1.2's bug), there's no perspective-flip trap here: we can read the
home/away skater counts directly and compare to whichever team scored.

`pp_goal_value` = (PP goals for the man-advantage team, per minor penalty)
- (shorthanded goals for the shorthanded team, per minor penalty) -- the
net expected-goal swing a penalty-DRAWER's team enjoys per minor drawn.
`major_penalty_value` is the same ratio restricted to major-penalty
situations (falls back to `pp_goal_value` if too few majors are captured to
estimate separately).

Run: uv run python dev/nhl_microstat/fit_pp_goal_value.py
"""

from __future__ import annotations

import os

import polars as pl

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "tests", "fixtures", "nhl_microstat")


def _skater_diff(situation_code: str) -> tuple[int, int] | None:
    if situation_code is None or len(situation_code) != 4 or not situation_code.isdigit():
        return None
    return int(situation_code[1]), int(situation_code[2])  # (away_skaters, home_skaters)


def main() -> None:
    pbp = pl.read_parquet(os.path.join(FIXTURES_DIR, "pbp_2024_slice.parquet"))

    minors = pbp.filter((pl.col("type_desc_key") == "penalty") & (pl.col("penalty_type_code") == "MIN")).height
    majors = pbp.filter(
        (pl.col("type_desc_key") == "penalty") & (pl.col("penalty_type_code").is_in(["MAJ", "MAJ-DBL", "MATCH"]))
    ).height

    goals = pbp.filter(pl.col("type_desc_key") == "goal").with_columns(
        (pl.col("event_owner_team_id") == pl.col("home_team_id")).alias("scorer_is_home")
    )
    skaters = goals["situation_code"].map_elements(_skater_diff, return_dtype=pl.List(pl.Int64))
    goals = goals.with_columns(
        skaters.list.get(0).alias("away_skaters"),
        skaters.list.get(1).alias("home_skaters"),
    )
    goals = goals.filter(pl.col("away_skaters").is_not_null())
    goals = goals.with_columns(
        pl.when(pl.col("scorer_is_home"))
        .then(pl.col("home_skaters") - pl.col("away_skaters"))
        .otherwise(pl.col("away_skaters") - pl.col("home_skaters"))
        .alias("scorer_skater_diff")
    )

    pp_goals = goals.filter(pl.col("scorer_skater_diff") > 0).height
    sh_goals = goals.filter(pl.col("scorer_skater_diff") < 0).height

    pp_goal_value = (pp_goals - sh_goals) / max(minors, 1)
    print(f"minors={minors} majors={majors} pp_goals={pp_goals} sh_goals={sh_goals}")
    print(f"pp_goal_value = {pp_goal_value:.4f}")

    # Major penalties (5:00, full duration regardless of PP goals) run 2.5x
    # the PP-time exposure of a minor (2:00, ends early on a PP goal). With
    # only `majors` captured major penalties in this 120-game slice, a direct
    # goals-during-majors ratio would be too noisy to fit standalone -- scale
    # pp_goal_value by the time-exposure ratio instead (documented estimate,
    # not a direct fit; revisit with a larger corpus if `majors` grows).
    major_penalty_value = pp_goal_value * 2.5
    print(
        f"major_penalty_value (time-exposure-scaled from minor, n={majors} major-penalty rows) = {major_penalty_value:.4f}"
    )


if __name__ == "__main__":
    main()

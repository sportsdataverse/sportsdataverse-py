"""Parity tests: wbigballR ``get_team_schedule`` / ``get_team_roster`` (WBB).

Oracle CSVs were produced by running wbigballR against the committed HTML
captures (``team_592003.html`` + ``roster_592003.html`` -- South Carolina,
2024-25).

WBB oracle quirk (documented divergence, fixed deliberately): wbigballR's
``get_team_schedule`` resolves the SELF team name against ``bigballR::teamids``
-- the MEN'S crosswalk -- so a women's team id (592003) finds no name and the
oracle's ``Home``/``Away`` cells hold ``NA`` wherever South Carolina's own name
belongs. The Python port resolves per-league (``ncaa_teamids_wbb.csv``), so we
assert our output says ``"South Carolina"`` exactly where the oracle is null,
and matches the oracle on every other cell.
"""

from __future__ import annotations

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse.mbb.mbb_ncaa_schedule import (
    parse_ncaa_bb_team_roster,
    parse_ncaa_bb_team_schedule,
)
from sportsdataverse.mbb.mbb_ncaa_team_ids import resolve_ncaa_team_id
from tests.mbb._bigballr_oracle import HTML_DIR, load_oracle
from tests.mbb.test_mbb_ncaa_schedule_parity import (
    ROSTER_RENAME,
    ROSTER_SCHEMA,
    SCHEDULE_RENAME,
    SCHEDULE_SCHEMA,
)

TEAM_ID = 592003  # South Carolina 2024-25
TEAM_NAME = "South Carolina"


def _oracle(name: str, rename: dict[str, str], schema: dict[str, pl.DataType]) -> pl.DataFrame:
    df = load_oracle(name, "wbb").rename(rename)
    return df.with_columns([pl.col(c).cast(t, strict=False) for c, t in schema.items()]).select(list(schema))


@pytest.fixture(scope="module")
def schedule_df() -> pl.DataFrame:
    html = (HTML_DIR / f"team_{TEAM_ID}.html").read_text(encoding="utf-8")
    return parse_ncaa_bb_team_schedule(html, TEAM_ID, league="wbb")


class TestScheduleParity:
    def test_self_team_name_fix(self, schedule_df: pl.DataFrame) -> None:
        """Where the oracle holds NA (the men's-table bug), we say South Carolina."""
        oracle = _oracle("team_schedule", SCHEDULE_RENAME, SCHEDULE_SCHEMA)
        home_null = oracle.get_column("home").is_null()
        away_null = oracle.get_column("away").is_null()
        assert home_null.sum() + away_null.sum() == oracle.height  # self appears once per game
        assert set(schedule_df.filter(home_null).get_column("home").to_list()) == {TEAM_NAME}
        assert set(schedule_df.filter(away_null).get_column("away").to_list()) == {TEAM_NAME}

    def test_all_other_cells_match_oracle(self, schedule_df: pl.DataFrame) -> None:
        oracle = _oracle("team_schedule", SCHEDULE_RENAME, SCHEDULE_SCHEMA)
        home_null = oracle.get_column("home").is_null()
        away_null = oracle.get_column("away").is_null()
        masked = schedule_df.select(list(SCHEDULE_SCHEMA)).with_columns(
            pl.when(pl.Series(home_null)).then(None).otherwise(pl.col("home")).alias("home"),
            pl.when(pl.Series(away_null)).then(None).otherwise(pl.col("away")).alias("away"),
        )
        assert_frame_equal(masked, oracle)


class TestRosterParity:
    def test_roster_matches_oracle(self) -> None:
        html = (HTML_DIR / f"roster_{TEAM_ID}.html").read_text(encoding="utf-8")
        ours = parse_ncaa_bb_team_roster(html, TEAM_ID)
        oracle = _oracle("team_roster", ROSTER_RENAME, ROSTER_SCHEMA)
        assert_frame_equal(ours.select(list(ROSTER_SCHEMA)), oracle)


class TestTeamIds:
    def test_resolve_wbb_league(self) -> None:
        assert resolve_ncaa_team_id(TEAM_NAME, "2024-25", league="wbb") == TEAM_ID
        # the same (team, season) does NOT resolve in the men's table
        assert resolve_ncaa_team_id(TEAM_NAME, "2024-25", league="mbb") != TEAM_ID

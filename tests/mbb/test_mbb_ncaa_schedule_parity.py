"""Parity tests: bigballR ``get_team_schedule`` / ``get_team_roster`` (MBB).

Oracle CSVs were produced by running bigballR against the committed HTML
captures (``tests/fixtures/ncaa/bigballr/html/team_609554.html`` +
``roster_609554.html`` -- Illinois, 2025-26). The Python parse cores must
reproduce the R output cell-for-cell after the snake_case rename.
"""

from __future__ import annotations

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse.mbb.mbb_ncaa_schedule import (
    ncaa_mbb_team_roster,
    ncaa_mbb_team_schedule,
    parse_ncaa_bb_team_roster,
    parse_ncaa_bb_team_schedule,
)
from sportsdataverse.mbb.mbb_ncaa_team_ids import (
    ncaa_mbb_team_ids,
    resolve_ncaa_team_id,
)
from tests.mbb._bigballr_oracle import HTML_DIR, load_oracle

TEAM_ID = 609554  # Illinois 2025-26

#: bigballR R contract -> sdv-py snake_case contract.
SCHEDULE_RENAME = {
    "Date": "game_date",
    "Home": "home",
    "Home_Score": "home_score",
    "Away": "away",
    "Away_Score": "away_score",
    "Box_ID": "box_id",
    "Game_ID": "game_id",
    "isNeutral": "is_neutral",
    "Detail": "detail",
    "Attendance": "attendance",
}

SCHEDULE_SCHEMA: dict[str, pl.DataType] = {
    "game_date": pl.Utf8,
    "home": pl.Utf8,
    "home_score": pl.Int64,
    "away": pl.Utf8,
    "away_score": pl.Int64,
    "box_id": pl.Utf8,
    "game_id": pl.Utf8,
    "is_neutral": pl.Boolean,
    "detail": pl.Utf8,
    "attendance": pl.Int64,
}

ROSTER_RENAME = {
    "GP": "gp",
    "GS": "gs",
    "#": "jersey",
    "Name": "name",
    "Class": "class",
    "Position": "position",
    "Height": "height",
    "Hometown": "hometown",
    "High School": "high_school",
    "Player": "player",
    "CleanName": "clean_name",
    "HtInches": "ht_inches",
}

ROSTER_SCHEMA: dict[str, pl.DataType] = {
    "gp": pl.Utf8,
    "gs": pl.Utf8,
    "jersey": pl.Utf8,
    "name": pl.Utf8,
    "class": pl.Utf8,
    "position": pl.Utf8,
    "height": pl.Utf8,
    "hometown": pl.Utf8,
    "high_school": pl.Utf8,
    "player": pl.Utf8,
    "clean_name": pl.Utf8,
    "ht_inches": pl.Int64,
}


def _oracle(name: str, rename: dict[str, str], schema: dict[str, pl.DataType]) -> pl.DataFrame:
    df = load_oracle(name, "mbb").rename(rename)
    return df.with_columns([pl.col(c).cast(t, strict=False) for c, t in schema.items()]).select(list(schema))


@pytest.fixture(scope="module")
def schedule_df() -> pl.DataFrame:
    html = (HTML_DIR / f"team_{TEAM_ID}.html").read_text(encoding="utf-8")
    return parse_ncaa_bb_team_schedule(html, TEAM_ID, league="mbb")


@pytest.fixture(scope="module")
def roster_df() -> pl.DataFrame:
    html = (HTML_DIR / f"roster_{TEAM_ID}.html").read_text(encoding="utf-8")
    return parse_ncaa_bb_team_roster(html, TEAM_ID)


class TestScheduleParity:
    def test_schedule_matches_oracle(self, schedule_df: pl.DataFrame) -> None:
        oracle = _oracle("team_schedule", SCHEDULE_RENAME, SCHEDULE_SCHEMA)
        assert schedule_df.select(list(SCHEDULE_SCHEMA)).schema == oracle.schema
        assert_frame_equal(schedule_df.select(list(SCHEDULE_SCHEMA)), oracle)

    def test_contract_columns_and_dtypes(self, schedule_df: pl.DataFrame) -> None:
        assert list(schedule_df.columns) == list(SCHEDULE_SCHEMA)
        assert schedule_df.schema["attendance"] == pl.Int64
        assert schedule_df.schema["game_id"] == pl.Utf8


class TestRosterParity:
    def test_roster_matches_oracle(self, roster_df: pl.DataFrame) -> None:
        """Cell-for-cell parity, order-insensitively.

        The MBB oracle was captured through chromote; the rendered
        DataTable re-sorts roster rows alphabetically, while the raw-HTML
        path (and this port) keeps the server's row order -- so both sides
        are sorted on a stable key before comparing. The WBB suite (static
        capture) asserts order-preserving parity.
        """
        oracle = _oracle("team_roster", ROSTER_RENAME, ROSTER_SCHEMA)
        assert_frame_equal(
            roster_df.select(list(ROSTER_SCHEMA)).sort("name"),
            oracle.sort("name"),
        )

    def test_contract_columns(self, roster_df: pl.DataFrame) -> None:
        # player_id is Python-only additive (from the /players/{id} hrefs);
        # the R oracle never had it, so it sits outside ROSTER_SCHEMA.
        assert list(roster_df.columns) == [*ROSTER_SCHEMA, "player_id"]

    def test_player_id_from_hrefs(self, roster_df: pl.DataFrame) -> None:
        ids = roster_df.get_column("player_id").drop_nulls()
        assert ids.len() == roster_df.height  # every fixture row carries a link
        assert ids.str.contains(r"^\d+$").all()


class TestTeamIds:
    def test_table_shape(self) -> None:
        df = ncaa_mbb_team_ids()
        assert list(df.columns) == ["team", "conference", "id", "season"]
        assert df.schema["id"] == pl.Int64
        assert df.height > 6000

    def test_resolve_exact_and_case_insensitive(self) -> None:
        assert resolve_ncaa_team_id("Illinois", "2025-26") == TEAM_ID
        assert resolve_ncaa_team_id("illinois", "2025-26") == TEAM_ID
        assert resolve_ncaa_team_id("No Such Team", "2025-26") is None


class _FakeFetcher:
    """Injectable stand-in for NcaaFetcher (offline)."""

    def __init__(self) -> None:
        self.paths: list[str] = []

    def fetch_html(self, path: str, **_: object) -> str:
        self.paths.append(path)
        name = f"roster_{TEAM_ID}.html" if path.endswith("/roster") else f"team_{TEAM_ID}.html"
        return (HTML_DIR / name).read_text(encoding="utf-8")


class TestPublicSurface:
    def test_schedule_by_id_and_by_name(self) -> None:
        fake = _FakeFetcher()
        by_id = ncaa_mbb_team_schedule(TEAM_ID, fetcher=fake)
        by_name = ncaa_mbb_team_schedule(team="Illinois", season="2025-26", fetcher=fake)
        assert fake.paths[0] == f"teams/{TEAM_ID}"
        assert_frame_equal(by_id, by_name)

    def test_roster_pandas(self) -> None:
        out = ncaa_mbb_team_roster(TEAM_ID, fetcher=_FakeFetcher(), return_as_pandas=True)
        assert list(out.columns) == [*ROSTER_SCHEMA, "player_id"]

    def test_improper_request(self) -> None:
        with pytest.raises(ValueError):
            ncaa_mbb_team_schedule()

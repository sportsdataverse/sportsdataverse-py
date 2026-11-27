"""ESPN soccer -> reep team crosswalk: the bundled table, its gate, and the matcher on real fixtures."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.soccer import soccer_espn_reep_team_crosswalk
from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_teams
from tools import build_soccer_espn_reep_crosswalk as build

FIX = Path(__file__).resolve().parents[1] / "fixtures"


@pytest.fixture(scope="module")
def xw() -> pl.DataFrame:
    return soccer_espn_reep_team_crosswalk()


@pytest.fixture(scope="module")
def inputs() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """The real ESPN eng.1 + usa.1 /teams fixtures and the real reep rows they touch."""
    espn = pl.concat(
        build.espn_frame(
            parse_soccer_teams(json.loads((FIX / f"espn/soccer/{lg}/site-v2/teams.json").read_text(encoding="utf-8"))),
            lg,
        )
        for lg in ("eng.1", "usa.1")
    )
    teams = pl.read_csv(FIX / "reep/teams.csv", infer_schema=False)
    aliases = pl.read_csv(FIX / "reep/names.csv", infer_schema=False)
    return espn, teams, aliases


@pytest.fixture(scope="module")
def fixture_match(inputs) -> tuple[pl.DataFrame, list]:
    return build.match(*inputs, build.load_overrides())


def test_bundled_table_passes_the_gate(xw: pl.DataFrame) -> None:
    assert build.gate_failures(xw) == []
    assert xw.height >= 160
    assert sorted(set(xw["espn_league"].to_list())) == sorted(build.LEAGUE_COUNTRIES)


def test_one_row_per_espn_team_and_no_reep_row_twice(xw: pl.DataFrame) -> None:
    assert xw["espn_team_id"].n_unique() == xw.height
    claimed = xw["reep_id"].drop_nulls()
    assert claimed.n_unique() == claimed.len()
    assert xw["espn_team_id"].str.contains(r"^[0-9]+$").all()


def test_schema_is_text_and_joins_the_espn_teams_parser(xw: pl.DataFrame) -> None:
    assert list(xw.columns) == list(build.OUT_COLS)
    assert set(xw.schema.values()) == {pl.Utf8}
    espn = parse_soccer_teams(json.loads((FIX / "espn/soccer/eng.1/site-v2/teams.json").read_text(encoding="utf-8")))
    assert espn.schema["team_id"] == xw.schema["espn_team_id"]
    joined = espn.join(xw, left_on="team_id", right_on="espn_team_id", how="inner")
    assert joined.height >= 15  # the 2025-26 EPL fixture vs the 2026-27 build: promotion/relegation moves a few


def test_match_methods_and_nulls_agree(xw: pl.DataFrame) -> None:
    assert set(xw["match_method"].unique().to_list()) <= {"key_espn", "name", "review", "unmatched"}
    unmatched = xw.filter(pl.col("match_method") == "unmatched")
    assert unmatched["reep_id"].is_null().all()
    assert xw.filter(pl.col("match_method") != "unmatched")["reep_id"].is_not_null().all()


def test_known_clubs(xw: pl.DataFrame) -> None:
    row = {r["espn_team_id"]: r for r in xw.iter_rows(named=True)}
    assert row["360"]["match_method"] == "key_espn" and row["360"]["key_transfermarkt"] == "985"  # Man United
    assert row["382"]["key_transfermarkt"] == "281"  # Manchester City, matched by name
    assert row["364"]["reep_id"] == "reep_td5dab25f" and row["364"]["match_method"] == "review"  # not A.F.C. Liverpool
    assert row["170"]["reep_name"] == "ES Troyes AC"  # the review overrules the location match to AS Troyes


def test_league_filter(xw: pl.DataFrame) -> None:
    epl = soccer_espn_reep_team_crosswalk(league="eng.1")
    assert epl.height == xw.filter(pl.col("espn_league") == "eng.1").height > 0
    with pytest.raises(ValueError, match="league must be one of"):
        soccer_espn_reep_team_crosswalk(league="usa.nwsl")
    pdf = soccer_espn_reep_team_crosswalk(league="usa.1", return_as_pandas=True)
    assert len(pdf) == xw.filter(pl.col("espn_league") == "usa.1").height


def test_matcher_on_real_fixtures_reproduces_the_bundled_rows(fixture_match, xw: pl.DataFrame) -> None:
    got, review = fixture_match
    assert review == []
    both = got.join(xw, on="espn_team_id", suffix="_bundled")
    assert both.height >= 40
    assert (both["reep_id"] == both["reep_id_bundled"]).all()
    assert both.filter(pl.col("reep_id").is_null() != pl.col("reep_id_bundled").is_null()).height == 0
    assert (both["match_method"] == both["match_method_bundled"]).all()


def test_ambiguous_names_need_a_review(inputs) -> None:
    _, review = build.match(*inputs, {})
    pending = {r["espn_team_id"]: r["candidates"] for r in review}
    assert pending["364"] == ["reep_t79dcad78", "reep_td5dab25f"]  # Liverpool F.C. vs A.F.C. Liverpool
    assert pending["18966"] == []  # LAFC: no name hit at all
    assert "359" not in pending  # Arsenal: one in-country row; the Lesotho and Dutch namesakes are filtered out


def test_gate_fails_on_a_shared_reep_row_and_on_lost_coverage(xw: pl.DataFrame) -> None:
    dup = xw.with_columns(
        pl.when(pl.col("espn_team_id") == "382")
        .then(pl.lit("reep_td5dab25f"))
        .otherwise(pl.col("reep_id"))
        .alias("reep_id")
    )
    assert any("reep_id on two ESPN teams" in f for f in build.gate_failures(dup))
    lost = xw.with_columns(
        pl.when(pl.col("espn_league") == "mex.1").then(None).otherwise(pl.col("reep_id")).alias("reep_id")
    )
    assert any(f.startswith("mex.1:") for f in build.gate_failures(lost))
    assert any(f.startswith("duplicate espn_team_id") for f in build.gate_failures(pl.concat([xw, xw.head(1)])))


def test_a_null_or_non_numeric_espn_id_is_refused(xw: pl.DataFrame) -> None:
    parsed = parse_soccer_teams(json.loads((FIX / "espn/soccer/eng.1/site-v2/teams.json").read_text(encoding="utf-8")))
    with pytest.raises(ValueError, match="non-numeric ESPN team ids"):
        build.espn_frame(parsed.with_columns(pl.lit(None, dtype=pl.Utf8).alias("team_id")), "eng.1")
    assert "null espn_team_id" in build.gate_failures(
        xw.with_columns(
            pl.when(pl.col("espn_team_id") == "360").then(None).otherwise(pl.col("espn_team_id")).alias("espn_team_id")
        )
    )


def test_a_provider_id_on_two_espn_teams_fails_the_gate(xw: pl.DataFrame) -> None:
    # two reep rows of one club share a Transfermarkt id (reep holds some clubs twice): reep_id stays unique
    shared = xw.with_columns(
        pl.when(pl.col("espn_team_id") == "382")
        .then(pl.lit("985"))
        .otherwise(pl.col("key_transfermarkt"))
        .alias("key_transfermarkt")
    )
    assert any(f.startswith("key_transfermarkt on two ESPN teams") for f in build.gate_failures(shared))


def test_the_bundled_frame_is_not_shared_with_callers() -> None:
    a = soccer_espn_reep_team_crosswalk()
    a.insert_column(0, pl.lit("x").alias("scratch"))
    assert "scratch" not in soccer_espn_reep_team_crosswalk().columns

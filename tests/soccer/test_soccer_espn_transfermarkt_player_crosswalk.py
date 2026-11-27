"""ESPN athlete -> Transfermarkt player crosswalk: the bundled table, its pre-registered gate, and the matcher on real rows."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.soccer import soccer_espn_transfermarkt_player_crosswalk
from tools import build_soccer_espn_transfermarkt_player_crosswalk as build

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "transfermarkt_player_bridge"


@pytest.fixture(scope="module")
def xw() -> pl.DataFrame:
    return soccer_espn_transfermarkt_player_crosswalk()


@pytest.fixture(scope="module")
def ref() -> pl.DataFrame:
    return build.reep_pairs(pl.read_csv(FIX / "reep_people.csv", infer_schema=False))


@pytest.fixture(scope="module")
def inputs() -> dict[str, pl.DataFrame]:
    espn = pl.read_csv(FIX / "espn_rows.csv", infer_schema=False).with_columns(pl.col("dob").str.to_date())
    players = pl.read_csv(FIX / "tm_players.csv", infer_schema=False).with_columns(
        pl.col("player_id", "current_club_id").cast(pl.Int64), pl.col("date_of_birth").str.to_date()
    )
    transfers = pl.read_csv(FIX / "tm_transfers.csv", infer_schema=False).with_columns(
        pl.col("player_id", "to_club_id").cast(pl.Int64), pl.col("transfer_date").str.to_date()
    )
    team_club = pl.read_csv(FIX / "team_club.csv", infer_schema=False).with_columns(pl.col("club_id").cast(pl.Int64))
    return {"espn": espn, "players": players, "transfers": transfers, "team_club": team_club}


def _resolve(inputs: dict[str, pl.DataFrame]) -> pl.DataFrame:
    return build.resolve(inputs["espn"], inputs["players"], inputs["team_club"], inputs["transfers"])


def test_bundled_table_passes_the_pre_registered_gate(xw, ref) -> None:
    assert build.gate_failures(xw, ref) == []
    assert xw.height > 4000 and sorted(set(xw["espn_league"].to_list())) == sorted(build.FIRST_TIER)


def test_schema_and_one_to_one(xw) -> None:
    assert xw.schema == pl.Schema(build.OUT_SCHEMA)
    assert xw["espn_athlete_id"].n_unique() == xw.height
    ids = xw["player_id"].drop_nulls()
    assert ids.n_unique() == ids.len()
    assert (xw["player_id"].is_null() == (xw["match_method"] == "unmatched")).all()
    assert set(xw["match_method"].unique().to_list()) <= {
        "name_dob", "name_dob_nationality", "name_jan1_club", "name_jan1_nationality", "unmatched",
    }  # fmt: skip


def test_the_matcher_on_real_rows_reproduces_the_bundled_rows(inputs, xw) -> None:
    got = _resolve(inputs)
    assert got.height == inputs["espn"].height == 50
    both = got.join(xw, on="espn_athlete_id", suffix="_bundled")
    assert both.height == got.height
    assert (both["player_id"].fill_null(-1) == both["player_id_bundled"].fill_null(-1)).all()
    assert (both["match_method"] == both["match_method_bundled"]).all()
    assert {"name_dob", "name_dob_nationality", "name_jan1_club", "name_jan1_nationality"} <= set(got["match_method"])


def test_a_1_january_date_needs_club_or_nationality(inputs) -> None:
    got = {r["espn_athlete_id"]: r for r in _resolve(inputs).iter_rows(named=True)}
    # two ESPN López born 2006-01-01, one Transfermarkt Eder López (FC Juárez) of that date
    assert got["259983"]["player_id"] == 1235196 and got["259983"]["match_method"] == "name_jan1_club"  # FC Juárez
    assert got["397289"]["player_id"] is None  # Espanyol: same name and date, no club or nationality evidence


def test_the_gate_fails_when_a_player_takes_two_athletes(xw, ref) -> None:
    pid = xw.filter(pl.col("player_id").is_not_null())["player_id"][0]
    other = xw.filter(pl.col("player_id").is_null())["espn_athlete_id"][0]
    broken = xw.with_columns(
        pl.when(pl.col("espn_athlete_id") == other).then(pl.lit(pid)).otherwise(pl.col("player_id")).alias("player_id"),
        pl.when(pl.col("espn_athlete_id") == other)
        .then(pl.lit("name_dob"))
        .otherwise(pl.col("match_method"))
        .alias("match_method"),
    )
    assert any("two ESPN athletes" in f for f in build.gate_failures(broken, ref))


def test_the_gate_fails_on_lost_coverage_or_reep_disagreement(xw, ref) -> None:
    lost = xw.with_columns(
        pl.when(pl.col("espn_league") == "ita.1").then(None).otherwise(pl.col("player_id")).alias("player_id"),
        pl.when(pl.col("espn_league") == "ita.1")
        .then(pl.lit("unmatched"))
        .otherwise(pl.col("match_method"))
        .alias("match_method"),
    )
    assert any(f.startswith("ita.1:") for f in build.gate_failures(lost, ref))
    wrong = ref.with_columns((pl.col("ref_player_id") + 1).alias("ref_player_id"))
    assert any(f.startswith("reep agreement") for f in build.gate_failures(xw, wrong))


def test_league_filter_and_pandas(xw) -> None:
    epl = soccer_espn_transfermarkt_player_crosswalk(league="eng.1")
    assert epl.height == xw.filter(pl.col("espn_league") == "eng.1").height > 0
    with pytest.raises(ValueError, match="league must be one of"):
        soccer_espn_transfermarkt_player_crosswalk(league="eng.2")
    pdf = soccer_espn_transfermarkt_player_crosswalk(league="usa.1", return_as_pandas=True)
    assert str(pdf["player_id"].dtype).startswith("int64")


def test_the_gate_thresholds_are_the_pre_registered_ones() -> None:
    assert build.GATE == {"min_share": 0.80, "min_league_share": 0.70, "min_reep_agreement": 0.98}
    assert build.MOVES_FROM == (2025, 7, 1)


def test_ambiguous_and_uncorroborated_athletes_stay_unmatched(inputs) -> None:
    got = {r["espn_athlete_id"]: r for r in _resolve(inputs).iter_rows(named=True)}
    for athlete in ("334227", "281323", "258855"):  # several same-date same-name players, none with the citizenship
        assert got[athlete]["match_method"] == "unmatched", athlete
    for athlete in ("172836", "324433", "323273"):  # several, two or more with the citizenship: no tie-break
        assert got[athlete]["match_method"] == "unmatched", athlete
    assert got["242439"]["match_method"] == "unmatched"  # a lone 1 January candidate with no club or nationality


def test_a_transfer_into_the_club_corroborates_a_1_january_date(inputs) -> None:
    """Real rows with one change: López's snapshot club is cleared, so only his 2025-26 move to FC Juárez remains."""
    moved_to = inputs["transfers"].filter(pl.col("player_id") == 1235196)
    if moved_to.is_empty():
        pytest.skip("the fixture has no 2025-07-01+ transfer for this player")
    club = moved_to["to_club_id"][0]
    team = inputs["team_club"].filter(pl.col("club_id") == club)["espn_team_id"].to_list()
    players = inputs["players"].with_columns(
        pl.when(pl.col("player_id") == 1235196).then(None).otherwise(pl.col("current_club_id")).alias("current_club_id")
    )
    got = build.resolve(inputs["espn"], players, inputs["team_club"], inputs["transfers"])
    row = got.filter(pl.col("espn_athlete_id") == "259983").row(0, named=True)
    assert team == ["17851"] and row["player_id"] == 1235196 and row["match_method"] == "name_jan1_club"
    # and with no club, no transfer and no citizenship evidence, the date and name alone are not enough
    stateless = players.with_columns(
        pl.when(pl.col("player_id") == 1235196)
        .then(None)
        .otherwise(pl.col("country_of_citizenship"))
        .alias("country_of_citizenship")
    )
    got = build.resolve(inputs["espn"], stateless, inputs["team_club"], inputs["transfers"].head(0))
    assert got.filter(pl.col("espn_athlete_id") == "259983")["player_id"].to_list() == [None]


def test_espn_frame_on_a_real_roster() -> None:
    import json

    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_team_roster

    fix = (
        Path(__file__).resolve().parents[1] / "fixtures" / "espn" / "soccer" / "eng.1" / "site-v2" / "team_roster.json"
    )
    roster = parse_soccer_team_roster(json.loads(fix.read_text(encoding="utf-8")))
    frame = build.espn_frame(pl.concat([roster, roster.head(2)]), "349", "eng.1")  # a repeated athlete is kept once
    assert frame["espn_athlete_id"].n_unique() == frame.height == roster["athlete_id"].n_unique()
    assert frame.schema["dob"] == pl.Date and frame["dob"].null_count() < frame.height
    blank = roster.head(1).with_columns(pl.lit("").alias("last_name"))
    assert build.espn_frame(blank, "349", "eng.1")["espn_last"][0] == roster["display_name"][0]

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess
from tests.conftest import fetch_pbp_or_skip, skip_if_no_live


def test_cfb_pbp_missing_competitions_raises_noespndata():
    """Guard (offline): an ESPN summary with no ``header.competitions`` raises a
    clean ``NoESPNDataError`` instead of a bare ``KeyError: 'competitions'``."""
    from sportsdataverse.errors import NoESPNDataError

    proc = CFBPlayProcess(gameId=401301025)
    with pytest.raises(NoESPNDataError):
        proc._CFBPlayProcess__helper_cfb_pbp({"header": {}})


@pytest.fixture()
def generated_cfb_data():
    test = CFBPlayProcess(gameId=401301025)
    fetch_pbp_or_skip(test)
    test.run_processing_pipeline()
    yield test


@pytest.fixture()
def cfb_box_score(generated_cfb_data):
    yield generated_cfb_data.create_box_score(pl.DataFrame(generated_cfb_data.plays_json, infer_schema_length=400))


def test_basic_cfb_pbp(generated_cfb_data):
    assert generated_cfb_data.json is not None

    generated_cfb_data.run_processing_pipeline()
    assert len(generated_cfb_data.plays_json) > 0
    assert generated_cfb_data.ran_pipeline == True
    assert isinstance(pl.DataFrame(generated_cfb_data.plays_json, infer_schema_length=400), pl.DataFrame)


def test_cfb_adv_box_score(cfb_box_score):
    assert cfb_box_score is not None
    # Subset direction (expected ⊆ actual): the box score must contain these sections;
    # additive sections are allowed so the test doesn't break when new ones are introduced.
    expected_sections = {
        "pass",
        "rush",
        "receiver",
        "team",
        "situational",
        "defensive",
        "defensive_players",
        "specialists",
        "turnover",
        "drives",
    }
    assert expected_sections.issubset(set(cfb_box_score.keys()))


def test_havoc_rate(cfb_box_score):
    defense_home = cfb_box_score["defensive"][0]
    # print(defense_home)
    passes_defended = defense_home.get("pass_breakups", 0)
    home_int = defense_home.get("Int", 0)
    tfl = defense_home.get("TFL", 0)
    fum = defense_home.get("fumbles", 0)
    plays = defense_home.get("scrimmage_plays", 0)

    assert plays > 0
    assert defense_home["havoc_total"] == (passes_defended + home_int + tfl + fum)
    assert round(defense_home["havoc_total_rate"], 4) == round(((passes_defended + home_int + tfl + fum) / plays), 4)


@pytest.fixture()
def dupe_fsu_play_base():
    test = CFBPlayProcess(gameId=401411109)
    fetch_pbp_or_skip(test)
    test.run_processing_pipeline()
    yield pl.DataFrame(test.plays_json, infer_schema_length=400)


def test_fsu_play_dedupe(dupe_fsu_play_base):
    target_strings = [
        {
            "text": "Jordan Travis pass intercepted Rance Conner return for no gain to the FlaSt 45",
            "down": 3,
            "distance": 9,
            "yardsToEndzone": 74,
        },
        {"down": 4, "text": "Malik Cunningham pass incomplete to Tyler Hudson", "distance": 2, "yardsToEndzone": 45},
    ]

    regression_cases = [
        {
            "text": "Alex Mastromanno punt for 52 yds , Braden Smith returns for no gain to the Lvile 37",
            "down": 4,
            "distance": 9,
            "yardsToEndzone": 89,
        },
    ]

    for item in target_strings:
        print(f"Checking known test cases for dupes for play_text '{item}'")
        assert (
            len(
                dupe_fsu_play_base.filter(
                    (pl.col("text") == item["text"])
                    & (pl.col("start.down") == item["down"])
                    & (pl.col("start.distance") == item["distance"])
                    & (pl.col("start.yardsToEndzone") == item["yardsToEndzone"]),
                ),
            )
            == 1
        )
        print(f"No dupes for play_text '{item}'")

    for item in regression_cases:
        print(f"Checking non-dupe base cases for dupes for play_text '{item}'")
        assert (
            len(
                dupe_fsu_play_base.filter(
                    (pl.col("text") == item["text"])
                    & (pl.col("start.down") == item["down"])
                    & (pl.col("start.distance") == item["distance"])
                    & (pl.col("start.yardsToEndzone") == item["yardsToEndzone"]),
                ),
            )
            == 1
        )
        print(f"confirmed no dupes for regression case of play_text '{item}'")


@pytest.fixture()
def iu_play_base():
    test = CFBPlayProcess(gameId=401426563)
    fetch_pbp_or_skip(test)
    test.run_processing_pipeline()
    yield test


@pytest.fixture()
def dupe_iu_play_base(iu_play_base):
    yield pl.DataFrame(iu_play_base.plays_json, infer_schema_length=400)


def test_iu_play_dedupe(dupe_iu_play_base):
    target_strings = [
        {
            "text": "A. Reed pass,to J. Beljan for 26 yds for a TD, (B. Narveson KICK)",
            "down": 2,
            "distance": 9,
            "yardsToEndzone": 26,
        },
    ]

    elimination_strings = [
        {
            "text": "Austin Reed pass complete to Joey Beljan for 26 yds for a TD",
            "down": 2,
            "distance": 9,
            "yardsToEndzone": 26,
        },
    ]

    for item in target_strings:
        print(f"Checking known test cases for dupes for play_text '{item}'")
        assert (
            len(
                dupe_iu_play_base.filter(
                    (pl.col("text") == item["text"])
                    & (pl.col("start.down") == item["down"])
                    & (pl.col("start.distance") == item["distance"])
                    & (pl.col("start.yardsToEndzone") == item["yardsToEndzone"]),
                ),
            )
            == 1
        )
        print(f"No dupes for play_text '{item}'")

    for item in elimination_strings:
        print(f"Checking for strings that should have been removed by dupe check for play_text '{item}'")
        assert (
            len(
                dupe_iu_play_base.filter(
                    (pl.col("text") == item["text"])
                    & (pl.col("start.down") == item["down"])
                    & (pl.col("start.distance") == item["distance"])
                    & (pl.col("start.yardsToEndzone") == item["yardsToEndzone"]),
                ),
            )
            == 0
        )
        print(f"Confirmed no values for play_text '{item}'")


@pytest.fixture()
def iu_play_base_box(iu_play_base):
    yield iu_play_base.create_box_score(pl.DataFrame(iu_play_base.plays_json, infer_schema_length=400))


def test_expected_turnovers(iu_play_base_box):
    defense_home = iu_play_base_box["defensive"][1]
    def_home_team = defense_home.get("def_pos_team", "NA")
    away_pd = iu_play_base_box["turnover"][0].get("pass_breakups", 0)
    away_off_int = iu_play_base_box["turnover"][0].get("Int", 0)
    away_fum = iu_play_base_box["turnover"][0].get("total_fumbles", 0)

    away_exp_xTO = (0.22 * (away_pd + away_off_int)) + (0.5 * away_fum)
    away_actual_xTO = iu_play_base_box["turnover"][0].get("expected_turnovers")
    away_team = iu_play_base_box["turnover"][0].get("pos_team", "NA")

    defense_away = iu_play_base_box["defensive"][0]
    def_away_team = defense_away.get("def_pos_team", "NA")
    home_pd = iu_play_base_box["turnover"][1].get("pass_breakups", 0)
    home_off_int = iu_play_base_box["turnover"][1].get("Int", 0)
    home_fum = iu_play_base_box["turnover"][1].get("total_fumbles", 0)

    home_exp_xTO = (0.22 * (home_pd + home_off_int)) + (0.5 * home_fum)
    home_actual_xTO = iu_play_base_box["turnover"][1].get("expected_turnovers")
    home_team = iu_play_base_box["turnover"][1].get("pos_team", "NA")

    print(
        f"home team: {home_team} vs def {def_away_team} - fum: {home_fum}, int: {home_off_int}, pd: {home_pd} -> xTO: {home_exp_xTO}",
    )
    print(
        f"away off {away_team} vs def {def_home_team} - fum: {away_fum}, int: {away_off_int}, pd: {away_pd} -> xTO: {away_exp_xTO}",
    )
    assert round(away_exp_xTO, 4) == round(away_actual_xTO, 4)
    assert round(home_exp_xTO, 4) == round(home_actual_xTO, 4)


@skip_if_no_live
def test_pbp_handles_python_float_overUnder():
    """Regression: 2024 CFP semifinal (game_id=401628334) previously failed in
    ``__helper_cfb_pbp_features`` because ``init["overUnder"].astype(float)``
    was called on a Python ``float`` (no ``.astype()`` attribute). The
    defensive cast in ``__helper_cfb_game_data`` now handles both numpy and
    Python scalar shapes for ``overUnder`` / ``gameSpread`` / ``homeFavorite``.
    """
    test = CFBPlayProcess(gameId=401628334)
    fetch_pbp_or_skip(test)
    test.run_processing_pipeline()  # must not raise
    assert test.ran_pipeline is True
    assert len(test.plays_json) > 0


@skip_if_no_live
def test_modern_2024_game_gets_real_spread_not_default():
    """ESPN emptied the legacy ``pickcenter`` array on the summary endpoint
    for 2024+ games. Before the modern-odds cascade, every play in those
    games silently inherited the defaults ``(2.5, 55.0/55.5, True)`` —
    corrupting WPA / EP. The modern core-odds endpoint
    (``sports.core.api.espn.com/v2/.../events/{gid}/competitions/{gid}/odds``)
    has 5 items for ``401628334`` (2024 CFP semifinal), so the cascade
    should pull a real spread / total instead of the defaults.
    """
    test = CFBPlayProcess(gameId=401628334)
    fetch_pbp_or_skip(test)
    test.run_processing_pipeline()
    assert test.ran_pipeline is True
    assert len(test.plays_json) > 0
    # gameSpreadAvailable should be True via the modern cascade (was False
    # before the cascade landed for this game).
    assert test.gameSpreadAvailable is True
    # Defaults are 2.5 spread / 55.5 (or 55.0 legacy) overUnder. Modern
    # endpoint reports a real spread + total for this game.
    assert float(test.gameSpread) != 2.5
    assert float(test.overUnder) not in (55.0, 55.5)


@skip_if_no_live
def test_old_2014_game_still_uses_legacy_pickcenter_when_available():
    """Regression guard: a pre-2024 game with a populated legacy
    ``pickcenter`` (``400547765`` historically had 3+ entries) must continue
    to use that legacy path — the cascade only invokes the modern endpoint
    when legacy is empty. This guards against accidentally regressing the
    older games while restoring the modern ones.
    """
    test = CFBPlayProcess(gameId=400547765)
    fetch_pbp_or_skip(test)
    test.run_processing_pipeline()
    assert test.ran_pipeline is True
    assert len(test.plays_json) > 0
    # Either real legacy values OR (if legacy was actually empty for this
    # game on ESPN's current API) modern fallback values; in neither case
    # should we crash, and gameSpreadAvailable should be True.
    assert test.gameSpreadAvailable is True

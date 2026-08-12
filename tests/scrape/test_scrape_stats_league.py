"""League-parameterization tests for the Phase 2 capture layer.

The heavyweight behavioral suites stay in the owning -raw repos (they exercise
their own league's behavior against this engine); these tests pin the
league-varying seams: season-string spelling, period time math, id decoding,
and the refill census. All offline.
"""

import importlib
import json

import pytest

from sportsdataverse.nba.nba_lineups import _QUARTER_BOX_RANGE_TYPE, _period_start_range
from sportsdataverse.scrape.stats import league_config, periods, refill
from sportsdataverse.scrape.stats.endpoints import (
    measure_types_for,
    season_string,
    season_variants,
)

# -- league_config -------------------------------------------------------------


def test_configs_are_consistent_and_resolvable() -> None:
    for cfg in (league_config.NBA, league_config.WNBA):
        assert league_config.by_league_id(cfg.league_id) is cfg
        mod = importlib.import_module(cfg.stats_module)
        fns = [n for n in dir(mod) if n.startswith(f"{cfg.stats_prefix}_")]
        assert len(fns) > 50, f"{cfg.stats_module} exposes {len(fns)} wrappers"


def test_unknown_league_id_raises() -> None:
    with pytest.raises(KeyError):
        league_config.by_league_id("99")


# -- endpoints: span vs bare season spelling -----------------------------------


def _fake_endpoint_span(season: str = "", league_id: str = "") -> None: ...
def _fake_endpoint_year(season_year: str = "", league_id: str = "") -> None: ...
def _fake_endpoint_year_nullable(season_year_nullable: str = "", league_id: str = "") -> None: ...


def test_nba_spans_the_season_string_wnba_stays_bare() -> None:
    """The load-bearing league difference: a bare year silently returns zero
    rows on several NBA endpoints, while the WNBA API takes the bare year."""
    (_v_nba,) = list(season_variants(_fake_endpoint_span, 2023, "00"))
    assert _v_nba[1]["season"] == "2023-24"
    (_v_wnba,) = list(season_variants(_fake_endpoint_span, 2023, "10"))
    assert _v_wnba[1]["season"] == "2023"


def test_season_year_is_bare_in_both_leagues() -> None:
    """`season_year` (draftcombine spelling) takes the bare draft year even on
    the NBA side — it is deliberately absent from _SPAN_SEASON_PARAMS."""
    for league_id in ("00", "10"):
        (variant,) = list(season_variants(_fake_endpoint_year, 2019, league_id))
        assert variant[1]["season_year"] == "2019"


def test_season_year_nullable_is_still_a_season_filter() -> None:
    """drafthistory's spelling must not fall through to "no season".

    _SEASON_PARAMS is matched by EXACT name, so `season_year_nullable` did not
    match `season_year` and the sweep sent league_id alone. Unfiltered,
    drafthistory answers with the whole 1947-2026 history, so the miss does not
    surface as an empty capture — it writes the identical full-history payload
    under every season (the state wehoop-wnba-stats-raw is in today).
    """
    for league_id in ("00", "10"):
        (variant,) = list(season_variants(_fake_endpoint_year_nullable, 2003, league_id))
        assert variant[1]["season_year_nullable"] == "2003", (
            "season_year_nullable dropped -> every season captures the full draft history"
        )


def test_season_string_spelling() -> None:
    assert season_string(2023) == "2023-24"
    assert season_string(1999) == "1999-00"


def test_measure_override_never_touches_other_axes() -> None:
    default = ("Regular Season", "Playoffs")
    got = measure_types_for("nba_stats_leaguedashteamstats", "season_type_all_star", default)
    assert got == default


# -- periods: league- and era-aware window math --------------------------------


def test_nba_window_matches_the_possession_engine_reader() -> None:
    """Drift-guard: the capture window must equal what nba_lineups computes when
    it reads these payloads back. Pinned for regulation + deep OT."""
    assert periods.QUARTER_BOX_RANGE_TYPE == _QUARTER_BOX_RANGE_TYPE
    for period in range(1, 13):
        assert periods.period_start_range(period, 2024, "00") == _period_start_range(period)


def test_wnba_halves_era_boundaries() -> None:
    # Through 2005: two 20-minute halves. Period 2 opens at 1200s, OT at 2400s.
    assert periods.regulation_shape(2005, "10") == (2, 1200)
    assert periods.period_elapsed_seconds(2, 2005, "10") == 1200
    assert periods.period_elapsed_seconds(3, 2005, "10") == 2400  # OT1
    assert periods.period_elapsed_seconds(4, 2005, "10") == 2700  # OT2


def test_wnba_quarters_era_boundaries() -> None:
    # From 2006: four 10-minute quarters. Period 2 opens at 600s, OT at 2400s.
    assert periods.regulation_shape(2006, "10") == (4, 600)
    assert periods.period_elapsed_seconds(2, 2006, "10") == 600
    assert periods.period_elapsed_seconds(5, 2006, "10") == 2400  # OT1
    assert periods.period_elapsed_seconds(6, 2006, "10") == 2700  # OT2


def test_wnba_regulation_total_is_constant_across_the_format_change() -> None:
    """2400s regulation in BOTH eras — the trap: a total check cannot catch a
    mixed-up era, only the period boundaries can."""
    for season in (2005, 2006):
        p, s = periods.regulation_shape(season, "10")
        assert p * s == 2400


def test_period_start_range_is_a_one_second_window() -> None:
    start, end = periods.period_start_range(2, 2006, "10")
    assert (start, end) == ("6000", "6010")


def test_season_of_league_offsets() -> None:
    assert periods.season_of("0020500469", "00") == 2006  # NBA: END year
    assert periods.season_of("1022600071", "10") == 2026  # WNBA: calendar year
    assert periods.season_of("0029900012", "00") == 2000  # 19xx pivot


def test_periods_in_game_reads_pbp_and_clamps() -> None:
    pbp = {"game": {"actions": [{"period": 1}, {"period": 4}, {"period": "5"}]}}
    assert periods.periods_in_game(pbp) == 5
    assert periods.periods_in_game({"game": {"actions": [{"period": 99}]}}) == periods.MAX_PERIODS
    assert periods.periods_in_game(None) == 0
    assert periods.periods_in_game({"game": {}}) == 0


# -- refill: census + root resolution (offline) --------------------------------


def test_find_empty_maps_the_store_layout(tmp_path) -> None:
    (tmp_path / "leagueleaders" / "1996").mkdir(parents=True)
    (tmp_path / "leagueleaders" / "1996" / "playoffs_pergame.json").write_text("{}")
    (tmp_path / "drafthistory").mkdir()
    (tmp_path / "drafthistory" / "2001.json").write_text("{}")
    ok = tmp_path / "leagueleaders" / "1996" / "regular-season_totals.json"
    ok.write_text(json.dumps({"resultSets": []}))  # a REAL empty answer — must survive
    empty = refill.find_empty(tmp_path)
    assert empty[1996] == {("leagueleaders", "playoffs_pergame")}
    assert empty[2001] == {("drafthistory", None)}


def test_store_root_env_override(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(league_config.NBA.store_env, str(tmp_path))
    assert refill.store_root(league_config.NBA, tmp_path / "unused") == tmp_path
    monkeypatch.delenv(league_config.NBA.store_env)
    assert refill.store_root(league_config.NBA, tmp_path / "d") == tmp_path / "d"


def test_refill_check_mode_is_offline(tmp_path, capsys) -> None:
    (tmp_path / "leagueleaders" / "1996").mkdir(parents=True)
    (tmp_path / "leagueleaders" / "1996" / "playoffs_pergame.json").write_text("{}")
    rc = refill.main(league_config.NBA, ["--check"], default_root=tmp_path)
    assert rc == 0
    out = capsys.readouterr().out
    assert "1 contentless payloads" in out

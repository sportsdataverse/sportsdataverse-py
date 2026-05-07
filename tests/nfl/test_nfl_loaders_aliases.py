"""Smoke tests for the nflreadpy-parity aliases inside ``sportsdataverse.nfl``.

Short-name aliases live inside the ``sportsdataverse.nfl`` namespace
that mirror nflreadpy's bare ``load_*`` shape (``load_pbp``,
``load_schedules``, ``get_current_season``, ...). Each alias is a simple
re-export of the canonical ``load_nfl_*`` (or ``get_current_nfl_*``)
function — no new code, no behavior change.

These tests verify two things:

1. Every alias is the SAME function object as its canonical counterpart
   (``alias is canonical``). That is what makes the alias a true zero-cost
   re-export rather than a wrapper that could drift.
2. ``load_nfl_ff_rankings`` accepts both ``type=`` and ``kind=`` and they
   produce identical data; ``kind`` wins when both are supplied.

Aliases are checked individually rather than via a parametrized loop so the
identity assertion shows up clearly in test output if any one of them
breaks.
"""

from __future__ import annotations

import polars as pl
import pytest

import sportsdataverse.nfl as nfl
from sportsdataverse.nfl import (  # Canonical names — these continue to work.; nflreadpy-style aliases.
    get_current_nfl_season,
    get_current_nfl_week,
    get_current_season,
    get_current_week,
    load_combine,
    load_contracts,
    load_depth_charts,
    load_draft_picks,
    load_ff_opportunity,
    load_ff_playerids,
    load_ff_rankings,
    load_ftn_charting,
    load_injuries,
    load_nextgen_stats,
    load_nfl_combine,
    load_nfl_contracts,
    load_nfl_depth_charts,
    load_nfl_draft_picks,
    load_nfl_ff_opportunity,
    load_nfl_ff_playerids,
    load_nfl_ff_rankings,
    load_nfl_ftn_charting,
    load_nfl_injuries,
    load_nfl_nextgen_stats,
    load_nfl_officials,
    load_nfl_pbp,
    load_nfl_pbp_participation,
    load_nfl_pfr_advstats,
    load_nfl_player_stats,
    load_nfl_players,
    load_nfl_rosters,
    load_nfl_schedule,
    load_nfl_snap_counts,
    load_nfl_team_stats,
    load_nfl_teams,
    load_nfl_trades,
    load_nfl_weekly_rosters,
    load_officials,
    load_participation,
    load_pbp,
    load_pfr_advstats,
    load_player_stats,
    load_players,
    load_rosters,
    load_rosters_weekly,
    load_schedules,
    load_snap_counts,
    load_team_stats,
    load_teams,
    load_trades,
)
from tests.conftest import skip_if_no_live

# ---------------------------------------------------------------------------
# Identity checks — every alias is its canonical function
# ---------------------------------------------------------------------------


def test_alias_load_pbp_is_canonical():
    assert load_pbp is load_nfl_pbp


def test_alias_load_schedules_is_canonical():
    # Note the singular -> plural rename: sdv-py uses ``load_nfl_schedule``
    # (singular) but nflreadpy uses ``load_schedules`` (plural). The alias
    # bridges the gap.
    assert load_schedules is load_nfl_schedule


def test_alias_load_player_stats_is_canonical():
    assert load_player_stats is load_nfl_player_stats


def test_alias_load_team_stats_is_canonical():
    assert load_team_stats is load_nfl_team_stats


def test_alias_load_rosters_is_canonical():
    assert load_rosters is load_nfl_rosters


def test_alias_load_rosters_weekly_is_canonical():
    # Word-order change: sdv-py ``load_nfl_weekly_rosters`` -> nflreadpy
    # ``load_rosters_weekly``.
    assert load_rosters_weekly is load_nfl_weekly_rosters


def test_alias_load_participation_is_canonical():
    # Word-order / shortening: sdv-py ``load_nfl_pbp_participation`` ->
    # nflreadpy ``load_participation``.
    assert load_participation is load_nfl_pbp_participation


def test_alias_load_nextgen_stats_is_canonical():
    assert load_nextgen_stats is load_nfl_nextgen_stats


def test_alias_load_pfr_advstats_is_canonical():
    assert load_pfr_advstats is load_nfl_pfr_advstats


def test_alias_load_ftn_charting_is_canonical():
    assert load_ftn_charting is load_nfl_ftn_charting


def test_alias_load_snap_counts_is_canonical():
    assert load_snap_counts is load_nfl_snap_counts


def test_alias_load_players_is_canonical():
    assert load_players is load_nfl_players


def test_alias_load_teams_is_canonical():
    assert load_teams is load_nfl_teams


def test_alias_load_draft_picks_is_canonical():
    assert load_draft_picks is load_nfl_draft_picks


def test_alias_load_injuries_is_canonical():
    assert load_injuries is load_nfl_injuries


def test_alias_load_contracts_is_canonical():
    assert load_contracts is load_nfl_contracts


def test_alias_load_officials_is_canonical():
    assert load_officials is load_nfl_officials


def test_alias_load_combine_is_canonical():
    assert load_combine is load_nfl_combine


def test_alias_load_depth_charts_is_canonical():
    assert load_depth_charts is load_nfl_depth_charts


def test_alias_load_trades_is_canonical():
    assert load_trades is load_nfl_trades


def test_alias_load_ff_playerids_is_canonical():
    assert load_ff_playerids is load_nfl_ff_playerids


def test_alias_load_ff_rankings_is_canonical():
    assert load_ff_rankings is load_nfl_ff_rankings


def test_alias_load_ff_opportunity_is_canonical():
    assert load_ff_opportunity is load_nfl_ff_opportunity


def test_alias_get_current_season_is_canonical():
    assert get_current_season is get_current_nfl_season


def test_alias_get_current_week_is_canonical():
    assert get_current_week is get_current_nfl_week


# ---------------------------------------------------------------------------
# Bulk count check — make sure every alias from the published map is wired
# ---------------------------------------------------------------------------


_EXPECTED_ALIASES = {
    "load_pbp": "load_nfl_pbp",
    "load_player_stats": "load_nfl_player_stats",
    "load_team_stats": "load_nfl_team_stats",
    "load_schedules": "load_nfl_schedule",
    "load_players": "load_nfl_players",
    "load_rosters": "load_nfl_rosters",
    "load_rosters_weekly": "load_nfl_weekly_rosters",
    "load_snap_counts": "load_nfl_snap_counts",
    "load_nextgen_stats": "load_nfl_nextgen_stats",
    "load_ftn_charting": "load_nfl_ftn_charting",
    "load_participation": "load_nfl_pbp_participation",
    "load_draft_picks": "load_nfl_draft_picks",
    "load_injuries": "load_nfl_injuries",
    "load_contracts": "load_nfl_contracts",
    "load_officials": "load_nfl_officials",
    "load_combine": "load_nfl_combine",
    "load_depth_charts": "load_nfl_depth_charts",
    "load_trades": "load_nfl_trades",
    "load_ff_playerids": "load_nfl_ff_playerids",
    "load_ff_rankings": "load_nfl_ff_rankings",
    "load_ff_opportunity": "load_nfl_ff_opportunity",
    "load_pfr_advstats": "load_nfl_pfr_advstats",
    "load_teams": "load_nfl_teams",
    "get_current_season": "get_current_nfl_season",
    "get_current_week": "get_current_nfl_week",
}


def test_all_24_aliases_resolve_to_their_canonical():
    """All 25 aliases (24 loader + utility, but counted as 25 here including
    both date utilities) point at the canonical function in the
    ``sportsdataverse.nfl`` namespace."""
    # 23 loader aliases + 2 date-utility aliases = 25 entries; the task
    # description rounds to "24" by treating the date utilities as a single
    # bucket. The truth is the map.
    assert len(_EXPECTED_ALIASES) == 25
    for alias_name, canonical_name in _EXPECTED_ALIASES.items():
        alias = getattr(nfl, alias_name)
        canonical = getattr(nfl, canonical_name)
        assert alias is canonical, (
            f"{alias_name} should be the same function object as {canonical_name} but they differ"
        )


def test_canonical_load_nfl_names_still_importable():
    """Sanity guard: the existing ``load_nfl_*`` names must keep working
    after the parity aliases were introduced — the aliases are additive,
    not a replacement."""
    # If any of these became unbound, the import at the top of this file
    # would have already failed; the test is here to make the contract
    # explicit and to catch any future refactor that drops them.
    assert callable(load_nfl_pbp)
    assert callable(load_nfl_schedule)
    assert callable(load_nfl_player_stats)
    assert callable(load_nfl_ff_rankings)
    assert callable(get_current_nfl_season)
    assert callable(get_current_nfl_week)


# ---------------------------------------------------------------------------
# `kind` parameter on load_nfl_ff_rankings
# ---------------------------------------------------------------------------


def test_load_nfl_ff_rankings_kind_param_validates():
    """Passing ``kind`` with an invalid value raises ValueError, same as
    ``type``."""
    with pytest.raises(ValueError, match="type/kind must be one of"):
        load_nfl_ff_rankings(kind="bogus")


def test_load_nfl_ff_rankings_type_param_still_validates():
    """Passing ``type`` with an invalid value still raises (parity preserved)."""
    with pytest.raises(ValueError, match="type/kind must be one of"):
        load_nfl_ff_rankings(type="bogus")


def test_load_nfl_ff_rankings_kind_wins_when_both_supplied():
    """When both ``type`` and ``kind`` are passed, ``kind`` wins. We assert
    this without a network call by leveraging the validator: pass a valid
    ``type`` and an invalid ``kind`` — should raise on the ``kind`` value.
    """
    with pytest.raises(ValueError, match="type/kind must be one of"):
        load_nfl_ff_rankings(type="draft", kind="bogus")


@skip_if_no_live
def test_load_nfl_ff_rankings_kind_returns_same_data_as_type():
    """Live check: ``kind="draft"`` returns the same dataframe shape as
    ``type="draft"``."""
    df_type = load_nfl_ff_rankings(type="draft")
    df_kind = load_nfl_ff_rankings(kind="draft")
    assert isinstance(df_type, pl.DataFrame)
    assert isinstance(df_kind, pl.DataFrame)
    assert df_type.shape == df_kind.shape
    assert df_type.columns == df_kind.columns


@skip_if_no_live
def test_load_nfl_ff_rankings_alias_returns_same_data():
    """Live check: the ``load_ff_rankings`` alias returns the same dataframe
    as the canonical ``load_nfl_ff_rankings``."""
    df_alias = load_ff_rankings(kind="draft")
    df_canonical = load_nfl_ff_rankings(kind="draft")
    assert df_alias.shape == df_canonical.shape
    assert df_alias.columns == df_canonical.columns


# ---------------------------------------------------------------------------
# Drop-in replacement smoke test — nflreadpy-style imports compile
# ---------------------------------------------------------------------------


def test_nflreadpy_style_import_block_compiles():
    """A user porting code from nflreadpy should be able to do

        from sportsdataverse.nfl import (
            load_pbp, load_schedules, load_player_stats, load_team_stats,
            load_rosters, load_rosters_weekly, load_snap_counts,
            load_nextgen_stats, load_pfr_advstats, load_participation,
            load_ftn_charting, load_injuries, load_contracts, load_officials,
            load_combine, load_depth_charts, load_trades, load_ff_playerids,
            load_ff_rankings, load_ff_opportunity, load_teams, load_players,
            load_draft_picks, get_current_season, get_current_week,
        )

    and have everything resolve. The imports at the top of THIS file already
    cover every alias, so if this module loaded at all, the import surface
    is intact. We assert callability here as a belt-and-suspenders check.
    """
    callables = [
        load_pbp,
        load_schedules,
        load_player_stats,
        load_team_stats,
        load_rosters,
        load_rosters_weekly,
        load_snap_counts,
        load_nextgen_stats,
        load_pfr_advstats,
        load_participation,
        load_ftn_charting,
        load_injuries,
        load_contracts,
        load_officials,
        load_combine,
        load_depth_charts,
        load_trades,
        load_ff_playerids,
        load_ff_rankings,
        load_ff_opportunity,
        load_teams,
        load_players,
        load_draft_picks,
        get_current_season,
        get_current_week,
    ]
    assert len(callables) == 25
    for fn in callables:
        assert callable(fn)

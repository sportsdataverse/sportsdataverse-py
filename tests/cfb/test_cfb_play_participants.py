"""Smoke tests for sportsdataverse.cfb.cfb_play_participants."""

from __future__ import annotations

import polars as pl

from sportsdataverse.cfb import espn_cfb_play_participants
from tests.conftest import skip_if_no_live

# Game id used across the suite. CFP semifinal Notre Dame vs Penn State,
# a high-snap-count game with a comprehensive participants payload.
GAME_ID: int = 401628334

# Columns the wrapper must always emit when ESPN ships any participant data
# at all. Endpoint-specific columns (e.g. ``returner_player_name``) are
# only present in seasons that recorded that participant type, so we test
# only the universally-present pair plus the meta columns.
CORE_COLUMNS: set[str] = {
    "game_id",
    "play_id",
    "passer_player_name",
    "passer_player_id",
    "rusher_player_name",
    "rusher_player_id",
    "receiver_player_name",
    "receiver_player_id",
}


@skip_if_no_live
def test_espn_cfb_play_participants_returns_polars_with_core_columns():
    df = espn_cfb_play_participants(game_id=GAME_ID)
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] > 100  # full game has 150+ plays
    missing = CORE_COLUMNS - set(df.columns)
    assert not missing, f"missing columns: {missing}"
    # Each row must correspond to a unique play_id.
    assert df["play_id"].n_unique() == df.shape[0]
    assert df["game_id"].unique().to_list() == [GAME_ID]


@skip_if_no_live
def test_espn_cfb_play_participants_raw_returns_dict():
    raw = espn_cfb_play_participants(game_id=GAME_ID, raw=True)
    assert isinstance(raw, dict)
    assert "items" in raw
    assert isinstance(raw["items"], list)
    assert len(raw["items"]) > 0
    # Each item should have the canonical participant shape.
    sample = next((p for p in raw["items"] if p.get("participants")), None)
    assert sample is not None, "no items with participants in raw payload"
    assert "id" in sample


@skip_if_no_live
def test_espn_cfb_play_participants_return_as_pandas_returns_pandas():
    import pandas as pd

    df = espn_cfb_play_participants(game_id=GAME_ID, return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "passer_player_name" in df.columns


@skip_if_no_live
def test_espn_cfb_play_participants_historical_2014_coverage():
    """Smoke check that 2014 (the earliest reliable ESPN CFB PBP season)
    still returns participant data — the regex extractor we are replacing
    only worked when the freeform play text matched its hand-rolled
    patterns, so confirming the participants endpoint covers the whole
    historical span guards against silently dropping coverage on
    pre-modern games."""
    df = espn_cfb_play_participants(game_id=400547765)
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] > 100
    assert "passer_player_name" in df.columns


@skip_if_no_live
def test_split_sack_emits_list_column_with_both_sackers():
    """Multi-entry participant types must surface every occurrence via
    the list family.

    Game 401411109 (FSU @ Louisville 2022) carries the
    ``"Tate Rodemaker sacked by Yasir Abdullah and YaYa Diaby"`` split
    sack on play 401411109104858801, where ESPN ships
    ``participants = ['passer', 'sackedBy', 'sackedBy']``. The legacy
    ``_pivot_wide`` scalar family collapses to first-only; the new list
    family preserves the full sequence so downstream code can read the
    second sacker via ``list.get(1)`` without falling back to a regex
    over ``cleaned_text``.

    Athlete-id list is the primary integrity check (the participants
    payload always carries the ``$ref``); the name list can have a
    ``None`` entry when the playbyplay sidecar lookup misses an older
    athlete — that's a known limitation of the sidecar approach,
    handled by the regex fallback in ``cfb_pbp.__add_player_cols``.
    """
    df = espn_cfb_play_participants(game_id=401411109)
    assert isinstance(df, pl.DataFrame)
    assert "sacked_by_player_names" in df.columns, (
        "Expected list-family column ``sacked_by_player_names`` from the second pivot pass — schema regression."
    )
    assert "sacked_by_player_ids" in df.columns

    target_play_id = 401411109104858801
    target = df.filter(pl.col("play_id") == target_play_id)
    if target.height == 0:
        # The fixture play id can drift if ESPN re-indexes; accept any
        # split-sack row as a fallback to keep the assertion durable.
        target = df.filter(pl.col("sacked_by_player_ids").list.len() >= 2)
        assert target.height >= 1, (
            "No split-sack rows found in 401411109; the participants list "
            "pivot is failing to retain duplicate ``sackedBy`` entries."
        )
    ids = target["sacked_by_player_ids"][0].to_list()
    assert len(ids) >= 2, f"Expected the list pivot to retain BOTH sacker athlete ids; got {ids}."
    assert all(i is not None for i in ids), f"Athlete ids must always be populated (sourced from $ref); got {ids}."

    # Names list must be the same length as the id list, even if a name
    # itself is None for an athlete missing from the sidecar.
    names = target["sacked_by_player_names"][0].to_list()
    assert len(names) == len(ids)

    # When the sidecar IS complete for the target game, the canonical
    # names show up. We assert at least the first one universally, and
    # check the second only if the sidecar covered both athletes (the
    # ``$ref`` resolution pass should fill it on the default code path,
    # but the assertion stays tolerant of an ESPN outage).
    assert names[0] == "Yasir Abdullah"
    if names[1] is not None:
        # ESPN's canonical ``displayName`` is "Yaya Diaby" (lowercase
        # second 'y'); the legacy roster strings used "YaYa Diaby". We
        # accept either form here so the test survives whichever path
        # filled the slot (sidecar vs $ref resolution).
        assert names[1].lower() == "yaya diaby"

    # Scalar column still carries the first sacker for backwards compat.
    assert target["sacked_by_player_name"][0] == names[0]


@skip_if_no_live
def test_single_participant_type_emits_single_element_list():
    """A play with exactly one participant of a given type must surface
    a length-1 list, never null. The list family pre-fills empty lists
    for missing types so consumers can call ``list.get(i)`` /
    ``list.len()`` without per-play null guards."""
    df = espn_cfb_play_participants(game_id=GAME_ID)
    assert "passer_player_names" in df.columns

    passer_rows = df.filter(pl.col("passer_player_name").is_not_null())
    assert passer_rows.height > 0

    # Lengths must match between the scalar and list column on every
    # passer-bearing play (one passer per pass, ESPN-canonical).
    lens = passer_rows["passer_player_names"].list.len()
    assert (lens == 1).all(), (
        "Expected every passer-bearing play to carry exactly one entry in ``passer_player_names``."
    )

    # And on plays with no passer the list family must surface an empty
    # list rather than null (downstream contract).
    non_passer = df.filter(pl.col("passer_player_name").is_null())
    if non_passer.height > 0:
        assert (non_passer["passer_player_names"].list.len() == 0).all()


# ---------------------------------------------------------------------------
# `$ref` resolution backfill (resolve_missing=True/False)
# ---------------------------------------------------------------------------


@skip_if_no_live
def test_resolve_missing_recovers_sidecar_omissions():
    """Default ``resolve_missing=True`` should backfill the second sacker's
    name on the FSU split-sack play, where the ``cdn.espn.com`` sidecar
    omits ``id=4686334`` (Yaya Diaby) but ESPN's per-athlete ``$ref`` URL
    resolves cleanly. The resolution writes back into the long frame
    before the pivot so BOTH the scalar and list families inherit the
    filled name."""
    df = espn_cfb_play_participants(game_id=401411109)
    target = df.filter(pl.col("play_id") == 401411109104858801)
    if target.height == 0:
        target = df.filter(pl.col("sacked_by_player_ids").list.len() >= 2)
        assert target.height >= 1
    names = target["sacked_by_player_names"][0].to_list()
    ids = target["sacked_by_player_ids"][0].to_list()
    assert len(ids) >= 2
    # With resolution on, both names must be populated.
    assert names[0] is not None, f"First sacker name must be populated; got {names} for ids {ids}."
    assert names[1] is not None, f"Resolution should fill the second sacker name; got {names} for ids {ids}."
    assert names[0] == "Yasir Abdullah"
    # ESPN's canonical displayName casing.
    assert names[1].lower() == "yaya diaby"


@skip_if_no_live
def test_resolve_missing_off_preserves_pre_enhancement_behavior():
    """With ``resolve_missing=False``, the second sacker on the split-sack
    play should remain null — the sidecar gap is exposed unmodified, which
    is the pre-enhancement behavior the downstream regex fallback in
    ``cfb_pbp.__add_player_cols`` was written for."""
    df = espn_cfb_play_participants(game_id=401411109, resolve_missing=False)
    target = df.filter(pl.col("play_id") == 401411109104858801)
    if target.height == 0:
        target = df.filter(pl.col("sacked_by_player_ids").list.len() >= 2)
        assert target.height >= 1
    names = target["sacked_by_player_names"][0].to_list()
    ids = target["sacked_by_player_ids"][0].to_list()
    assert len(ids) >= 2
    assert names[0] == "Yasir Abdullah"
    # Sidecar omits id=4686334 — without resolution the slot stays null.
    assert names[1] is None, f"With resolve_missing=False the sidecar gap must remain visible; got {names} (ids {ids})."


@skip_if_no_live
def test_resolve_missing_max_caps_fanout(caplog):
    """Setting ``resolve_missing_max=1`` should resolve at most one
    missing athlete on a game that has multiple gaps, log a warning,
    and leave the surplus athletes with null names. Game 401411109 has
    ~6 unique missing athletes so the cap will be exercised."""
    import logging

    with caplog.at_level(logging.WARNING, logger="sdv.cfb.cfb_play_participants"):
        df = espn_cfb_play_participants(game_id=401411109, resolve_missing_max=1)

    # The cap warning must be emitted.
    assert any("max_fetches=1" in r.message for r in caplog.records), (
        f"Expected a fan-out cap warning; got: {[r.message for r in caplog.records]}"
    )

    # And at least one scalar gap must remain (since the game has more
    # missing athletes than the cap allows).
    families = ("sacked_by", "kicker", "forced_by", "tackler")
    remaining_gaps = 0
    for fam in families:
        nc, ic = f"{fam}_player_name", f"{fam}_player_id"
        if nc in df.columns and ic in df.columns:
            remaining_gaps += df.filter(pl.col(nc).is_null() & pl.col(ic).is_not_null()).height
    assert remaining_gaps > 0, f"Expected unresolved gaps after capping fan-out at 1; got {remaining_gaps}."

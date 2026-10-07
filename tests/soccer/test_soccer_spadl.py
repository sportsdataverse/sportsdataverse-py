"""SPADL conversion: vocabularies, mapping, post-processing and parity with the socceraction oracle."""

from __future__ import annotations

import dataclasses
import os
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from sportsdataverse.soccer import soccer_spadl as spadl_fn
from sportsdataverse.soccer import spadl
from sportsdataverse.soccer.soccer_events import _stamp_game_id

from tests.conftest import skip_if_no_live

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
EVENTS = FIXTURES / "kloppy" / "statsbomb_8658_events.json"
LINEUPS = FIXTURES / "kloppy" / "statsbomb_8658_lineups.json"
ORACLE = FIXTURES / "socceraction" / "8658_spadl.csv"


def test_vocabularies_are_socceraction_verbatim() -> None:
    assert spadl.FIELD_LENGTH == 105.0 and spadl.FIELD_WIDTH == 68.0
    assert spadl.BODYPARTS == ("foot", "head", "other", "head/other", "foot_left", "foot_right")
    assert spadl.RESULTS == ("fail", "success", "offside", "owngoal", "yellow_card", "red_card")
    assert spadl.ACTIONTYPES == (
        "pass",
        "cross",
        "throw_in",
        "freekick_crossed",
        "freekick_short",
        "corner_crossed",
        "corner_short",
        "take_on",
        "foul",
        "tackle",
        "interception",
        "shot",
        "shot_penalty",
        "shot_freekick",
        "keeper_save",
        "keeper_claim",
        "keeper_punch",
        "keeper_pick_up",
        "clearance",
        "bad_touch",
        "non_action",
        "dribble",
        "goalkick",
    )
    assert spadl.SPADL_COLUMNS == (
        "game_id",
        "original_event_id",
        "action_id",
        "period_id",
        "time_seconds",
        "team_id",
        "player_id",
        "start_x",
        "start_y",
        "end_x",
        "end_y",
        "bodypart_id",
        "bodypart_name",
        "type_id",
        "type_name",
        "result_id",
        "result_name",
    )
    assert spadl.SPADL_SCHEMA["game_id"] == pl.Utf8 and spadl.SPADL_SCHEMA["action_id"] == pl.Int64
    assert spadl.SPADL_SCHEMA["start_x"] == pl.Float64


statsbomb = pytest.importorskip("kloppy.statsbomb")


def _with_game_id(ds: Any) -> Any:
    """File-loaded kloppy datasets carry no game_id; the fixtures are match 8658."""
    return _stamp_game_id(ds, 8658)


def _dataset() -> Any:
    return _with_game_id(statsbomb.load(event_data=str(EVENTS), lineup_data=str(LINEUPS)))


def _kd() -> Any:
    import kloppy.domain as kd

    return kd


def test_parse_event_maps_the_first_pass_of_the_fixture() -> None:
    kd = _kd()
    ds = _dataset()
    first_pass = next(e for e in ds.events if e.event_type == kd.EventType.PASS)
    t, r, b = spadl._parse_event(first_pass, kd)
    assert spadl.ACTIONTYPES[t] in {
        "pass",
        "cross",
        "freekick_short",
        "freekick_crossed",
        "corner_short",
        "corner_crossed",
        "throw_in",
        "goalkick",
    }
    assert spadl.RESULTS[r] in {"fail", "success", "offside"}
    assert spadl.BODYPARTS[b] in spadl.BODYPARTS


def test_parse_event_maps_every_fixture_event_into_the_vocabularies() -> None:
    kd = _kd()
    for e in _dataset().events:
        t, r, b = spadl._parse_event(e, kd)
        assert 0 <= t < len(spadl.ACTIONTYPES) and 0 <= r < len(spadl.RESULTS) and 0 <= b < len(spadl.BODYPARTS)


def test_parse_event_non_action_for_substitutions() -> None:
    kd = _kd()
    ds = _dataset()
    subs = [e for e in ds.events if e.event_type == kd.EventType.SUBSTITUTION]
    if not subs:
        pytest.skip("fixture has no substitution")
    t, r, b = spadl._parse_event(subs[0], kd)
    assert spadl.ACTIONTYPES[t] == "non_action"


def test_end_location_of_a_pass_is_the_receiver_location() -> None:
    kd = _kd()
    ds = _dataset()
    p = next(e for e in ds.events if e.event_type == kd.EventType.PASS and e.receiver_coordinates)
    assert spadl._end_location(p, kd) == (p.receiver_coordinates.x, p.receiver_coordinates.y)


def test_end_location_falls_back_to_the_start() -> None:
    kd = _kd()
    ds = _dataset()
    e = next(
        e
        for e in ds.events
        if e.event_type not in (kd.EventType.PASS, kd.EventType.CARRY, kd.EventType.SHOT) and e.coordinates
    )
    assert spadl._end_location(e, kd) == (e.coordinates.x, e.coordinates.y)


def test_soccer_spadl_schema_and_sort_order() -> None:
    df = spadl.soccer_spadl(_dataset())
    assert isinstance(df, pl.DataFrame)
    assert tuple(df.columns) == spadl.SPADL_COLUMNS
    assert {c: df.schema[c] for c in ("game_id", "team_id", "player_id", "action_id", "start_x")} == {
        "game_id": pl.Utf8,
        "team_id": pl.Utf8,
        "player_id": pl.Utf8,
        "action_id": pl.Int64,
        "start_x": pl.Float64,
    }
    assert df["action_id"].to_list() == list(range(df.height))
    assert "non_action" not in df["type_name"].to_list()
    assert df.sort(["period_id", "time_seconds", "action_id"])["action_id"].to_list() == df["action_id"].to_list()


def test_coordinates_are_on_the_105_by_68_pitch() -> None:
    df = spadl.soccer_spadl(_dataset()).drop_nulls(["start_x", "start_y"])
    assert df["start_x"].min() >= 0 and df["start_x"].max() <= 105
    assert df["start_y"].min() >= 0 and df["start_y"].max() <= 68


def test_null_coordinates_stay_null() -> None:
    kd = _kd()
    ds = _dataset()
    has_none = [e for e in ds.events if e.coordinates is None]
    df = spadl.soccer_spadl(ds)
    if has_none:
        ids = {e.event_id for e in has_none}
        sub = df.filter(pl.col("original_event_id").is_in(list(ids)))
        assert sub["start_x"].is_null().all()
    assert df["start_x"].null_count() == len(
        [
            e
            for e in ds.events
            if e.coordinates is None and spadl.ACTIONTYPES[spadl._parse_event(e, kd)[0]] != "non_action"
        ]
    )


def test_period_ids_pass_through_and_direction_is_constant() -> None:
    df = spadl.soccer_spadl(_dataset())
    assert set(df["period_id"].unique().to_list()) <= {1, 2, 3, 4, 5}
    # every team attacks left to right in every period: shots start in the attacking half far more often than not
    shots = df.filter(pl.col("type_name") == "shot")
    if shots.height:
        assert (shots["start_x"] > 52.5).mean() > 0.9


def test_other_provider_warns_and_returns(caplog: pytest.LogCaptureFixture) -> None:
    ds = _dataset()
    kd = _kd()
    ds.metadata = dataclasses.replace(ds.metadata, provider=kd.Provider.OPTA)
    with caplog.at_level("WARNING"):
        df = spadl.soccer_spadl(ds)
    assert df.height > 0 and "opta" in caplog.text.lower()


def test_empty_dataset_returns_the_documented_schema() -> None:
    out = spadl.soccer_spadl(_dataset().filter(lambda e: False))
    assert out.height == 0 and tuple(out.columns) == spadl.SPADL_COLUMNS
    assert out.schema["action_id"] == pl.Int64


@skip_if_no_live
def test_live_full_match_height() -> None:
    from sportsdataverse.soccer import soccer_open_dataset

    oracle = pl.read_csv(ORACLE)
    out = spadl.soccer_spadl(soccer_open_dataset("statsbomb", 8658))
    assert abs(out.height - oracle.height) <= 0.02 * oracle.height


def test_soccer_open_dataset_stamps_the_match_id(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.soccer import soccer_open_dataset

    def fake(match_id: Any, **_: Any) -> Any:
        return statsbomb.load(event_data=str(EVENTS), lineup_data=str(LINEUPS))

    monkeypatch.setattr(statsbomb, "load_open_data", fake)
    ds = soccer_open_dataset("statsbomb", 8658)
    assert ds.metadata.game_id == "8658"
    assert spadl.soccer_spadl(ds)["game_id"].unique().to_list() == ["8658"]


def test_public_function_is_reexported_from_the_package() -> None:
    assert spadl_fn is spadl.soccer_spadl


def test_game_id_argument_and_metadata() -> None:
    df = spadl.soccer_spadl(_dataset(), game_id=8658)
    assert df["game_id"].unique().to_list() == ["8658"]


def test_fix_clearances_uses_the_next_start() -> None:
    df = pl.DataFrame(
        {
            "game_id": ["g", "g"],
            "team_id": ["a", "a"],
            "period_id": [1, 1],
            "time_seconds": [1.0, 2.0],
            "type_id": [spadl._TYPE["clearance"], spadl._TYPE["pass"]],
            "start_x": [10.0, 40.0],
            "start_y": [5.0, 30.0],
            "end_x": [10.0, 60.0],
            "end_y": [5.0, 31.0],
        }
    )
    out = spadl._fix_clearances(df)
    assert out["end_x"].to_list() == [40.0, 60.0] and out["end_y"].to_list() == [30.0, 31.0]


def test_fix_clearances_mirrors_the_other_teams_next_start() -> None:
    df = pl.DataFrame(
        {
            "game_id": ["g", "g"],
            "team_id": ["a", "b"],
            "type_id": [spadl._TYPE["clearance"], spadl._TYPE["pass"]],
            "start_x": [10.0, 40.0],
            "start_y": [5.0, 30.0],
            "end_x": [10.0, 60.0],
            "end_y": [5.0, 31.0],
        }
    )
    out = spadl._fix_clearances(df)
    assert out["end_x"][0] == 105.0 - 40.0 and out["end_y"][0] == 68.0 - 30.0


def test_add_dribbles_inserts_a_synthetic_carry() -> None:
    base = {
        "game_id": ["g", "g"],
        "original_event_id": ["a", "b"],
        "action_id": [0, 1],
        "period_id": [1, 1],
        "time_seconds": [10.0, 14.0],
        "team_id": ["t", "t"],
        "player_id": ["p", "p"],
        "start_x": [10.0, 20.0],
        "start_y": [10.0, 10.0],
        "end_x": [12.0, 30.0],
        "end_y": [10.0, 10.0],
        "bodypart_id": [0, 0],
        "type_id": [spadl._TYPE["pass"], spadl._TYPE["pass"]],
        "result_id": [1, 1],
    }
    out = spadl._add_dribbles(pl.DataFrame(base))
    assert out.height == 3
    mid = out.row(1, named=True)
    assert mid["type_id"] == spadl._TYPE["dribble"] and mid["time_seconds"] == 12.0
    assert (mid["start_x"], mid["end_x"]) == (12.0, 20.0)
    assert out["action_id"].to_list() == [0, 1, 2]


def test_add_dribbles_skips_any_shot_and_any_headed_action() -> None:
    def frame(next_type: str, next_body: str) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "game_id": ["g", "g"],
                "original_event_id": ["a", "b"],
                "action_id": [0, 1],
                "period_id": [1, 1],
                "time_seconds": [10.0, 14.0],
                "team_id": ["t", "t"],
                "player_id": ["p", "p"],
                "start_x": [10.0, 20.0],
                "start_y": [10.0, 10.0],
                "end_x": [12.0, 30.0],
                "end_y": [10.0, 10.0],
                "bodypart_id": [0, spadl._BODYPART[next_body]],
                "type_id": [spadl._TYPE["pass"], spadl._TYPE[next_type]],
                "result_id": [1, 1],
            }
        )

    assert spadl._add_dribbles(frame("shot", "foot")).height == 2  # any shot
    assert spadl._add_dribbles(frame("pass", "head")).height == 2  # any headed action
    assert spadl._add_dribbles(frame("pass", "foot")).height == 3


def test_add_dribbles_respects_the_thresholds() -> None:
    base = {
        "game_id": ["g", "g"],
        "original_event_id": ["a", "b"],
        "action_id": [0, 1],
        "period_id": [1, 1],
        "time_seconds": [10.0, 14.0],
        "team_id": ["t", "t"],
        "player_id": ["p", "p"],
        "start_x": [10.0, 13.0],
        "start_y": [10.0, 10.0],
        "end_x": [12.0, 30.0],
        "end_y": [10.0, 10.0],
        "bodypart_id": [0, 0],
        "type_id": [spadl._TYPE["pass"], spadl._TYPE["pass"]],
        "result_id": [1, 1],
    }
    assert spadl._add_dribbles(pl.DataFrame(base)).height == 2  # 1 m apart: below MIN_DRIBBLE_LENGTH


def test_parity_with_the_socceraction_oracle() -> None:
    """Direct-converter oracle (spec §4): >= 99 % agreement on types/results/bodyparts and coordinates."""
    ours = spadl.soccer_spadl(_dataset_full())
    oracle = pl.read_csv(
        ORACLE,
        schema_overrides={"game_id": pl.Utf8, "original_event_id": pl.Utf8, "team_id": pl.Utf8, "player_id": pl.Utf8},
    )
    # rows with an event id: every real event, StatsBomb carries (dribbles) included; synthetic dribbles have none
    ours_id = ours.drop_nulls("original_event_id")
    oracle_id = oracle.drop_nulls("original_event_id")
    ours_only = sorted(set(ours_id["original_event_id"]) - set(oracle_id["original_event_id"]))
    oracle_only = sorted(set(oracle_id["original_event_id"]) - set(ours_id["original_event_id"]))
    assert not oracle_only, oracle_only[:20]
    assert ours_only == sorted(_PARITY_OURS_ONLY), ours_only[:20]
    # the oracle itself splits the interception-pass into two rows sharing one id, so those are not extra ours-only rows
    shared = oracle_id.height - oracle_id["original_event_id"].n_unique()
    assert ours.height == oracle.height + len(_PARITY_OURS_ONLY) - shared
    real = ours_id.join(oracle_id, on="original_event_id", suffix="_o", how="inner")
    assert real.height >= 0.98 * oracle_id.height
    agree = (
        (pl.col("type_name") == pl.col("type_name_o"))
        & (pl.col("result_name") == pl.col("result_name_o"))
        & (pl.col("bodypart_name") == pl.col("bodypart_name_o"))
    )
    coords = (
        ((pl.col("start_x") - pl.col("start_x_o")).abs() < 1e-6)
        & ((pl.col("start_y") - pl.col("start_y_o")).abs() < 1e-6)
        & ((pl.col("end_x") - pl.col("end_x_o")).abs() < 1e-6)
        & ((pl.col("end_y") - pl.col("end_y_o")).abs() < 1e-6)
    ).fill_null(True)
    disagree = real.filter(~(agree & coords))
    allowed = set(_PARITY_ALLOWLIST)
    unexpected = sorted(set(disagree["original_event_id"].to_list()) - allowed)
    assert real.filter(agree & coords).height >= 0.99 * real.height, unexpected[:20]
    assert not unexpected, unexpected[:20]
    assert (real["time_seconds"] - real["time_seconds_o"]).abs().max() < 1e-3

    # synthetic dribbles (no event id): pair each with the id of the row before it and compare in place
    def synthetic(df: pl.DataFrame) -> pl.DataFrame:
        prev = df.sort("action_id").with_columns(pl.col("original_event_id").shift(1).alias("prev_id"))
        return prev.filter(pl.col("original_event_id").is_null()).unique("prev_id", keep="first")

    pairs = synthetic(ours).join(synthetic(oracle), on="prev_id", suffix="_o", how="inner")
    assert synthetic(ours).height == synthetic(oracle).height == pairs.height
    assert pairs.height == 36  # measured 2026-10-07 — a change here means the dribble rule changed
    for c in ("start_x", "start_y", "end_x", "end_y", "time_seconds"):
        assert (pairs[c] - pairs[c + "_o"]).abs().max() < 1e-6, c
    assert pairs["player_id"].to_list() == pairs["player_id_o"].to_list()


# original_event_ids where kloppy's StatsBomb deserializer and statsbombpy legitimately differ. Grows only with a ledger entry.
_PARITY_ALLOWLIST: tuple[str, ...] = (
    # StatsBomb Pass with pass.type "Interception": statsbombpy -> interception; kloppy emits a plain PASS (the pass type is not kept)
    "a1abd2bb-d135-4622-b467-bebafe3fd845",
    # StatsBomb Goal Keeper "Keeper Sweeper" with outcome Claim: statsbombpy -> keeper_claim; kloppy's sweeper action maps to keeper_pick_up
    "a1fe186c-ed79-44e4-9d13-5b2ef8326344",
)


# Non-dribble rows kloppy emits that the oracle lacks. Same rule: only genuine kloppy-vs-statsbombpy differences, one reason each.
_PARITY_OURS_ONLY: dict[str, str] = {
    # kloppy splits a StatsBomb Pass with pass.type "Interception" into a pass plus a synthetic interception event
    "interception-a1abd2bb-d135-4622-b467-bebafe3fd845": "kloppy interception-split of a pass",
    # StatsBomb pass outcome "Injury Clearance": socceraction -> non_action; kloppy maps it to PassResult.OUT -> pass/fail
    "8de77d94-e304-4d75-8c09-a3befaa8a391": "Injury Clearance pass kept as pass/fail by kloppy",
    "e14ae27f-6483-4182-9b30-74f22541aec3": "Injury Clearance pass kept as pass/fail by kloppy",
}

_FULL_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"


def _dataset_full() -> Any:
    """The full 8658 match from a gitignored cache, downloaded once (two requests) under SDV_PY_LIVE_TESTS=1."""
    cache = FIXTURES / "socceraction" / "_8658_full"
    ev, lu = cache / "events.json", cache / "lineups.json"
    if not (ev.exists() and lu.exists()):
        if os.environ.get("SDV_PY_LIVE_TESTS") != "1":
            pytest.skip("full-match events not cached; run with SDV_PY_LIVE_TESTS=1 once to populate (gitignored)")
        from sportsdataverse.dl_utils import download

        cache.mkdir(parents=True, exist_ok=True)
        try:
            for kind, path in (("lineups", lu), ("events", ev)):  # events.json is the completion sentinel
                r = download(f"{_FULL_URL}/{kind}/8658.json", timeout=60)
                r.raise_for_status()
                path.write_bytes(r.content)
        except Exception as exc:  # noqa: BLE001 - any fetch failure skips rather than erroring the module
            ev.unlink(missing_ok=True)
            pytest.skip(f"could not download the full 8658 match: {exc}")
    return _with_game_id(statsbomb.load(event_data=str(ev), lineup_data=str(lu)))

"""Offline `$ref` backfill of `espn_play_participants` (supp-007): the cap and the swallowed failure.

Every test of the network path used to be `@skip_if_no_live`. Here the real NFL game 401872922 drives the
public function with its three network seams replaced by committed captures: the Core v2 play items
(`tests/nfl/fixtures/plays_401872922.json.gz`), the summary's athlete lookup (`summary_401872922.json`) and
the `$ref` payloads of the 7 athletes that lookup misses (`athlete_refs_401872922.json.gz`, captured
2026-10-11 from `sports.core.api.espn.com`, one request each; README in that directory).
"""

from __future__ import annotations

import gzip
import importlib
import json
import logging
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.errors import AssetFetchError

mod = importlib.import_module("sportsdataverse.football.play_participants")
FIX = Path(__file__).resolve().parents[1] / "nfl" / "fixtures"
GAME = 401872922


def _gz(name: str):
    with gzip.open(FIX / name, "rt", encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="module")
def captures():
    summary = json.loads((FIX / f"summary_{GAME}.json").read_text(encoding="utf-8"))
    return (
        _gz(f"plays_{GAME}.json.gz")["items"],
        mod.athlete_lookup_from_summary(summary),
        _gz(f"athlete_refs_{GAME}.json.gz"),
    )


def _run(monkeypatch, captures, *, fail: set[str] = frozenset(), **kw):
    items, lookup, refs = captures
    fetched: list[str] = []
    seen: dict[str, pl.DataFrame] = {}

    def fake_json(_download, url, **_kw):
        fetched.append(url)
        if url in fail:
            raise AssetFetchError(f"offline test: {url} answered HTTP 503")
        return refs[url]

    def spy_pivot(long):
        seen["long"] = long
        return pivot(long)

    pivot = mod._pivot_wide
    monkeypatch.setattr(mod, "_download_plays", lambda game_id, league, **k: items)
    monkeypatch.setattr(mod, "_download_athlete_lookup", lambda game_id, league, **k: lookup)
    monkeypatch.setattr(mod, "_download_json", fake_json)
    monkeypatch.setattr(mod, "_pivot_wide", spy_pivot)
    wide = mod.espn_play_participants(GAME, league="nfl", **kw)
    long = seen["long"]
    unnamed = long.filter(pl.col("athlete_id").is_not_null() & pl.col("player_name").is_null())
    return wide, fetched, sorted(set(unnamed["athlete_id"].to_list()))


def test_every_sidecar_gap_is_filled_from_its_ref(monkeypatch, captures):
    _, _, refs = captures
    wide, fetched, unnamed = _run(monkeypatch, captures)
    assert sorted(fetched) == sorted(refs) and len(fetched) == 7
    assert unnamed == []
    names = {v for c in wide.columns if c.endswith("_player_name") for v in wide[c].drop_nulls().to_list()}
    assert {p["displayName"] for p in refs.values()} <= names
    print("PARTICIPANTS_REF_BACKFILL_OK", len(fetched))


def test_the_cap_stops_at_max_fetches_and_says_so(monkeypatch, captures, caplog):
    with caplog.at_level(logging.WARNING, logger=mod.logger.name):
        _, fetched, unnamed = _run(monkeypatch, captures, resolve_missing_max=3)
    assert len(fetched) == 3 and len(unnamed) == 4
    assert "7 unique athletes need $ref resolution but max_fetches=3" in caplog.text


def test_a_failed_ref_is_logged_and_only_that_athlete_stays_unnamed(monkeypatch, captures, caplog):
    _, _, refs = captures
    bad = next(iter(refs))
    bad_id = bad.split("/athletes/")[1].split("?")[0]
    with caplog.at_level(logging.WARNING, logger=mod.logger.name):
        wide, fetched, unnamed = _run(monkeypatch, captures, fail={bad})
    assert len(fetched) == 7 and unnamed == [bad_id]
    assert f"failed to resolve athlete {bad_id}" in caplog.text
    assert wide.height > 0

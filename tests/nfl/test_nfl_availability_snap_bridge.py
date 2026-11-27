"""Live `load_nfl_snap_counts` frames (PFR ids) reach the availability model (real 2020-2023 fixtures).

`load_nfl_snap_counts` keys players by ``pfr_player_id``; `season_availability` needs the gsis ``player_id``,
so live it found no snap row and `nfl_availability_projection` projected nobody. The fitted corpus
(``snap_counts_2020_2023.parquet``) bridged regular-season rows through `load_nfl_ff_playerids`; the
``*_live_*`` / ``ff_playerids_bridge`` fixtures are those loaders' own rows (``README.md``).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.nfl.nfl_availability as av

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_projection"


@pytest.fixture(scope="module")
def frames() -> dict[str, pl.DataFrame]:
    names = {
        "live": "snap_counts_live_2020_2023",
        "bridge": "ff_playerids_bridge",
        "corpus": "snap_counts_2020_2023",
        "rosters": "rosters_live_2020_2023",
    }
    return {k: pl.read_parquet(FIX / f"{v}.parquet") for k, v in names.items()}


def _key(df: pl.DataFrame) -> pl.DataFrame:
    return df.select("player_id", "season", "week", "offense_snaps").sort("player_id", "season", "week")


def test_the_bridge_rebuilds_the_fitted_corpus(frames):
    bridged = av._snaps_by_gsis(frames["live"], frames["bridge"])
    assert bridged.schema["player_id"] == frames["corpus"].schema["player_id"] == pl.Utf8
    assert set(bridged["season"].unique().to_list()) == {2020, 2021, 2022, 2023}
    corpus = _key(frames["corpus"])
    got = _key(bridged)
    # the corpus was captured 2026-07-08; the crosswalk has moved a few ids since (and ambiguous PFR ids drop)
    missing = corpus.join(got, on=["player_id", "season", "week"], how="anti").height
    extra = got.join(corpus, on=["player_id", "season", "week"], how="anti").height
    assert corpus.height > 80_000
    assert missing < 0.002 * corpus.height and extra < 0.002 * corpus.height


def test_postseason_rows_are_not_counted(frames):
    bridged = av._snaps_by_gsis(frames["live"], frames["bridge"])
    assert frames["live"]["game_type"].n_unique() > 1
    assert bridged["week"].max() <= 18


def test_a_broken_bridge_fails_loudly(frames):
    with pytest.raises(ValueError, match="crosswalk is broken"):
        av._snaps_by_gsis(frames["live"], frames["bridge"].head(100))
    with pytest.raises(ValueError, match="pfr_player_id"):
        av.season_availability(frames["live"], frames["rosters"])


def test_live_availability_matches_the_corpus_path(frames, monkeypatch):
    monkeypatch.setattr(av, "load_nfl_snap_counts", lambda *a, **k: frames["live"], raising=True)
    monkeypatch.setattr(av, "load_nfl_ff_playerids", lambda *a, **k: frames["bridge"], raising=True)
    monkeypatch.setattr(av, "load_nfl_rosters", lambda *a, **k: frames["rosters"], raising=True)
    live = av.nfl_availability_projection([2020, 2021, 2022, 2023], 2024)
    assert live.height > 1000  # live used to project nobody
    monkeypatch.setattr(av, "load_nfl_snap_counts", lambda *a, **k: frames["corpus"], raising=True)
    corpus = av.nfl_availability_projection([2020, 2021, 2022, 2023], 2024)
    both = live.join(corpus, on="player_id", suffix="_corpus")
    assert both.height > 0.99 * corpus.height
    assert (both["proj_availability"] - both["proj_availability_corpus"]).abs().max() < 0.05
    same = both.filter((both["proj_availability"] - both["proj_availability_corpus"]).abs() < 1e-9).height
    assert same > 0.98 * both.height

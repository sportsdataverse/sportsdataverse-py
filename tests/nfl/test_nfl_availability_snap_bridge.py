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
    assert bridged["game_type"].unique().to_list() == ["REG"]


def test_an_ambiguous_mapping_is_dropped_both_ways(frames):
    live, bridge = frames["live"], frames["bridge"]
    base = av._snaps_by_gsis(live, bridge)
    pfr, gsis = bridge.filter(pl.col("pfr_id").is_in(base["pfr_player_id"].implode())).row(0)[:2]
    n_rows = base.filter(pl.col("player_id") == gsis).height
    assert n_rows > 0
    for extra in ({"pfr_id": pfr, "gsis_id": "00-9999999"}, {"pfr_id": "ZzzzZz99", "gsis_id": gsis}):
        dup = pl.concat([bridge, pl.DataFrame([{**extra, "position": "WR"}], schema=bridge.schema)])
        got = av._snaps_by_gsis(live, dup)
        assert got.filter(pl.col("player_id") == gsis).height == 0
        assert got.height == base.height - n_rows


def test_mis_typed_or_unkeyed_snaps_raise(frames):
    live, bridge = frames["live"], frames["bridge"]
    with pytest.raises(TypeError, match="both must be String"):
        av._snaps_by_gsis(live, bridge.with_columns(pl.col("pfr_id").cast(pl.Categorical)))
    with pytest.raises(ValueError, match="neither player_id nor pfr_player_id"):
        av._snaps_by_gsis(live.drop("pfr_player_id"), bridge)


def test_a_broken_bridge_fails_loudly(frames):
    with pytest.raises(ValueError, match="crosswalk is broken"):
        av._snaps_by_gsis(frames["live"], frames["bridge"].head(100))
    # one season's ids gone from the crosswalk fails even though the pooled rate would still pass
    one_season = frames["live"].with_columns(
        pl.when(pl.col("season") == 2023)
        .then(pl.lit("x") + pl.col("pfr_player_id"))
        .otherwise("pfr_player_id")
        .alias("pfr_player_id")
    )
    with pytest.raises(ValueError, match="of 2023 skill-position"):
        av._snaps_by_gsis(one_season, frames["bridge"])
    with pytest.raises(ValueError, match="pfr_player_id"):
        av.season_availability(frames["live"], frames["rosters"])
    bridged = av.season_availability(frames["live"], frames["rosters"], player_ids=frames["bridge"])
    assert bridged.height > 1000


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


def test_snaps_that_count_nobody_raise_in_the_projection(frames, monkeypatch):
    idle = frames["live"].with_columns(pl.lit(0).cast(frames["live"].schema["offense_snaps"]).alias("offense_snaps"))
    monkeypatch.setattr(av, "load_nfl_snap_counts", lambda *a, **k: idle, raising=True)
    monkeypatch.setattr(av, "load_nfl_ff_playerids", lambda *a, **k: frames["bridge"], raising=True)
    monkeypatch.setattr(av, "load_nfl_rosters", lambda *a, **k: frames["rosters"], raising=True)
    with pytest.raises(ValueError, match="gave no season availability"):
        av.nfl_availability_projection([2020, 2021, 2022, 2023], 2024)


def test_one_bad_season_fails_although_the_pooled_rate_passes(frames):
    live, bridge = frames["live"], frames["bridge"]
    skill = (
        (pl.col("game_type") == "REG")
        & pl.col("position").is_in(["QB", "RB", "WR", "TE"])
        & (pl.col("offense_snaps") > 0)
    )
    ids_2023 = live.filter(skill & (pl.col("season") == 2023))["pfr_player_id"].unique().sort()
    gone = set(ids_2023.head(int(0.06 * ids_2023.len())).to_list())  # ~6% of 2023's ids missing from the crosswalk
    broken = live.with_columns(
        pl.when((pl.col("season") == 2023) & pl.col("pfr_player_id").is_in(list(gone)))
        .then(pl.lit("x") + pl.col("pfr_player_id"))
        .otherwise("pfr_player_id")
        .alias("pfr_player_id")
    )
    rows = broken.filter(skill)
    hit = pl.col("pfr_player_id").is_in(bridge["pfr_id"].implode())
    assert rows.select(hit.mean()).item() >= av.SNAP_BRIDGE_MIN_MATCH  # pooled across seasons: passes
    assert rows.filter(pl.col("season") == 2023).select(hit.mean()).item() < av.SNAP_BRIDGE_MIN_MATCH
    with pytest.raises(ValueError, match="crosswalk is broken"):
        av._snaps_by_gsis(broken, bridge)


def test_gsis_keyed_snaps_still_drop_the_postseason(frames):
    live, bridge = frames["live"], frames["bridge"]
    keyed = live.join(bridge.select("pfr_id", "gsis_id"), left_on="pfr_player_id", right_on="pfr_id").rename(
        {"gsis_id": "player_id"}
    )
    assert keyed["game_type"].n_unique() > 1
    got = av._snaps_by_gsis(keyed, bridge)
    assert got["game_type"].unique().to_list() == ["REG"]
    assert got.height == keyed.filter(pl.col("game_type") == "REG").height

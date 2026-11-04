"""Offline round-trip for the generated nba dataset loaders.

The loader *machinery* (404-safe skip, min-season guard, render) is covered
generically in tests/codegen/test_load_module.py. This pins the one loader with
a bespoke published schema -- load_nba_player_impact -- so a releases.yaml /
loader_schemas.yaml drift on it fails here rather than silently at read time.
"""

import polars as pl
import pytest

from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.nba import nba_loaders

# The 25-column nba_player_impact schema the hoopR-nba-stats-data producer
# writes (nba_model_publish.builders.build_nba_player_impact). Kept in lockstep
# with tools/codegen/schemas/loader_schemas.yaml::load_nba_player_impact.
_IMPACT_SCHEMA = {
    "player_id": pl.Int64,
    # Identity columns are part of the published contract: a fixture without them
    # lets the round-trip test pass while the release has changed underneath it.
    "player_name": pl.Utf8,
    "team_id": pl.Int64,
    "team_abbreviation": pl.Utf8,
    "team_name": pl.Utf8,
    "teams": pl.Utf8,
    "o_rapm": pl.Float64,
    "d_rapm": pl.Float64,
    "rapm": pl.Float64,
    "off_poss": pl.Int64,
    "def_poss": pl.Int64,
    "o_adj_rapm": pl.Float64,
    "d_adj_rapm": pl.Float64,
    "adj_rapm": pl.Float64,
    "ospm": pl.Float64,
    "dspm": pl.Float64,
    "spm": pl.Float64,
    "min": pl.Float64,
    "gp": pl.Int64,
    "obpm": pl.Float64,
    "dbpm": pl.Float64,
    "bpm": pl.Float64,
    "war": pl.Float64,
    "boban_filtered_skill": pl.Float64,
    "boban_projected_rating": pl.Float64,
    "boban_projected_sd": pl.Float64,
    "darko_filtered_skill": pl.Float64,
    "darko_projected_rating": pl.Float64,
    "darko_projected_sd": pl.Float64,
    "season": pl.Int64,
}


def _impact_row(season: int) -> pl.DataFrame:
    # Seed each column with a value of its OWN dtype -- a hardcoded 0 cannot build
    # the Utf8 identity columns.
    return pl.DataFrame(
        {c: pl.Series(["x"] if t == pl.Utf8 else [0], dtype=t) for c, t in _IMPACT_SCHEMA.items()}
    ).with_columns(pl.lit(season, dtype=pl.Int64).alias("season"))


def test_load_nba_player_impact_round_trips_schema(monkeypatch):
    # Two published seasons + one missing -> concat of the present ones, 404-safe.
    def fake_read(url):
        return _impact_row(2023) if "2023" in url else (_impact_row(2024) if "2024" in url else None)

    monkeypatch.setattr(nba_loaders, "_read_release_parquet", fake_read)
    with pytest.warns(UserWarning, match=r"no data for season\(s\) \[2019\]"):
        out = nba_loaders.load_nba_player_impact(seasons=[2023, 2024, 2019])
    assert out.height == 2  # 2019 missing -> skipped, not raised
    assert dict(out.schema) == _IMPACT_SCHEMA  # documented schema round-trips exactly


def test_load_nba_player_impact_floors_at_1996():
    with pytest.raises(SeasonNotFoundError):
        nba_loaders.load_nba_player_impact(seasons=1995)


_BOBAN = ["boban_filtered_skill", "boban_projected_rating", "boban_projected_sd"]
_DARKO = [c.replace("boban_", "darko_") for c in _BOBAN]


@pytest.mark.parametrize("shipped, other", [(_DARKO, _BOBAN), (_BOBAN, _DARKO)])
def test_load_nba_player_impact_emits_both_names_whichever_the_release_ships(monkeypatch, shipped, other):
    # Today's release ships darko_*; after the producer rename it ships boban_*. Either way the frame
    # carries both, with equal values, boban_* first -- the one-release deprecation window (BB-D11).
    vals = {"rapm": [0.5], **{c: [1.5 + i] for i, c in enumerate(shipped)}}
    monkeypatch.setattr(nba_loaders, "_read_release_parquet", lambda url: pl.DataFrame(vals))
    out = nba_loaders.load_nba_player_impact(seasons=2024)
    assert out.columns == ["rapm", *_BOBAN, *_DARKO]
    for b, d in zip(_BOBAN, _DARKO):
        assert out[b].to_list() == out[d].to_list() != [None]
    assert out[_BOBAN[1]].to_list() == [2.5]

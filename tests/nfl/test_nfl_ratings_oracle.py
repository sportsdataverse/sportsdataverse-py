"""Phase-1 oracle gates: native NFL ratings vs ESPN FPI + raw team EPA (2023).

Offline -- consumes the committed fixtures under
``tests/fixtures/nfl_prediction/`` (provenance in that directory's README).

Gate rule (binding): floors are set from the observed value at gate time and
are NEVER lowered to make a red gate pass -- debug the model instead.
Observed at fit time (ridge_lambda=25, competitive wp window 0.05-0.95):
``spearman(adj_net, fpi) = 0.8904`` and
``spearman(adj_off_epa, off_epa_per_play) = 0.9652``.
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl.nfl_prediction_constants import spearman_corr
from sportsdataverse.nfl.nfl_ratings import efficiency_ratings, special_teams_ratings

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_prediction"


@pytest.fixture(scope="module")
def oracle_corpus():
    return {
        "pbp": pl.read_parquet(FIXTURES / "pbp_2023_sample.parquet"),
        "fpi": pl.read_parquet(FIXTURES / "fpi_2023.parquet"),
        "team_stats": pl.read_parquet(FIXTURES / "team_stats_2023.parquet"),
    }


def ratings_from_fixture(pbp: pl.DataFrame) -> pl.DataFrame:
    """Full-season efficiency + special-teams ratings, joined on team_id."""
    eff = efficiency_ratings(pbp)
    st = special_teams_ratings(pbp)
    assert eff.schema["team_id"] == st.schema["team_id"] == pl.Utf8
    return eff.join(st, on="team_id", how="left").with_columns(pl.col("adj_st_epa").fill_null(0.0))


def test_adj_net_tracks_fpi(oracle_corpus):
    """Gate: spearman(adj_net, fpi) >= 0.85 (observed 0.8904 on 2023).

    FPI blends preseason priors + QB adjustments, so a pure-EPA rating tracks
    but does not equal it -- the 0.85 floor reflects that documented gap.
    """
    mine = ratings_from_fixture(oracle_corpus["pbp"])
    fpi = oracle_corpus["fpi"]
    j = mine.join(fpi, on="team_id", how="inner")
    assert j.schema["team_id"] == fpi.schema["team_id"] == pl.Utf8
    assert j.height == 32  # every team matched (abbr normalization holds)
    assert spearman_corr(j["adj_net"].to_numpy(), j["fpi"].to_numpy()) >= 0.85


def test_adj_off_tracks_raw_team_epa(oracle_corpus):
    """Gate: spearman(adj_off_epa, raw off_epa_per_play) >= 0.90 (observed 0.9652).

    The opponent adjustment should reorder teams only moderately relative to
    raw (unadjusted) offensive EPA per play -- the internal sanity oracle.
    """
    mine = ratings_from_fixture(oracle_corpus["pbp"])
    raw = oracle_corpus["team_stats"]
    j = mine.join(raw, on="team_id", how="inner")
    assert j.height == 32
    assert spearman_corr(j["adj_off_epa"].to_numpy(), j["off_epa_per_play"].to_numpy()) >= 0.90

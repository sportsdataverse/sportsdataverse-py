"""Unit tests for model (2): matchup defensive RAPM (reuses the RAPM RidgeCV)."""

import polars as pl

from sportsdataverse.nba.nba_matchup_drapm import build_matchup_drapm_design, nba_matchup_drapm


def test_design_shape_and_targets():
    m = pl.DataFrame(
        {
            "off_player_id": [10, 11, 10],
            "def_player_id": [20, 20, 21],
            "partial_poss": [50.0, 40.0, 5.0],  # 3rd row below min_poss -> dropped
            "player_pts": [55.0, 40.0, 6.0],
        }
    )
    X, y, w, dids, oids = build_matchup_drapm_design(m, min_poss=25.0)
    assert X.shape == (2, len(dids) + len(oids))
    assert abs(y[0] - 100 * 55.0 / 50.0) < 1e-9
    assert w.tolist() == [50.0, 40.0]
    assert dids == [20] and oids == [10, 11]


def test_design_empty():
    from scipy.sparse import csr_matrix

    X, y, w, dids, oids = build_matchup_drapm_design(pl.DataFrame())
    assert X.shape == (0, 0)
    assert isinstance(X, csr_matrix)
    assert len(y) == 0 and len(w) == 0 and dids == [] and oids == []


def _synthetic_matchups() -> pl.DataFrame:
    # Defender 20 consistently allows fewer points-per-poss than 21 across
    # common offensive opponents 100/101/102.
    rows = []
    for off in (100, 101, 102):
        rows.append({"off_player_id": off, "def_player_id": 20, "partial_poss": 60.0, "player_pts": 45.0})
        rows.append({"off_player_id": off, "def_player_id": 21, "partial_poss": 60.0, "player_pts": 75.0})
    return pl.DataFrame(rows)


def test_matchup_drapm_sign_and_schema():
    out = nba_matchup_drapm("2023-24", matchups=_synthetic_matchups(), config=None)
    assert out.schema["player_id"] == pl.Int64
    d20 = out.filter(pl.col("player_id") == 20)["matchup_drapm"][0]
    d21 = out.filter(pl.col("player_id") == 21)["matchup_drapm"][0]
    assert d20 > d21


def test_matchup_drapm_pandas_and_empty():
    pdf = nba_matchup_drapm("2023-24", matchups=_synthetic_matchups(), return_as_pandas=True)
    assert type(pdf).__name__ == "DataFrame" and hasattr(pdf, "iloc")

    empty = nba_matchup_drapm("2023-24", matchups=pl.DataFrame())
    assert empty.height == 0
    assert set(empty.columns) == {"player_id", "matchup_drapm", "matchup_poss"}

# tests/nba/test_nba_rapm.py
import polars as pl

from sportsdataverse.nba.nba_rapm import build_rapm_design


def _poss(rows: list[tuple[tuple[int, ...], tuple[int, ...], int]]) -> pl.DataFrame:
    # rows: list of (off5 tuple, def5 tuple, points)
    data = {f"off_player_{i + 1}": [r[0][i] for r in rows] for i in range(5)}
    data.update({f"def_player_{i + 1}": [r[1][i] for r in rows] for i in range(5)})
    data["points"] = [r[2] for r in rows]
    return pl.DataFrame(data)


def test_design_matrix_encoding():
    # 2 possessions, players 1..5 on offense / 11..15 on defense (poss A), reversed (poss B)
    rows = [((1, 2, 3, 4, 5), (11, 12, 13, 14, 15), 2), ((11, 12, 13, 14, 15), (1, 2, 3, 4, 5), 0)]
    X, y, pids = build_rapm_design(_poss(rows))
    assert pids == list(range(1, 6)) + list(range(11, 16))  # sorted distinct, P=10
    P = len(pids)
    assert X.shape == (2, 2 * P)
    Xd = X.toarray()
    i = {p: k for k, p in enumerate(pids)}
    for p in (1, 2, 3, 4, 5):
        assert Xd[0, i[p]] == 1
    for p in (11, 12, 13, 14, 15):
        assert Xd[0, P + i[p]] == 1
    assert Xd[0].sum() == 10  # exactly 5 offense + 5 defense indicators
    assert list(y) == [2.0, 0.0]


def test_design_matrix_empty_input():
    """Empty possessions DataFrame returns the specified zero shapes."""
    empty = pl.DataFrame(
        {
            col: pl.Series([], dtype=pl.Int64)
            for col in [f"off_player_{i}" for i in range(1, 6)] + [f"def_player_{i}" for i in range(1, 6)] + ["points"]
        }
    )
    X, y, pids = build_rapm_design(empty)
    assert pids == []
    assert X.shape == (0, 0)
    assert y.shape == (0,)


def test_design_matrix_shared_players():
    """Players appearing on both offense and defense get separate indicator columns."""
    # Player 1 is on offense in poss 0 and defense in poss 1
    rows = [((1, 2, 3, 4, 5), (6, 7, 8, 9, 10), 1), ((6, 7, 8, 9, 10), (1, 2, 3, 4, 5), 3)]
    X, y, pids = build_rapm_design(_poss(rows))
    P = len(pids)
    Xd = X.toarray()
    idx = {p: k for k, p in enumerate(pids)}
    # Player 1 should have offense=1 in poss 0, defense=1 in poss 1
    assert Xd[0, idx[1]] == 1  # offense col, poss 0
    assert Xd[0, P + idx[1]] == 0  # defense col, poss 0
    assert Xd[1, idx[1]] == 0  # offense col, poss 1
    assert Xd[1, P + idx[1]] == 1  # defense col, poss 1
    assert list(y) == [1.0, 3.0]


def test_design_matrix_drops_null_lineup_rows():
    """A possession with a null lineup cell is dropped, never injecting a phantom id."""
    # Three possessions; the middle one has a null off_player_5 (partial lineup).
    data = {
        "off_player_1": [1, 21, 31],
        "off_player_2": [2, 22, 32],
        "off_player_3": [3, 23, 33],
        "off_player_4": [4, 24, 34],
        "off_player_5": [5, None, 35],  # poss index 1 is partial -> dropped
        "def_player_1": [11, 26, 36],
        "def_player_2": [12, 27, 37],
        "def_player_3": [13, 28, 38],
        "def_player_4": [14, 29, 39],
        "def_player_5": [15, 30, 40],
        "points": [2, 1, 0],
    }
    schema = {
        col: pl.Int64
        for col in [f"off_player_{i}" for i in range(1, 6)] + [f"def_player_{i}" for i in range(1, 6)] + ["points"]
    }
    poss = pl.DataFrame(data, schema_overrides=schema)
    # Confirm the partial column is genuinely nullable Int64 with a null present.
    assert poss.schema["off_player_5"] == pl.Int64
    assert poss["off_player_5"].null_count() == 1

    X, y, pids = build_rapm_design(poss)
    # No phantom / sentinel ids: every id is a real positive player id.
    assert all(p > 0 for p in pids)
    assert -9223372036854775808 not in pids
    # The null-lineup possession (index 1) was dropped: only 2 rows survive.
    assert X.shape[0] == 2
    assert len(y) == 2
    # Each surviving possession encodes exactly 5 offense + 5 defense indicators.
    Xd = X.toarray()
    assert Xd[0].sum() == 10
    assert Xd[1].sum() == 10
    assert list(y) == [2.0, 0.0]

# tests/nba/test_nba_rapm.py
import polars as pl

from sportsdataverse.nba.nba_rapm import build_rapm_design


def _poss(rows):  # rows: list of (off5 tuple, def5 tuple, points)
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

# tests/nba/test_nba_rapm.py
import numpy as np
import polars as pl

from sportsdataverse.nba.nba_rapm import RAPM_SCHEMA, build_rapm_design, nba_rapm


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


# ---------------------------------------------------------------------------
# Task 2: nba_rapm() tests
# ---------------------------------------------------------------------------


def test_rapm_empty_input_returns_schema_frame():
    """nba_rapm on empty input must return zero-row frame with exact RAPM_SCHEMA."""
    # completely empty DataFrame (no columns)
    out = nba_rapm(pl.DataFrame())
    assert out.is_empty()
    assert dict(out.schema) == RAPM_SCHEMA

    # empty but correctly-structured DataFrame
    out2 = nba_rapm(_poss([]))
    assert out2.is_empty()
    assert dict(out2.schema) == RAPM_SCHEMA


def test_rapm_schema_sign_and_counts():
    """Small hand-built frame: verify schema dtypes, sign convention, off/def poss counts."""
    # 4 possessions: players 1-5 offense vs 6-10 defense (twice), then reversed (twice)
    rows = [
        ((1, 2, 3, 4, 5), (6, 7, 8, 9, 10), 2),
        ((1, 2, 3, 4, 5), (6, 7, 8, 9, 10), 2),
        ((6, 7, 8, 9, 10), (1, 2, 3, 4, 5), 1),
        ((6, 7, 8, 9, 10), (1, 2, 3, 4, 5), 1),
    ]
    out = nba_rapm(_poss(rows)).sort("player_id")

    # Exactly the 6 RAPM_SCHEMA columns with correct dtypes
    assert dict(out.schema) == RAPM_SCHEMA

    # rapm == o_rapm + d_rapm row-wise
    computed_rapm = (out["o_rapm"] + out["d_rapm"]).to_numpy()
    stored_rapm = out["rapm"].to_numpy()
    np.testing.assert_allclose(computed_rapm, stored_rapm, atol=1e-10)

    # Players 1-5 were on offense 2× and defense 2×; players 6-10 likewise
    assert (out["off_poss"] == 2).all()
    assert (out["def_poss"] == 2).all()

    # dtype checks
    assert out["player_id"].dtype == pl.Int64
    assert out["o_rapm"].dtype == pl.Float64
    assert out["d_rapm"].dtype == pl.Float64
    assert out["rapm"].dtype == pl.Float64
    assert out["off_poss"].dtype == pl.Int64
    assert out["def_poss"].dtype == pl.Int64


def test_synthetic_recovery():
    """Ridge regression must recover planted per-100-possession effects (corr > 0.7)."""
    rng = np.random.default_rng(42)
    P = 40
    true_off = rng.normal(0, 0.06, P)  # per-possession effects
    true_def = rng.normal(0, 0.06, P)
    players = list(range(1, P + 1))
    M = 8000
    rows = []
    for _ in range(M):
        pick = rng.choice(players, size=10, replace=False)
        off5, def5 = pick[:5], pick[5:]
        oi = [p - 1 for p in off5]
        di = [p - 1 for p in def5]
        pts = 1.05 + true_off[oi].sum() - true_def[di].sum() + rng.normal(0, 0.4)
        pts = int(max(0, round(pts)))  # integer points per possession
        rows.append((tuple(int(x) for x in off5), tuple(int(x) for x in def5), pts))
    df = _poss(rows)
    out = nba_rapm(df).sort("player_id")
    o_est = out["o_rapm"].to_numpy()
    d_est = out["d_rapm"].to_numpy()
    # Ridge shrinks magnitude but must RECOVER the structure
    corr_o = np.corrcoef(o_est, true_off[: len(o_est)])[0, 1]
    corr_d = np.corrcoef(d_est, true_def[: len(d_est)])[0, 1]
    assert corr_o > 0.7, f"Offense recovery corr={corr_o:.4f} below threshold 0.7"
    assert corr_d > 0.7, f"Defense recovery corr={corr_d:.4f} below threshold 0.7"

    # Determinism: same input → same output
    out2 = nba_rapm(df).sort("player_id")
    assert out.equals(out2)


def test_rapm_single_possession():
    # A 1-possession design must not crash; ridge shrinks to ~0 but the frame is valid.
    rows = [((1, 2, 3, 4, 5), (6, 7, 8, 9, 10), 2)]
    out = nba_rapm(_poss(rows))
    assert not out.is_empty()
    assert dict(out.schema) == RAPM_SCHEMA
    assert np.isfinite(out["rapm"].to_numpy()).all()  # no NaN/inf coefficients


# ---------------------------------------------------------------------------
# Task 3: nba_rapm_from_games() tests
# ---------------------------------------------------------------------------

import json
import pathlib

import sportsdataverse.nba.nba_rapm as R
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away,
    parse_rotation_resultsets,
    players_on_court_from_rotation,
)
from sportsdataverse.nba.nba_possessions import attach_possession_lineups, build_possessions

FXR = pathlib.Path("tests/fixtures/nba_engine")
GAMES = ["0022200001", "0022300001", "0022100001"]


def _game_poss(g: str) -> pl.DataFrame:
    """Build possession frame from captured fixtures for game *g*."""
    fx = FXR / g
    enh = enhanced_pbp_from_payload(json.loads((fx / "playbyplayv3.json").read_text()))
    box = json.loads((fx / "boxscoretraditionalv3.json").read_text())
    home, away = boxscore_home_away(box)
    oc = players_on_court_from_rotation(
        enh,
        parse_rotation_resultsets(json.loads((fx / "gamerotation.json").read_text())),
        home_team_id=home,
        away_team_id=away,
    )
    return attach_possession_lineups(build_possessions(enh), oc, enh, home_team_id=home)


def test_three_game_smoke_and_offline_fetcher(monkeypatch):
    by_game = {g: _game_poss(g) for g in GAMES}
    monkeypatch.setattr(R, "_fetch_possessions", lambda gid, lg: by_game[gid])
    out = R.nba_rapm_from_games(GAMES)
    assert out.height > 0
    assert dict(out.schema) == R.RAPM_SCHEMA
    assert np.isfinite(out["rapm"].to_numpy()).all()
    assert abs(out["rapm"].mean()) < 5.0  # ridge-centered
    # Deterministic: same input → same sorted output
    assert out.sort("player_id").equals(R.nba_rapm_from_games(GAMES).sort("player_id"))
    import pandas as pd

    assert isinstance(R.nba_rapm_from_games(GAMES, return_as_pandas=True), pd.DataFrame)


def test_rapm_from_games_empty_list():
    """Empty game_ids returns a zero-row frame with RAPM_SCHEMA, no network call, no raise."""
    out = R.nba_rapm_from_games([])
    assert out.is_empty()
    assert dict(out.schema) == R.RAPM_SCHEMA


def test_rapm_from_games_skips_empty_games(monkeypatch):
    # one game fetches empty, one fetches a real frame -> empty filtered out, still valid output
    by_game = {"bad_game": pl.DataFrame(), "0022200001": _game_poss("0022200001")}
    monkeypatch.setattr(R, "_fetch_possessions", lambda gid, lg: by_game[gid])
    out = R.nba_rapm_from_games(["bad_game", "0022200001"])
    assert out.height > 0
    assert dict(out.schema) == R.RAPM_SCHEMA


def test_rapm_from_games_all_empty_nonempty_ids(monkeypatch):
    # non-empty game_ids but every fetch is empty -> hits the `if not frames` guard, no concat([])
    monkeypatch.setattr(R, "_fetch_possessions", lambda gid, lg: pl.DataFrame())
    out = R.nba_rapm_from_games(["x", "y"])
    assert out.height == 0
    assert dict(out.schema) == R.RAPM_SCHEMA


from tests.conftest import skip_if_no_nba_stats_live


@skip_if_no_nba_stats_live
def test_nba_rapm_from_games_live():
    out = R.nba_rapm_from_games(["0022200001"])
    assert out.height > 0
    assert np.isfinite(out["rapm"].to_numpy()).all()

"""Offline tests for sportsdataverse.nfl.ep_wp — no model files required.

All tests monkeypatch ``_load_model`` so the XGBoost .ubj files need not be
present.  The fixture returns a ``FakeBooster`` that produces deterministic
constant outputs.

Test philosophy
---------------
* Feature shape + dtype — both ESPN and nflverse paths produce (N, K) float32
* Feature VALUES parity — ESPN path == nflverse path for identical game state
* Era-bin edge cases — boundary seasons land in the right era bucket
* Roof default — ESPN plays default retractable=1 / dome=0 / outdoors=0
* Down casting — boolean down columns → int (0/1) without sign error
* WP naive/spread shape — include_spread toggle changes column count
* Public API output columns — calculate_expected_points / calculate_win_probability
  add the right columns at the right position
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nfl.ep_wp import (
    CP_FEATURES,
    EP_FEATURES,
    WP_NAIVE_FEATURES,
    WP_SPREAD_FEATURES,
    XYAC_FEATURES,
    _EP_POINT_VALUES,
    _add_wp_aux,
    _espn_cp_features,
    _espn_ep_features,
    _espn_wp_features,
    _espn_xyac_features,
    _make_cp_mutations,
    _make_model_mutations,
    calculate_completion_probability,
    calculate_expected_points,
    calculate_win_probability,
    calculate_xyac,
)
from tests.conftest import skip_if_no_live


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _nflverse_row(
    season: int = 2022,
    half_sec: float = 1800.0,
    yardline: float = 75.0,
    home: int = 1,
    ydstogo: float = 10.0,
    down: int = 1,
    pos_timeouts: int = 3,
    def_timeouts: int = 3,
    posteam: str = "KC",
    home_team: str = "KC",
    roof: str = "outdoors",
) -> pl.DataFrame:
    """Minimal nflverse-format single-play DataFrame."""
    return pl.DataFrame(
        {
            "season": [season],
            "half_seconds_remaining": [float(half_sec)],
            "yardline_100": [float(yardline)],
            "ydstogo": [float(ydstogo)],
            "down": [down],
            "posteam": [posteam],
            "home_team": [home_team],
            "roof": [roof],
            "posteam_timeouts_remaining": [pos_timeouts],
            "defteam_timeouts_remaining": [def_timeouts],
            "score_differential": [0.0],
            "game_seconds_remaining": [float(half_sec)],
            "spread_line": [None],
            "receive_2h_ko": [0],
        }
    )


def _espn_row(
    season: int = 2022,
    half_sec: float = 1800.0,
    yardline: float = 75.0,
    home: int = 1,
    ydstogo: float = 10.0,
    down: int = 1,
    pos_timeouts: int = 3,
    def_timeouts: int = 3,
) -> pl.DataFrame:
    """Minimal ESPN-format single-play DataFrame."""
    return pl.DataFrame(
        {
            "season": [season],
            "start.TimeSecsRem": [float(half_sec)],
            "start.yardsToEndzone": [float(yardline)],
            "start.distance": [float(ydstogo)],
            "start.down": [down],
            "start.is_home": [bool(home)],
            "start.posTeamTimeouts": [pos_timeouts],
            "start.defPosTeamTimeouts": [def_timeouts],
            "down_1": [down == 1],
            "down_2": [down == 2],
            "down_3": [down == 3],
            "down_4": [down == 4],
            # WP columns
            "start.pos_team_receives_2H_kickoff": [False],
            "start.spread_time": [0.0],
            "start.adj_TimeSecsRem": [float(half_sec)],
            "pos_score_diff_start": [0.0],
            # extra columns that _espn_wp_features may need for end variants
            "end.TimeSecsRem": [float(half_sec)],
            "end.yardsToEndzone": [float(yardline)],
            "end.distance": [float(ydstogo)],
            "end.down": [down],
            "end.is_home": [bool(home)],
            "end.posTeamTimeouts": [pos_timeouts],
            "end.defPosTeamTimeouts": [def_timeouts],
            "end.pos_team_receives_2H_kickoff": [False],
            "end.spread_time": [0.0],
            "end.adj_TimeSecsRem": [float(half_sec)],
            "end.pos_score_diff": [0.0],
        }
    )


# ---------------------------------------------------------------------------
# FakeBooster — stands in for xgboost.Booster
# ---------------------------------------------------------------------------


class _FakeBooster:
    """Booster stub that returns constant predictions without model files."""

    def __init__(self, n_classes: int = 1):
        self._n_classes = n_classes

    def predict(self, dmatrix):
        n = dmatrix.num_row()
        if self._n_classes == 7:
            # uniform 1/7 for each EP class
            return np.full((n, 7), 1.0 / 7.0, dtype=np.float32)
        if self._n_classes == 76:
            # uniform multinomial over the 76 YAC buckets (rows sum to 1)
            return np.full((n, 76), 1.0 / 76.0, dtype=np.float32)
        # binary logistic → 0.5
        return np.full(n, 0.5, dtype=np.float32)

    def load_model(self, path: str) -> None:  # noqa: ARG002
        pass


@pytest.fixture()
def mock_ep_model(monkeypatch):
    """Patch _load_model so EP returns uniform class probs."""
    import sportsdataverse.nfl.ep_wp as _mod

    _original = _mod._load_model
    _original.cache_clear()

    def _fake_load(name: str, models_dir=None):  # noqa: ARG001
        return _FakeBooster(n_classes=7 if "ep_model" in name else 1)

    monkeypatch.setattr("sportsdataverse.nfl.ep_wp._load_model", _fake_load)
    yield
    _original.cache_clear()


@pytest.fixture()
def mock_wp_model(monkeypatch):
    """Patch _load_model so WP models return 0.5."""
    import sportsdataverse.nfl.ep_wp as _mod

    _original = _mod._load_model
    _original.cache_clear()

    def _fake_load(name: str, models_dir=None):  # noqa: ARG001
        return _FakeBooster(n_classes=1)

    monkeypatch.setattr("sportsdataverse.nfl.ep_wp._load_model", _fake_load)
    yield
    _original.cache_clear()


@pytest.fixture()
def mock_both_models(monkeypatch):
    """Patch _load_model for EP + WP."""
    import sportsdataverse.nfl.ep_wp as _mod

    _original = _mod._load_model
    _original.cache_clear()

    def _fake_load(name: str, models_dir=None):  # noqa: ARG001
        return _FakeBooster(n_classes=7 if "ep_model" in name else 1)

    monkeypatch.setattr("sportsdataverse.nfl.ep_wp._load_model", _fake_load)
    yield
    _original.cache_clear()


# ---------------------------------------------------------------------------
# _espn_ep_features — shape, dtype, column order
# ---------------------------------------------------------------------------


class TestEspnEpFeatures:
    def test_shape(self):
        X = _espn_ep_features(_espn_row())
        assert X.shape == (1, 18)
        assert X.dtype == np.float32

    def test_column_count_matches_ep_features(self):
        X = _espn_ep_features(_espn_row(season=2022))
        assert X.shape[1] == len(EP_FEATURES)

    def test_yardline_passthrough(self):
        X = _espn_ep_features(_espn_row(yardline=65.0))
        idx = EP_FEATURES.index("yardline_100")
        assert X[0, idx] == pytest.approx(65.0)

    def test_ydstogo_passthrough(self):
        X = _espn_ep_features(_espn_row(ydstogo=7.0))
        idx = EP_FEATURES.index("ydstogo")
        assert X[0, idx] == pytest.approx(7.0)

    def test_home_bool_to_int(self):
        X_home = _espn_ep_features(_espn_row(home=1))
        X_away = _espn_ep_features(_espn_row(home=0))
        idx = EP_FEATURES.index("home")
        assert X_home[0, idx] == 1.0
        assert X_away[0, idx] == 0.0

    def test_roof_defaults_retractable(self):
        X = _espn_ep_features(_espn_row())
        r_idx = EP_FEATURES.index("retractable")
        d_idx = EP_FEATURES.index("dome")
        o_idx = EP_FEATURES.index("outdoors")
        assert X[0, r_idx] == 1.0
        assert X[0, d_idx] == 0.0
        assert X[0, o_idx] == 0.0

    def test_down1_flag(self):
        X = _espn_ep_features(_espn_row(down=1))
        assert X[0, EP_FEATURES.index("down1")] == 1.0
        assert X[0, EP_FEATURES.index("down2")] == 0.0
        assert X[0, EP_FEATURES.index("down3")] == 0.0
        assert X[0, EP_FEATURES.index("down4")] == 0.0

    def test_down4_flag(self):
        X = _espn_ep_features(_espn_row(down=4))
        assert X[0, EP_FEATURES.index("down1")] == 0.0
        assert X[0, EP_FEATURES.index("down4")] == 1.0

    def test_timeouts_passthrough(self):
        X = _espn_ep_features(_espn_row(pos_timeouts=2, def_timeouts=1))
        assert X[0, EP_FEATURES.index("posteam_timeouts_remaining")] == 2.0
        assert X[0, EP_FEATURES.index("defteam_timeouts_remaining")] == 1.0

    def test_touchback_yardline_col(self):
        df = _espn_row(yardline=75.0)
        df = df.with_columns(pl.lit(75.0).alias("start.yardsToEndzone.touchback"))
        X = _espn_ep_features(df, yardline_col="start.yardsToEndzone.touchback")
        assert X[0, EP_FEATURES.index("yardline_100")] == pytest.approx(75.0)

    def test_end_variant_down_cols(self):
        df = _espn_row(down=2)
        df = df.with_columns(
            pl.lit(False).alias("down_1_end"),
            pl.lit(True).alias("down_2_end"),
            pl.lit(False).alias("down_3_end"),
            pl.lit(False).alias("down_4_end"),
        )
        X = _espn_ep_features(
            df,
            half_sec_col="end.TimeSecsRem",
            yardline_col="end.yardsToEndzone",
            home_col="end.is_home",
            ydstogo_col="end.distance",
            down1_col="down_1_end",
            down2_col="down_2_end",
            down3_col="down_3_end",
            down4_col="down_4_end",
            pos_timeouts_col="end.posTeamTimeouts",
            def_timeouts_col="end.defPosTeamTimeouts",
        )
        assert X[0, EP_FEATURES.index("down2")] == 1.0

    def test_multi_row(self):
        rows = pl.concat([_espn_row(season=2018), _espn_row(season=2005)])
        X = _espn_ep_features(rows)
        assert X.shape == (2, 18)


# ---------------------------------------------------------------------------
# Era-bin edge cases (shared between ESPN and nflverse paths)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "season,expected_era",
    [
        (1999, "era0"),
        (2001, "era0"),
        (2002, "era1"),
        (2005, "era1"),
        (2006, "era2"),
        (2013, "era2"),
        (2014, "era3"),
        (2017, "era3"),
        (2018, "era4"),
        (2024, "era4"),
    ],
)
def test_espn_ep_era_bins(season: int, expected_era: str):
    X = _espn_ep_features(_espn_row(season=season))
    for era in ("era0", "era1", "era2", "era3", "era4"):
        val = X[0, EP_FEATURES.index(era)]
        if era == expected_era:
            assert val == 1.0, f"Expected {era}=1 for season {season}"
        else:
            assert val == 0.0, f"Expected {era}=0 for season {season} (got {val})"


def test_nflverse_era_bins_match():
    """_make_model_mutations and _espn_ep_features produce the same era flags."""
    for season in [1999, 2002, 2006, 2014, 2018]:
        nv = _make_model_mutations(_nflverse_row(season=season, roof="outdoors"))
        espn = _espn_ep_features(_espn_row(season=season))
        for era in ("era0", "era1", "era2", "era3", "era4"):
            nv_val = nv[era][0]
            espn_val = espn[0, EP_FEATURES.index(era)]
            assert nv_val == espn_val, f"era mismatch for season {season} era {era}: nflverse={nv_val} espn={espn_val}"


# ---------------------------------------------------------------------------
# Feature parity: ESPN path == nflverse path for identical game state
# ---------------------------------------------------------------------------


def test_ep_feature_parity_outdoors():
    """Both paths produce identical EP features when roof='outdoors'."""
    season, half_sec, yardline, ydstogo, pos_to, def_to = 2022, 900.0, 40.0, 8.0, 2, 3

    nv_df = _make_model_mutations(
        _nflverse_row(
            season=season,
            half_sec=half_sec,
            yardline=yardline,
            ydstogo=ydstogo,
            pos_timeouts=pos_to,
            def_timeouts=def_to,
            home=1,
            roof="outdoors",
        )
    )
    nv_X = nv_df.select(EP_FEATURES).to_numpy(allow_copy=True).astype(np.float32)

    espn_X = _espn_ep_features(
        _espn_row(
            season=season,
            half_sec=half_sec,
            yardline=yardline,
            ydstogo=ydstogo,
            pos_timeouts=pos_to,
            def_timeouts=def_to,
            home=1,
        ),
    )

    # Era flags + home + timeouts + yardline + ydstogo must be identical.
    # Roof flags differ by design (nflverse gets outdoor=1; ESPN defaults retractable=1).
    comparable = [f for f in EP_FEATURES if f not in ("retractable", "dome", "outdoors")]
    for feat in comparable:
        i = EP_FEATURES.index(feat)
        assert nv_X[0, i] == espn_X[0, i], f"parity failure on {feat}"


def test_ep_feature_parity_dome():
    """When roof='dome' on nflverse side, ESPN path still defaults retractable=1."""
    nv_df = _make_model_mutations(_nflverse_row(roof="dome"))
    nv_X = nv_df.select(EP_FEATURES).to_numpy(allow_copy=True).astype(np.float32)
    # nflverse: dome=1, retractable=0, outdoors=0
    assert nv_X[0, EP_FEATURES.index("dome")] == 1.0
    assert nv_X[0, EP_FEATURES.index("retractable")] == 0.0

    espn_X = _espn_ep_features(_espn_row())
    # ESPN: dome=0, retractable=1 (no per-play roof data)
    assert espn_X[0, EP_FEATURES.index("dome")] == 0.0
    assert espn_X[0, EP_FEATURES.index("retractable")] == 1.0


# ---------------------------------------------------------------------------
# _espn_wp_features — shape, dtype, toggle
# ---------------------------------------------------------------------------


class TestEspnWpFeatures:
    def test_spread_shape(self):
        X = _espn_wp_features(_espn_row())
        assert X.shape == (1, 12)
        assert X.dtype == np.float32

    def test_naive_shape(self):
        X = _espn_wp_features(_espn_row(), include_spread=False)
        assert X.shape == (1, 11)

    def test_column_count_matches_feature_lists(self):
        assert _espn_wp_features(_espn_row()).shape[1] == len(WP_SPREAD_FEATURES)
        assert _espn_wp_features(_espn_row(), include_spread=False).shape[1] == len(WP_NAIVE_FEATURES)

    def test_score_diff_passthrough(self):
        df = _espn_row()
        df = df.with_columns(pl.lit(7.0).alias("pos_score_diff_start"))
        X = _espn_wp_features(df)
        idx = WP_SPREAD_FEATURES.index("score_differential")
        assert X[0, idx] == pytest.approx(7.0)

    def test_diff_time_ratio_computed(self):
        """Diff_Time_Ratio = score_diff / exp(-4 * elapsed_share)."""
        score_diff = 7.0
        game_sec = 1800.0
        elapsed = (3600.0 - game_sec) / 3600.0
        expected_dtr = score_diff / np.exp(-4.0 * elapsed)

        df = _espn_row(half_sec=game_sec)
        df = df.with_columns(pl.lit(score_diff).alias("pos_score_diff_start"))
        X = _espn_wp_features(df)
        idx = WP_SPREAD_FEATURES.index("Diff_Time_Ratio")
        assert X[0, idx] == pytest.approx(expected_dtr, rel=1e-5)

    def test_diff_time_ratio_zero_game_sec(self):
        """At game_seconds_remaining=0, elapsed_share=1, ratio=score*exp(4)."""
        score_diff = 3.0
        expected_dtr = score_diff * np.exp(4.0)

        df = _espn_row(half_sec=0.0)
        df = df.with_columns(
            pl.lit(score_diff).alias("pos_score_diff_start"),
            pl.lit(0.0).alias("start.adj_TimeSecsRem"),
        )
        X = _espn_wp_features(df, game_sec_col="start.adj_TimeSecsRem")
        idx = WP_SPREAD_FEATURES.index("Diff_Time_Ratio")
        assert X[0, idx] == pytest.approx(expected_dtr, rel=1e-4)

    def test_end_variant(self):
        df = _espn_row()
        X = _espn_wp_features(
            df,
            receive_ko_col="end.pos_team_receives_2H_kickoff",
            spread_time_col="end.spread_time",
            home_col="end.is_home",
            half_sec_col="end.TimeSecsRem",
            game_sec_col="end.adj_TimeSecsRem",
            score_diff_col="end.pos_score_diff",
            down_col="end.down",
            ydstogo_col="end.distance",
            yardline_col="end.yardsToEndzone",
            pos_timeouts_col="end.posTeamTimeouts",
            def_timeouts_col="end.defPosTeamTimeouts",
        )
        assert X.shape == (1, 12)

    def test_multi_row(self):
        rows = pl.concat([_espn_row(), _espn_row()])
        assert _espn_wp_features(rows).shape == (2, 12)


# ---------------------------------------------------------------------------
# WP feature parity: ESPN path Diff_Time_Ratio == nflverse _add_wp_aux
# ---------------------------------------------------------------------------


def test_wp_dtr_parity():
    """ESPN and nflverse paths produce the same Diff_Time_Ratio value."""
    score_diff = 10.0
    game_sec = 900.0
    elapsed = (3600.0 - game_sec) / 3600.0
    expected_dtr = score_diff / np.exp(-4.0 * elapsed)

    # nflverse path
    nv_df = _nflverse_row(half_sec=game_sec)
    nv_df = nv_df.with_columns(
        pl.lit(score_diff).alias("score_differential"),
        pl.lit(0.0).alias("posteam_spread"),  # spread_line is null → naive path
    )
    nv_aug = _add_wp_aux(nv_df)
    nv_dtr = nv_aug["Diff_Time_Ratio"][0]

    # ESPN path
    espn_df = _espn_row(half_sec=game_sec)
    espn_df = espn_df.with_columns(pl.lit(score_diff).alias("pos_score_diff_start"))
    X_espn = _espn_wp_features(espn_df)
    espn_dtr = float(X_espn[0, WP_SPREAD_FEATURES.index("Diff_Time_Ratio")])

    assert nv_dtr == pytest.approx(expected_dtr, rel=1e-5)
    assert espn_dtr == pytest.approx(expected_dtr, rel=1e-5)


# ---------------------------------------------------------------------------
# Public API: calculate_expected_points
# ---------------------------------------------------------------------------


class TestCalculateExpectedPoints:
    def test_returns_polars(self, mock_ep_model):
        df = calculate_expected_points(_nflverse_row())
        assert isinstance(df, pl.DataFrame)

    def test_adds_ep_column(self, mock_ep_model):
        df = calculate_expected_points(_nflverse_row())
        assert "ep" in df.columns

    def test_adds_all_prob_columns(self, mock_ep_model):
        from sportsdataverse.nfl.ep_wp import _EP_CLASS_NAMES

        df = calculate_expected_points(_nflverse_row())
        for col in _EP_CLASS_NAMES:
            assert col in df.columns

    def test_ep_value_from_uniform_probs(self, mock_ep_model):
        """Uniform 1/7 probs → ep = sum(point_vals) / 7 = (7-7+3-3+2-2+0)/7 = 0."""
        df = calculate_expected_points(_nflverse_row())
        expected_ep = float(_EP_POINT_VALUES.sum()) / 7.0
        assert df["ep"][0] == pytest.approx(expected_ep, abs=1e-4)

    def test_drops_stale_ep_columns(self, mock_ep_model):
        stale = _nflverse_row().with_columns(pl.lit(999.0).alias("ep"))
        df = calculate_expected_points(stale)
        assert df["ep"][0] != pytest.approx(999.0)

    def test_return_as_pandas(self, mock_ep_model):
        import pandas

        df = calculate_expected_points(_nflverse_row(), return_as_pandas=True)
        assert isinstance(df, pandas.DataFrame)

    def test_multi_row(self, mock_ep_model):
        rows = pl.concat([_nflverse_row(season=2020), _nflverse_row(season=2015)])
        df = calculate_expected_points(rows)
        assert df.shape[0] == 2
        assert "ep" in df.columns


# ---------------------------------------------------------------------------
# Public API: calculate_win_probability
# ---------------------------------------------------------------------------


class TestCalculateWinProbability:
    def test_returns_polars(self, mock_wp_model):
        df = calculate_win_probability(_nflverse_row())
        assert isinstance(df, pl.DataFrame)

    def test_adds_wp_and_vegas_wp(self, mock_wp_model):
        df = calculate_win_probability(_nflverse_row())
        assert "wp" in df.columns
        assert "vegas_wp" in df.columns

    def test_wp_is_05_from_fake_model(self, mock_wp_model):
        df = calculate_win_probability(_nflverse_row())
        assert df["wp"][0] == pytest.approx(0.5, abs=1e-4)

    def test_null_spread_uses_naive_for_vegas_wp(self, mock_wp_model):
        """When spread_line is null, vegas_wp should equal naive wp (both 0.5)."""
        df = calculate_win_probability(_nflverse_row())
        # null spread → vegas_wp falls back to naive
        assert df["vegas_wp"][0] == pytest.approx(df["wp"][0], abs=1e-4)

    def test_drops_stale_wp_columns(self, mock_wp_model):
        stale = _nflverse_row().with_columns(
            pl.lit(0.99).alias("wp"),
            pl.lit(0.99).alias("vegas_wp"),
        )
        df = calculate_win_probability(stale)
        assert df["wp"][0] != pytest.approx(0.99)

    def test_return_as_pandas(self, mock_wp_model):
        import pandas

        df = calculate_win_probability(_nflverse_row(), return_as_pandas=True)
        assert isinstance(df, pandas.DataFrame)

    def test_skips_make_mutations_if_home_present(self, mock_wp_model):
        """If 'home' column already exists, _make_model_mutations is not re-run."""
        df = _nflverse_row().with_columns(pl.lit(1).alias("home"))
        result = calculate_win_probability(df)
        assert "wp" in result.columns


# ---------------------------------------------------------------------------
# Pass-play helpers — extend base rows with air_yards / pass columns
# ---------------------------------------------------------------------------


def _nflverse_pass_row(
    season: int = 2022,
    half_sec: float = 1800.0,
    yardline: float = 40.0,
    ydstogo: float = 10.0,
    down: int = 1,
    air_yards: float | None = 5.0,
    complete_pass: int = 1,
    ep: float = 2.0,
    cp: float = 0.7,
    roof: str = "outdoors",
) -> pl.DataFrame:
    """Minimal nflverse-format single pass-play DataFrame with ep + cp.

    Set ``air_yards=None`` to produce a non-pass row with a matching schema.
    """
    base = _nflverse_row(
        season=season,
        half_sec=half_sec,
        yardline=yardline,
        ydstogo=ydstogo,
        down=down,
        roof=roof,
    )
    _air: float | None = float(air_yards) if air_yards is not None else None
    # ``pass_location="left"`` keeps ``pass_middle`` at 0 so the CP feature
    # parity check (ESPN side defaults pass_middle=0) still holds.
    return base.with_columns(
        pl.lit(_air, dtype=pl.Float64).alias("air_yards"),
        pl.lit(complete_pass).alias("complete_pass"),
        pl.lit(0).alias("incomplete_pass"),
        pl.lit(0).alias("interception"),
        pl.lit(ep).alias("ep"),
        pl.lit(cp).alias("cp"),
        # nflfastR add_xyac valid_pass inputs
        pl.lit(0.5).alias("air_epa"),
        pl.lit("left").alias("pass_location"),
        pl.lit("X.Receiver").alias("receiver_player_name"),
    )


def _espn_pass_row(
    season: int = 2022,
    half_sec: float = 1800.0,
    yardline: float = 40.0,
    ydstogo: float = 10.0,
    down: int = 1,
    air_yards: float = 5.0,
    cp: float = 0.7,
    ep: float = 2.0,
) -> pl.DataFrame:
    """Minimal ESPN-format single pass-play DataFrame with cp + ep."""
    base = _espn_row(
        season=season,
        half_sec=half_sec,
        yardline=yardline,
        ydstogo=ydstogo,
        down=down,
    )
    return base.with_columns(
        pl.lit(float(air_yards)).alias("air_yards"),
        pl.lit(cp).alias("cp"),
        pl.lit(ep).alias("ep"),
    )


# ---------------------------------------------------------------------------
# Additional fixtures for CP / XYAC models
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_cp_model(monkeypatch):
    """Patch _load_model so CP model returns 0.5, EP model returns uniform."""
    import sportsdataverse.nfl.ep_wp as _mod

    _original = _mod._load_model
    _original.cache_clear()

    def _fake_load(name: str, models_dir=None):  # noqa: ARG001
        return _FakeBooster(n_classes=7 if "ep_model" in name else 1)

    monkeypatch.setattr("sportsdataverse.nfl.ep_wp._load_model", _fake_load)
    yield
    _original.cache_clear()


@pytest.fixture()
def mock_xyac_model(monkeypatch):
    """Patch _load_model: EP → 7-class uniform, xYAC → 76-class uniform.

    The xYAC derivation re-scores expected points on every outcome row, so the
    EP model (``ep_model.ubj``) must also be stubbed.  A uniform 76-class
    distribution makes the derivation deterministic and model-file-free.
    """
    import sportsdataverse.nfl.ep_wp as _mod

    _original = _mod._load_model
    _original.cache_clear()

    def _fake_load(name: str, models_dir=None):
        if "xyac_model" in name:
            return _FakeBooster(n_classes=76)
        return _FakeBooster(n_classes=7 if "ep_model" in name else 1)

    monkeypatch.setattr("sportsdataverse.nfl.ep_wp._load_model", _fake_load)
    yield
    _original.cache_clear()


# ---------------------------------------------------------------------------
# _espn_cp_features — shape, dtype, derived values
# ---------------------------------------------------------------------------


class TestEspnCpFeatures:
    def test_shape(self):
        X = _espn_cp_features(_espn_pass_row())
        assert X.shape == (1, 18)
        assert X.dtype == np.float32

    def test_column_count_matches_cp_features(self):
        assert _espn_cp_features(_espn_pass_row()).shape[1] == len(CP_FEATURES)

    def test_air_yards_passthrough(self):
        X = _espn_cp_features(_espn_pass_row(air_yards=7.5))
        assert X[0, CP_FEATURES.index("air_yards")] == pytest.approx(7.5)

    def test_air_is_zero_when_zero_air_yards(self):
        df = _espn_pass_row(air_yards=0.0)
        X = _espn_cp_features(df)
        assert X[0, CP_FEATURES.index("air_is_zero")] == 1.0

    def test_air_is_zero_when_nonzero_air_yards(self):
        X = _espn_cp_features(_espn_pass_row(air_yards=5.0))
        assert X[0, CP_FEATURES.index("air_is_zero")] == 0.0

    def test_distance_to_sticks(self):
        # distance_to_sticks = air_yards - ydstogo = 7 - 10 = -3 (nflfastR sign)
        df = _espn_pass_row(ydstogo=10.0, air_yards=7.0)
        X = _espn_cp_features(df)
        assert X[0, CP_FEATURES.index("distance_to_sticks")] == pytest.approx(-3.0)

    def test_no_era0_era1_in_cp_features(self):
        assert "era0" not in CP_FEATURES
        assert "era1" not in CP_FEATURES

    def test_era2_for_2010_season(self):
        X = _espn_cp_features(_espn_pass_row(season=2010))
        assert X[0, CP_FEATURES.index("era2")] == 1.0
        assert X[0, CP_FEATURES.index("era3")] == 0.0
        assert X[0, CP_FEATURES.index("era4")] == 0.0

    def test_era4_for_2022_season(self):
        X = _espn_cp_features(_espn_pass_row(season=2022))
        assert X[0, CP_FEATURES.index("era2")] == 0.0
        assert X[0, CP_FEATURES.index("era3")] == 0.0
        assert X[0, CP_FEATURES.index("era4")] == 1.0

    def test_roof_defaults_retractable(self):
        X = _espn_cp_features(_espn_pass_row())
        assert X[0, CP_FEATURES.index("retractable")] == 1.0
        assert X[0, CP_FEATURES.index("dome")] == 0.0
        assert X[0, CP_FEATURES.index("outdoors")] == 0.0

    def test_qb_hit_default_zero(self):
        X = _espn_cp_features(_espn_pass_row())
        assert X[0, CP_FEATURES.index("qb_hit")] == 0.0

    def test_qb_hit_from_col(self):
        df = _espn_pass_row().with_columns(pl.lit(True).alias("qb_hit_flag"))
        X = _espn_cp_features(df, qb_hit_col="qb_hit_flag")
        assert X[0, CP_FEATURES.index("qb_hit")] == 1.0

    def test_pass_middle_default_zero(self):
        X = _espn_cp_features(_espn_pass_row())
        assert X[0, CP_FEATURES.index("pass_middle")] == 0.0

    def test_pass_middle_from_col(self):
        df = _espn_pass_row().with_columns(pl.lit(True).alias("pm_flag"))
        X = _espn_cp_features(df, pass_middle_col="pm_flag")
        assert X[0, CP_FEATURES.index("pass_middle")] == 1.0

    def test_multi_row(self):
        rows = pl.concat([_espn_pass_row(season=2018), _espn_pass_row(season=2010)])
        assert _espn_cp_features(rows).shape == (2, 18)


# ---------------------------------------------------------------------------
# _espn_xyac_features — shape, dtype, passthrough values
# ---------------------------------------------------------------------------


class TestEspnXyacFeatures:
    def test_shape(self):
        X = _espn_xyac_features(_espn_pass_row())
        assert X.shape == (1, 19)
        assert X.dtype == np.float32

    def test_column_count_matches_xyac_features(self):
        assert _espn_xyac_features(_espn_pass_row()).shape[1] == len(XYAC_FEATURES)

    def test_no_cp_ep_in_features(self):
        # The faithful multinomial xYAC model takes no cp/ep features.
        assert "cp" not in XYAC_FEATURES
        assert "ep" not in XYAC_FEATURES

    def test_air_yards_passthrough(self):
        X = _espn_xyac_features(_espn_pass_row(air_yards=8.0))
        assert X[0, XYAC_FEATURES.index("air_yards")] == pytest.approx(8.0)

    def test_distance_to_goal_computed(self):
        # distance_to_goal = yardline_100 - air_yards = 40 - 8 = 32
        X = _espn_xyac_features(_espn_pass_row(yardline=40.0, air_yards=8.0))
        assert X[0, XYAC_FEATURES.index("distance_to_goal")] == pytest.approx(32.0)

    def test_distance_to_sticks_computed(self):
        # distance_to_sticks = air_yards - ydstogo = 8 - 10 = -2
        X = _espn_xyac_features(_espn_pass_row(ydstogo=10.0, air_yards=8.0))
        assert X[0, XYAC_FEATURES.index("distance_to_sticks")] == pytest.approx(-2.0)

    def test_air_is_zero_computed(self):
        X = _espn_xyac_features(_espn_pass_row(air_yards=0.0))
        assert X[0, XYAC_FEATURES.index("air_is_zero")] == 1.0

    def test_air_is_zero_nonzero(self):
        X = _espn_xyac_features(_espn_pass_row(air_yards=4.0))
        assert X[0, XYAC_FEATURES.index("air_is_zero")] == 0.0

    def test_down_one_hots(self):
        X = _espn_xyac_features(_espn_pass_row(down=3))
        assert X[0, XYAC_FEATURES.index("down1")] == 0.0
        assert X[0, XYAC_FEATURES.index("down3")] == 1.0
        assert X[0, XYAC_FEATURES.index("down4")] == 0.0

    def test_roof_defaults_retractable(self):
        X = _espn_xyac_features(_espn_pass_row())
        assert X[0, XYAC_FEATURES.index("retractable")] == 1.0
        assert X[0, XYAC_FEATURES.index("dome")] == 0.0
        assert X[0, XYAC_FEATURES.index("outdoors")] == 0.0

    def test_era_flags(self):
        X = _espn_xyac_features(_espn_pass_row(season=2015))
        assert X[0, XYAC_FEATURES.index("era2")] == 0.0
        assert X[0, XYAC_FEATURES.index("era3")] == 1.0
        assert X[0, XYAC_FEATURES.index("era4")] == 0.0

    def test_multi_row(self):
        rows = pl.concat([_espn_pass_row(season=2022), _espn_pass_row(season=2015)])
        assert _espn_xyac_features(rows).shape == (2, 19)


# ---------------------------------------------------------------------------
# CP/XYAC parity: ESPN adapters produce identical non-roof features vs nflverse
# ---------------------------------------------------------------------------


def test_cp_feature_parity_era_and_yardage():
    """ESPN and nflverse CP paths agree on era flags, air_yards, distance_to_sticks."""
    season, ydstogo, air_yards = 2016, 12.0, 8.0

    nv_df = _nflverse_pass_row(
        season=season,
        ydstogo=ydstogo,
        air_yards=air_yards,
        roof="outdoors",
    )
    nv_df = _make_cp_mutations(nv_df)
    nv_X = nv_df.select(CP_FEATURES).to_numpy(allow_copy=True).astype(np.float32)

    espn_df = _espn_pass_row(season=season, ydstogo=ydstogo, air_yards=air_yards)
    espn_X = _espn_cp_features(espn_df)

    # Roof flags differ by design; compare everything else
    comparable = [f for f in CP_FEATURES if f not in ("retractable", "dome", "outdoors")]
    for feat in comparable:
        i = CP_FEATURES.index(feat)
        assert nv_X[0, i] == espn_X[0, i], f"CP parity failure on {feat}"


# ---------------------------------------------------------------------------
# Public API: calculate_completion_probability
# ---------------------------------------------------------------------------


class TestCalculateCompletionProbability:
    def test_adds_cp_column(self, mock_cp_model):
        df = _nflverse_pass_row()
        result = calculate_completion_probability(df)
        assert "cp" in result.columns

    def test_adds_cpoe_column(self, mock_cp_model):
        result = calculate_completion_probability(_nflverse_pass_row())
        assert "cpoe" in result.columns

    def test_cp_not_null_for_pass_plays(self, mock_cp_model):
        result = calculate_completion_probability(_nflverse_pass_row())
        assert result["cp"][0] is not None

    def test_cp_null_for_nonpass_plays(self, mock_cp_model):
        df = _nflverse_row().with_columns(pl.lit(None).cast(pl.Float64).alias("air_yards"))
        result = calculate_completion_probability(df)
        assert result["cp"][0] is None

    def test_cpoe_value_with_complete_pass(self, mock_cp_model):
        """cpoe = 100 * (complete_pass(1) - cp(0.5 from fake)) = 50.0.

        nflfastR's ``helper_add_cp_cpoe.R`` defines ``cpoe`` on the
        percentage-point scale (``100 * (complete_pass - cp)``).
        """
        result = calculate_completion_probability(_nflverse_pass_row(complete_pass=1))
        assert result["cpoe"][0] == pytest.approx(50.0, abs=1e-4)

    def test_cpoe_null_without_complete_pass_col(self, mock_cp_model):
        df = _nflverse_row().with_columns(pl.lit(5.0).alias("air_yards"))
        result = calculate_completion_probability(df)
        assert result["cpoe"][0] is None

    def test_drops_stale_cp(self, mock_cp_model):
        stale = _nflverse_pass_row().with_columns(pl.lit(0.99).alias("cp"))
        result = calculate_completion_probability(stale)
        assert result["cp"][0] != pytest.approx(0.99)

    def test_mixed_pass_nonpass(self, mock_cp_model):
        """Pass play gets cp; non-pass play gets null; row order preserved."""
        pass_play = _nflverse_pass_row(season=2022)
        nonpass_play = _nflverse_pass_row(season=2020, air_yards=None)
        df = pl.concat([pass_play, nonpass_play])
        result = calculate_completion_probability(df)
        assert result["cp"][0] is not None
        assert result["cp"][1] is None

    def test_return_as_pandas(self, mock_cp_model):
        import pandas

        result = calculate_completion_probability(_nflverse_pass_row(), return_as_pandas=True)
        assert isinstance(result, pandas.DataFrame)

    def test_multi_row_pass(self, mock_cp_model):
        rows = pl.concat([_nflverse_pass_row(season=2022), _nflverse_pass_row(season=2018)])
        result = calculate_completion_probability(rows)
        assert result.shape[0] == 2
        assert result["cp"].null_count() == 0


# ---------------------------------------------------------------------------
# Public API: calculate_xyac
# ---------------------------------------------------------------------------


class TestCalculateXyac:
    _OUT = ("xyac_epa", "xyac_mean_yardage", "xyac_median_yardage", "xyac_success", "xyac_fd")

    def test_adds_xyac_columns(self, mock_xyac_model):
        result = calculate_xyac(_nflverse_pass_row())
        for col in self._OUT:
            assert col in result.columns

    def test_xyac_not_null_for_pass_plays(self, mock_xyac_model):
        result = calculate_xyac(_nflverse_pass_row())
        for col in self._OUT:
            assert result[col][0] is not None, f"{col} unexpectedly null"

    def test_xyac_null_for_nonpass(self, mock_xyac_model):
        # air_yards null → fails the valid_pass filter → all xyac cols null
        df = _nflverse_pass_row(air_yards=None)
        result = calculate_xyac(df)
        assert result["xyac_epa"][0] is None

    def test_xyac_null_when_not_a_pass_attempt(self, mock_xyac_model):
        """Plays with air_yards but no completion/incompletion/INT are excluded."""
        df = _nflverse_pass_row(complete_pass=0)
        result = calculate_xyac(df)
        assert result["xyac_epa"][0] is None

    def test_xyac_null_when_distance_to_goal_zero(self, mock_xyac_model):
        """distance_to_goal == 0 (air_yards == yardline_100) is excluded."""
        df = _nflverse_pass_row(yardline=10.0, air_yards=10.0)
        result = calculate_xyac(df)
        assert result["xyac_epa"][0] is None

    def test_drops_stale_xyac_cols(self, mock_xyac_model):
        stale = _nflverse_pass_row().with_columns(pl.lit(999.0).alias("xyac_mean_yardage"))
        result = calculate_xyac(stale)
        assert result["xyac_mean_yardage"][0] != pytest.approx(999.0)

    def test_mixed_pass_nonpass_row_order(self, mock_xyac_model):
        pass_play = _nflverse_pass_row(season=2022)
        nonpass_play = _nflverse_pass_row(season=2020, air_yards=None)
        df = pl.concat([pass_play, nonpass_play])
        result = calculate_xyac(df)
        assert result["xyac_epa"][0] is not None
        assert result["xyac_epa"][1] is None

    def test_return_as_pandas(self, mock_xyac_model):
        import pandas

        result = calculate_xyac(_nflverse_pass_row(), return_as_pandas=True)
        assert isinstance(result, pandas.DataFrame)


# ---------------------------------------------------------------------------
# Public API: calculate_xyac — air_epa absent (Shield-native / ESPN) path
# ---------------------------------------------------------------------------


class TestCalculateXyacAirEpaAbsent:
    """When air_epa is absent (native path), calculate_xyac computes it.

    Previously this raised polars.ColumnNotFoundError because _derive_xyac read
    pl.col("air_epa"); the fix derives air_epa from the yac == 0 outcome.
    """

    _OUT = ("xyac_epa", "xyac_mean_yardage", "xyac_median_yardage", "xyac_success", "xyac_fd")

    def test_no_crash_when_air_epa_absent(self, mock_xyac_model):
        df = _nflverse_pass_row().drop("air_epa")
        assert "air_epa" not in df.columns
        # Must not raise ColumnNotFoundError.
        result = calculate_xyac(df)
        for col in self._OUT:
            assert col in result.columns

    def test_xyac_epa_non_null_on_qualifying_pass(self, mock_xyac_model):
        df = _nflverse_pass_row().drop("air_epa")
        result = calculate_xyac(df)
        assert result["xyac_epa"][0] is not None

    def test_air_epa_surfaced_as_output_when_absent(self, mock_xyac_model):
        df = _nflverse_pass_row().drop("air_epa")
        result = calculate_xyac(df)
        assert "air_epa" in result.columns
        assert result["air_epa"][0] is not None

    def test_air_epa_not_added_when_present(self, mock_xyac_model):
        """Supplied air_epa is kept verbatim and not duplicated/overwritten."""
        df = _nflverse_pass_row()  # carries air_epa = 0.5
        result = calculate_xyac(df)
        # Single air_epa column, value preserved exactly.
        assert result.columns.count("air_epa") == 1
        assert result["air_epa"][0] == pytest.approx(0.5)

    def test_computed_air_epa_matches_yac0_outcome_identity(self, mock_xyac_model):
        """xyac_epa(no air_epa) == Σ((ep-orig)·prob) - air_epa_computed.

        Equivalently: feeding the computed air_epa back in as the supplied
        column reproduces the same xyac_epa, proving the two paths share the
        identical Σ((ep-original_ep)·prob) term and differ only by which
        air_epa baseline is subtracted.
        """
        df = _nflverse_pass_row().drop("air_epa")
        out_native = calculate_xyac(df)
        computed_air = out_native["air_epa"][0]
        # Supply the computed air_epa back -> xyac_epa must be byte-identical.
        df_supplied = df.with_columns(pl.lit(computed_air).alias("air_epa"))
        out_supplied = calculate_xyac(df_supplied)
        assert out_supplied["xyac_epa"][0] == pytest.approx(out_native["xyac_epa"][0])

    def test_air_epa_absent_with_nonpass_row(self, mock_xyac_model):
        """Non-qualifying rows stay null; no crash without air_epa."""
        df = _nflverse_pass_row(air_yards=None).drop("air_epa")
        result = calculate_xyac(df)
        assert result["xyac_epa"][0] is None


# ---------------------------------------------------------------------------
# Download-on-demand + cache resolution for the large xYAC model.
#
# These exercise ``_load_model``'s resolution order (override → bundled →
# cache → download) and ``clear_cache``'s models/ preservation WITHOUT touching
# the network (the bundled EP/WP/CP models stay on the bundled path; xYAC is
# faked via a tmp cache dir / bogus URL). The single real-network end-to-end
# download is live-gated below.
# ---------------------------------------------------------------------------


class TestXyacModelCacheResolution:
    def _write_dummy_model(self, path):
        """Write a tiny valid XGBoost Booster .ubj so load_model() succeeds."""
        import numpy as np
        import xgboost as xgb

        path.parent.mkdir(parents=True, exist_ok=True)
        dtrain = xgb.DMatrix(np.zeros((4, 1), dtype=np.float32), label=np.array([0, 1, 0, 1]))
        booster = xgb.train({"objective": "binary:logistic"}, dtrain, num_boost_round=1)
        booster.save_model(str(path))

    def test_loads_from_cache_dir(self, tmp_path, monkeypatch):
        """A model placed in <cache_dir>/models is found (cache hit, no download)."""
        import sportsdataverse.nfl.ep_wp as ew
        from sportsdataverse.nfl import reset_config, update_config

        ew._load_model.cache_clear()
        update_config(cache_dir=str(tmp_path))
        try:
            cache_model = tmp_path / "models" / "xyac_model.ubj"
            self._write_dummy_model(cache_model)

            # Guard: a download attempt would be a bug here — make download() blow up.
            def _boom(*a, **k):
                raise AssertionError("download() must not be called on a cache hit")

            monkeypatch.setattr("sportsdataverse.dl_utils.download", _boom)

            booster = ew._load_model("xyac_model.ubj")
            assert booster is not None
        finally:
            ew._load_model.cache_clear()
            reset_config()

    def test_models_dir_override(self, tmp_path):
        """An explicit models_dir override is preferred and loaded."""
        import sportsdataverse.nfl.ep_wp as ew

        ew._load_model.cache_clear()
        try:
            override = tmp_path / "custom"
            self._write_dummy_model(override / "xyac_model.ubj")
            booster = ew._load_model("xyac_model.ubj", models_dir=str(override))
            assert booster is not None
        finally:
            ew._load_model.cache_clear()

    def test_download_failure_raises_filenotfound(self, tmp_path, monkeypatch):
        """No cache + a failing download → FileNotFoundError (so enrich skips)."""
        import sportsdataverse.nfl.ep_wp as ew
        from sportsdataverse.nfl import reset_config, update_config

        ew._load_model.cache_clear()
        update_config(cache_dir=str(tmp_path))
        try:

            def _boom(*a, **k):
                raise RuntimeError("network down / bogus URL")

            monkeypatch.setattr("sportsdataverse.dl_utils.download", _boom)
            with pytest.raises(FileNotFoundError):
                ew._load_model("xyac_model.ubj")
        finally:
            ew._load_model.cache_clear()
            reset_config()

    def test_calculate_xyac_propagates_filenotfound(self, tmp_path, monkeypatch):
        """When the model is unavailable, calculate_xyac raises FileNotFoundError.

        This is the contract ``enrich_nfl_pbp`` relies on to skip the xYAC step
        (its ``except FileNotFoundError`` → ``RuntimeWarning``) and leave the
        five xyac columns null while the rest of the pipeline proceeds.
        """
        import sportsdataverse.nfl.ep_wp as ew
        from sportsdataverse.nfl import reset_config, update_config

        ew._load_model.cache_clear()
        update_config(cache_dir=str(tmp_path))  # empty cache, no bundled xyac
        try:

            def _boom(*a, **k):
                raise RuntimeError("network down / bogus URL")

            monkeypatch.setattr("sportsdataverse.dl_utils.download", _boom)
            with pytest.raises(FileNotFoundError):
                calculate_xyac(_nflverse_pass_row())
        finally:
            ew._load_model.cache_clear()
            reset_config()

    def test_enrich_skips_xyac_when_unavailable(self, monkeypatch, mock_both_models):
        """enrich_nfl_pbp degrades gracefully (RuntimeWarning + null xyac) offline.

        ``mock_both_models`` stubs EP/WP/CP via ``_load_model``; we then force
        the xYAC step to raise ``FileNotFoundError`` and assert the five xyac
        cols are present-but-null and a ``RuntimeWarning`` fired.  The input is
        the full nflverse-contract frame built by ``test_enrich``'s helper.
        """
        import warnings

        import sportsdataverse.nfl.ep_wp as ew
        from tests.nfl.test_enrich import _synthetic_frame

        def _raise_xyac(df, *, models_dir=None, return_as_pandas=False):
            raise FileNotFoundError("xyac unavailable (offline test)")

        monkeypatch.setattr("sportsdataverse.nfl.ep_wp.calculate_xyac", _raise_xyac)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            out = ew.enrich_nfl_pbp(_synthetic_frame())

        assert any(issubclass(w.category, RuntimeWarning) for w in caught)
        # xYAC was skipped, but for schema stability the five columns are still
        # present and all-null — the rest of the enrichment is intact.
        assert "ep" in out.columns and "wp" in out.columns
        for col in ("xyac_epa", "xyac_mean_yardage", "xyac_median_yardage", "xyac_success", "xyac_fd"):
            assert col in out.columns, f"{col} missing — schema not stable offline"
            assert out[col].null_count() == out.height, f"{col} should be all-null when skipped"

    def test_clear_cache_preserves_models(self, tmp_path):
        """clear_cache() wipes data entries but keeps <cache_dir>/models intact."""
        from sportsdataverse.nfl import clear_cache, reset_config, update_config

        update_config(cache_mode="filesystem", cache_dir=str(tmp_path))
        try:
            # A data-cache parquet alongside a downloaded model.
            (tmp_path / "deadbeef.parquet").write_bytes(b"stale")
            model_file = tmp_path / "models" / "xyac_model.ubj"
            model_file.parent.mkdir(parents=True, exist_ok=True)
            model_file.write_bytes(b"pretend-model")

            clear_cache()

            assert not (tmp_path / "deadbeef.parquet").exists(), "data entry should be wiped"
            assert model_file.exists(), "models/ must survive clear_cache()"
            assert model_file.read_bytes() == b"pretend-model"
        finally:
            reset_config()


@skip_if_no_live
class TestXyacDownloadLive:
    """Real download from the nfl_model_artifacts release (gated by SDV_PY_LIVE_TESTS=1)."""

    def test_end_to_end_download_and_cache_reuse(self, tmp_path):
        import sportsdataverse.nfl.ep_wp as ew
        from sportsdataverse.nfl import load_nfl_pbp, reset_config, update_config

        ew._load_model.cache_clear()
        update_config(cache_dir=str(tmp_path))
        try:
            dest = tmp_path / "models" / "xyac_model.ubj"
            assert not dest.exists()

            pbp = load_nfl_pbp([2023]).head(400)
            enriched = ew.enrich_nfl_pbp(pbp)

            # Downloaded + cached (~33.7 MB).
            assert dest.exists(), "xyac_model.ubj should be downloaded into <cache_dir>/models"
            assert dest.stat().st_size > 30_000_000

            # At least one qualifying pass got a non-null xyac value.
            qualifying = enriched.filter(pl.col("xyac_epa").is_not_null())
            assert qualifying.height > 0
            for col in ("xyac_epa", "xyac_mean_yardage", "xyac_median_yardage", "xyac_success", "xyac_fd"):
                assert col in enriched.columns

            # Re-run loads from cache — no re-download (mtime unchanged).
            mtime_before = dest.stat().st_mtime
            ew._load_model.cache_clear()  # force a fresh resolution path
            ew.enrich_nfl_pbp(load_nfl_pbp([2023]).head(400))
            assert dest.stat().st_mtime == mtime_before, "cache hit must not re-download"
        finally:
            ew._load_model.cache_clear()
            reset_config()

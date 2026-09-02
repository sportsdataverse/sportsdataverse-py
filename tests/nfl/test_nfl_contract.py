"""Tests for NFLVERSE_FRAME_CONTRACT and centralized NFL constants.

TDD-first: these tests were written BEFORE model_vars.py was updated,
so they document the expected shape of the additions. Run with:

    uv run pytest tests/nfl/test_nfl_contract.py -q
"""

from __future__ import annotations

import numpy as np
import pytest


# ---------------------------------------------------------------------------
# Import guard — the names must be importable from model_vars
# ---------------------------------------------------------------------------


def test_imports() -> None:
    """All new names must be importable from model_vars without error."""
    from sportsdataverse.nfl.model_vars import (  # noqa: F401
        _EP_POINT_VALUES,
        ERA_SEASON_CUTS,
        NFLVERSE_FRAME_CONTRACT,
        SPREAD_TIME_DECAY_EXPONENT,
        TOUCHBACK_YARDLINE_POST_2016,
        TOUCHBACK_YARDLINE_PRE_2016,
    )


# ---------------------------------------------------------------------------
# NFLVERSE_FRAME_CONTRACT — immutability + required columns
# ---------------------------------------------------------------------------


class TestNflverseFrameContract:
    """NFLVERSE_FRAME_CONTRACT is a frozenset and contains required columns."""

    def _contract(self):  # type: ignore[return]
        from sportsdataverse.nfl.model_vars import NFLVERSE_FRAME_CONTRACT

        return NFLVERSE_FRAME_CONTRACT

    def test_is_frozenset(self) -> None:
        assert isinstance(self._contract(), frozenset), "NFLVERSE_FRAME_CONTRACT must be a frozenset (immutable)"

    def test_immutable_cannot_add(self) -> None:
        contract = self._contract()
        with pytest.raises(AttributeError):
            contract.add("unexpected_col")  # type: ignore[attr-defined]

    # --- EP feature inputs (nflverse native column names) ---

    def test_ep_feature_inputs_present(self) -> None:
        contract = self._contract()
        ep_required = {
            "season",
            "half_seconds_remaining",
            "yardline_100",
            "ydstogo",
            "down",
            "posteam_timeouts_remaining",
            "defteam_timeouts_remaining",
            "home",  # derived: posteam == home_team
            "retractable",  # derived from roof
            "dome",  # derived from roof
            "outdoors",  # derived from roof
        }
        missing = ep_required - contract
        assert not missing, f"EP feature columns missing from contract: {sorted(missing)}"

    # --- WP feature inputs (nflverse native column names) ---

    def test_wp_feature_inputs_present(self) -> None:
        contract = self._contract()
        wp_required = {
            "score_differential",
            "game_seconds_remaining",
            "spread_line",
            "receive_2h_ko",
        }
        missing = wp_required - contract
        assert not missing, f"WP feature columns missing from contract: {sorted(missing)}"

    # --- Identity columns ---

    def test_identity_columns_present(self) -> None:
        contract = self._contract()
        identity = {
            "game_id",
            "play_id",
            "posteam",
            "defteam",
            "home_team",
        }
        missing = identity - contract
        assert not missing, f"Identity columns missing from contract: {sorted(missing)}"

    # --- EPA / WPA derivation inputs ---

    def test_epa_wpa_derivation_inputs_present(self) -> None:
        contract = self._contract()
        derivation = {
            "posteam_score",
            "defteam_score",
            "game_half",
        }
        missing = derivation - contract
        assert not missing, f"EPA/WPA derivation inputs missing from contract: {sorted(missing)}"

    def test_contract_nonempty(self) -> None:
        assert len(self._contract()) >= 20, "Contract should have at least 20 columns"


# ---------------------------------------------------------------------------
# _EP_POINT_VALUES — canonical array
# ---------------------------------------------------------------------------


class TestEpPointValues:
    """_EP_POINT_VALUES mirrors nflfastR [7,-7,3,-3,2,-2,0] and the
    existing ep_class_to_score_mapping dict in model_vars."""

    def test_values_exact(self) -> None:
        from sportsdataverse.nfl.model_vars import _EP_POINT_VALUES

        expected = np.array([7.0, -7.0, 3.0, -3.0, 2.0, -2.0, 0.0], dtype=np.float64)
        np.testing.assert_array_equal(_EP_POINT_VALUES, expected)

    def test_dtype_float64(self) -> None:
        from sportsdataverse.nfl.model_vars import _EP_POINT_VALUES

        assert _EP_POINT_VALUES.dtype == np.float64

    def test_length_7(self) -> None:
        from sportsdataverse.nfl.model_vars import _EP_POINT_VALUES

        assert len(_EP_POINT_VALUES) == 7

    def test_consistent_with_ep_class_to_score_mapping(self) -> None:
        """Array must be consistent with the existing dict (same ordering)."""
        from sportsdataverse.nfl.model_vars import (
            _EP_POINT_VALUES,
            ep_class_to_score_mapping,
        )

        for idx, expected_score in ep_class_to_score_mapping.items():
            assert float(_EP_POINT_VALUES[idx]) == float(expected_score), (
                f"_EP_POINT_VALUES[{idx}]={_EP_POINT_VALUES[idx]} != ep_class_to_score_mapping[{idx}]={expected_score}"
            )

    def test_imported_from_ep_wp_is_same_object_values(self) -> None:
        """ep_wp.py must re-import from model_vars (not define its own copy)."""
        from sportsdataverse.nfl import ep_wp
        from sportsdataverse.nfl.model_vars import _EP_POINT_VALUES

        np.testing.assert_array_equal(ep_wp._EP_POINT_VALUES, _EP_POINT_VALUES)


# ---------------------------------------------------------------------------
# ERA_SEASON_CUTS — tuple of boundary years
# ---------------------------------------------------------------------------


class TestEraSeasonCuts:
    """ERA_SEASON_CUTS encodes the nflfastR-canonical era boundaries.

    Era assignment:
        era0  season <= 2001
        era1  2002 .. 2005
        era2  2006 .. 2013
        era3  2014 .. 2017
        era4  >= 2018
    Boundaries (upper inclusive per era0..3): (2001, 2005, 2013, 2017)
    """

    def test_is_tuple(self) -> None:
        from sportsdataverse.nfl.model_vars import ERA_SEASON_CUTS

        assert isinstance(ERA_SEASON_CUTS, tuple)

    def test_immutable(self) -> None:
        from sportsdataverse.nfl.model_vars import ERA_SEASON_CUTS

        with pytest.raises((AttributeError, TypeError)):
            ERA_SEASON_CUTS[0] = 9999  # type: ignore[index]

    def test_exact_values(self) -> None:
        """Must match the nflfastR-canonical cuts used in ep_wp._make_model_mutations."""
        from sportsdataverse.nfl.model_vars import ERA_SEASON_CUTS

        assert ERA_SEASON_CUTS == (2001, 2005, 2013, 2017), f"Expected (2001, 2005, 2013, 2017), got {ERA_SEASON_CUTS}"

    def test_four_boundaries_five_eras(self) -> None:
        from sportsdataverse.nfl.model_vars import ERA_SEASON_CUTS

        assert len(ERA_SEASON_CUTS) == 4, "4 boundary years → 5 eras (era0..era4)"

    def test_ascending(self) -> None:
        from sportsdataverse.nfl.model_vars import ERA_SEASON_CUTS

        assert list(ERA_SEASON_CUTS) == sorted(ERA_SEASON_CUTS)


# ---------------------------------------------------------------------------
# TOUCHBACK_YARDLINE_* — pre/post-2016 constants
# ---------------------------------------------------------------------------


class TestTouchbackYardlines:
    """Kickoff-touchback yardline constants (nflfastR canonical: 80 / 75)."""

    def test_pre_2016_is_80(self) -> None:
        from sportsdataverse.nfl.model_vars import TOUCHBACK_YARDLINE_PRE_2016

        assert TOUCHBACK_YARDLINE_PRE_2016 == 80

    def test_post_2016_is_75(self) -> None:
        from sportsdataverse.nfl.model_vars import TOUCHBACK_YARDLINE_POST_2016

        assert TOUCHBACK_YARDLINE_POST_2016 == 75

    def test_pre_gt_post(self) -> None:
        from sportsdataverse.nfl.model_vars import (
            TOUCHBACK_YARDLINE_POST_2016,
            TOUCHBACK_YARDLINE_PRE_2016,
        )

        assert TOUCHBACK_YARDLINE_PRE_2016 > TOUCHBACK_YARDLINE_POST_2016, (
            "Pre-2016 touchback line (80) should be deeper than post-2016 (75)"
        )

    def test_types_are_int(self) -> None:
        from sportsdataverse.nfl.model_vars import (
            TOUCHBACK_YARDLINE_POST_2016,
            TOUCHBACK_YARDLINE_PRE_2016,
        )

        assert isinstance(TOUCHBACK_YARDLINE_PRE_2016, int)
        assert isinstance(TOUCHBACK_YARDLINE_POST_2016, int)


# ---------------------------------------------------------------------------
# SPREAD_TIME_DECAY_EXPONENT — the "-4" in spread * exp(-4 * elapsed_share)
# ---------------------------------------------------------------------------


class TestSpreadTimeDecayExponent:
    """SPREAD_TIME_DECAY_EXPONENT encodes the -4 decay factor."""

    def test_value_is_negative_four(self) -> None:
        from sportsdataverse.nfl.model_vars import SPREAD_TIME_DECAY_EXPONENT

        assert SPREAD_TIME_DECAY_EXPONENT == -4.0

    def test_type_is_float(self) -> None:
        from sportsdataverse.nfl.model_vars import SPREAD_TIME_DECAY_EXPONENT

        assert isinstance(SPREAD_TIME_DECAY_EXPONENT, float)

    def test_formula_roundtrip(self) -> None:
        """spread_time = spread * exp(EXPONENT * elapsed_share)."""
        from sportsdataverse.nfl.model_vars import SPREAD_TIME_DECAY_EXPONENT

        elapsed = 0.5
        spread = 3.0
        result = spread * np.exp(SPREAD_TIME_DECAY_EXPONENT * elapsed)
        expected = spread * np.exp(-4.0 * elapsed)
        assert abs(result - expected) < 1e-12

    def test_appliers_read_one_source(self) -> None:
        """No applier hardcodes the exponent — every formula imports the constant.

        The exponent is fitted and travels with retrains through the trainer's
        model card; the card check in ``_load_booster_from`` can only mean
        something if the applier reads exactly ONE source.  A ``-4 * pl.col(``
        literal anywhere in ``ep_wp.py`` / ``nfl_pbp.py`` is a second source.
        """
        import re
        from pathlib import Path

        import sportsdataverse.nfl.ep_wp as ep_wp
        import sportsdataverse.nfl.nfl_pbp as nfl_pbp

        for mod in (ep_wp, nfl_pbp):
            src = Path(mod.__file__).read_text(encoding="utf-8")
            hits = re.findall(r"-4(?:\.0)?\s*\*\s*pl\.col", src)
            assert not hits, f"{mod.__name__} still hardcodes the spread_time decay exponent: {hits}"


# ---------------------------------------------------------------------------
# Model card check — a card beside an artifact must agree with the applier
# ---------------------------------------------------------------------------


class TestModelCardConstantCheck:
    """``_load_model`` refuses an artifact whose card records a different fitted constant.

    Exercises the production resolution path (``models_dir`` override) with a
    tiny real booster written to ``tmp_path``; a card is its ``.json`` sibling.
    """

    @staticmethod
    def _booster(tmp_path, name: str):
        import xgboost as xgb

        X = np.arange(20, dtype=np.float32).reshape(10, 2)
        y = np.array([0, 1] * 5)
        booster = xgb.train({"objective": "binary:logistic"}, xgb.DMatrix(X, label=y), num_boost_round=2)
        path = tmp_path / name
        booster.save_model(str(path))
        return path

    @staticmethod
    def _card(path, exponent) -> None:
        import json

        path.with_suffix(".json").write_text(
            json.dumps({"derived_feature_constants": {"spread_time_decay_exponent": exponent}}), encoding="utf-8"
        )

    def test_mismatched_card_raises(self, tmp_path) -> None:
        from sportsdataverse.nfl.ep_wp import _load_model

        path = self._booster(tmp_path, "wp_mismatch.ubj")
        self._card(path, -3.0)
        with pytest.raises(ValueError, match="spread_time_decay_exponent=-3.0"):
            _load_model("wp_mismatch.ubj", models_dir=tmp_path)

    def test_matching_card_loads(self, tmp_path) -> None:
        from sportsdataverse.nfl.ep_wp import _load_model
        from sportsdataverse.nfl.model_vars import SPREAD_TIME_DECAY_EXPONENT

        path = self._booster(tmp_path, "wp_match.ubj")
        self._card(path, SPREAD_TIME_DECAY_EXPONENT)
        assert _load_model("wp_match.ubj", models_dir=tmp_path).num_boosted_rounds() == 2

    def test_no_card_and_card_without_key_load(self, tmp_path) -> None:
        import json

        from sportsdataverse.nfl.ep_wp import _load_model

        self._booster(tmp_path, "wp_nocard.ubj")
        assert _load_model("wp_nocard.ubj", models_dir=tmp_path).num_boosted_rounds() == 2
        path = self._booster(tmp_path, "ep_card_no_key.ubj")
        path.with_suffix(".json").write_text(json.dumps({"model_type": "ep"}), encoding="utf-8")
        assert _load_model("ep_card_no_key.ubj", models_dir=tmp_path).num_boosted_rounds() == 2


# ---------------------------------------------------------------------------
# ERA_MAX_KNOWN_SEASON — a season past the validated eras warns, never silent
# ---------------------------------------------------------------------------


class TestEraMaxKnownSeason:
    """``era4`` is open-ended; a season beyond ``ERA_MAX_KNOWN_SEASON`` must warn."""

    @staticmethod
    def _ep_frame(season: int):
        import polars as pl

        return pl.DataFrame({"season": [season], "down": [1], "posteam": ["A"], "home_team": ["A"], "roof": ["dome"]})

    @staticmethod
    def _cp_frame(season: int):
        import polars as pl

        return pl.DataFrame(
            {
                "season": [season],
                "air_yards": [5.0],
                "ydstogo": [10],
                "down": [1],
                "posteam": ["A"],
                "home_team": ["A"],
                "pass_location": ["middle"],
                "roof": ["dome"],
            }
        )

    def test_value_covers_the_last_era_cut(self) -> None:
        from sportsdataverse.nfl.model_vars import ERA_MAX_KNOWN_SEASON, ERA_SEASON_CUTS

        assert isinstance(ERA_MAX_KNOWN_SEASON, int)
        assert ERA_MAX_KNOWN_SEASON >= 2025, "the era-aware retrain corpus ends 2025 (nfl-data DEFAULT_SEASONS)"
        assert ERA_MAX_KNOWN_SEASON > ERA_SEASON_CUTS[-1]

    def test_model_mutations_warn_beyond_max(self) -> None:
        from sportsdataverse.errors import EraCoverageWarning
        from sportsdataverse.nfl.ep_wp import _make_model_mutations
        from sportsdataverse.nfl.model_vars import ERA_MAX_KNOWN_SEASON

        beyond = ERA_MAX_KNOWN_SEASON + 1
        with pytest.warns(EraCoverageWarning, match=f"season {beyond} is beyond ERA_MAX_KNOWN_SEASON"):
            out = _make_model_mutations(self._ep_frame(beyond))
        assert out["era4"].to_list() == [1], "still scored under era4 — the warning is the flag, not a filter"

    def test_every_out_of_range_season_warns(self) -> None:
        """A mixed frame warns once per offending season, not just the max."""
        import warnings as _warnings

        import polars as pl

        from sportsdataverse.errors import EraCoverageWarning
        from sportsdataverse.nfl.ep_wp import _make_model_mutations
        from sportsdataverse.nfl.model_vars import ERA_MAX_KNOWN_SEASON

        a, b = ERA_MAX_KNOWN_SEASON + 1, ERA_MAX_KNOWN_SEASON + 2
        frame = pl.concat([self._ep_frame(ERA_MAX_KNOWN_SEASON), self._ep_frame(a), self._ep_frame(b)])
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always", EraCoverageWarning)
            _make_model_mutations(frame)
        seasons_warned = {
            s for s in (a, b, ERA_MAX_KNOWN_SEASON) if any(f"season {s} is beyond" in str(w.message) for w in caught)
        }
        assert seasons_warned == {a, b}, f"both offending seasons must warn, in-range must not; got {seasons_warned}"

    def test_cp_mutations_warn_beyond_max(self) -> None:
        from sportsdataverse.errors import EraCoverageWarning
        from sportsdataverse.nfl.ep_wp import _make_cp_mutations
        from sportsdataverse.nfl.model_vars import ERA_MAX_KNOWN_SEASON

        with pytest.warns(EraCoverageWarning):
            _make_cp_mutations(self._cp_frame(ERA_MAX_KNOWN_SEASON + 1))

    def test_silent_at_and_below_max(self) -> None:
        import warnings

        from sportsdataverse.nfl.ep_wp import _make_cp_mutations, _make_model_mutations
        from sportsdataverse.nfl.model_vars import ERA_MAX_KNOWN_SEASON

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _make_model_mutations(self._ep_frame(ERA_MAX_KNOWN_SEASON))
            _make_model_mutations(self._ep_frame(2017))
            _make_cp_mutations(self._cp_frame(ERA_MAX_KNOWN_SEASON))

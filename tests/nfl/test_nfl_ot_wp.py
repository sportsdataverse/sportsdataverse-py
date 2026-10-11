"""Overtime WP overlay — nflfastR ``add_wp_variables`` L820-899, by rule format.

Why this file exists
--------------------
nflfastR **never scores overtime with the WP boosters.**  It splits the frame
before prediction: the models run on ``qtr <= 4`` and ``qtr > 4`` gets a closed
form off the EP class probabilities, with ``vegas_wp`` assigned the same value
as ``wp`` (the spread model is not consulted at all).  sdv-py once scored
overtime with the boosters, which is a model extrapolation outside the domain
nflverse ever asks it about.

Why the form depends on the rule format
---------------------------------------
sdv-internal-refs ``rules/nfl.yaml`` catalogues four overtime rule eras
(``nfl-2010-overtime-modified-sudden-death-postseason``,
``nfl-2012-overtime-modified-sudden-death-regular-season``,
``nfl-2022-overtime-both-possess-postseason``,
``nfl-2025-overtime-both-possess-regular-season``; the 10-minute period of
``nfl-2017-overtime-ten-minutes`` enters no form).  nflfastR carries the
sudden-death and One_FG forms but keys One_FG on a batch-global
``First_Drive`` that is dead in the data it publishes, and has no
both-possess branch at all, so its published overtime ``wp`` is the pure
sudden-death form on practically every row.  sdv-py scopes ``Drive_Diff`` per
game and selects the form by ``(season, season_type)`` from
``model_vars.OVERTIME_FORMATS``; the owner accepted that nflverse parity moves
where the rule is right and nflfastR is wrong.

What is pinned
--------------
* the three closed forms, exactly, on a seam frame with hand-checked numbers
  (``0.389`` both-possess < ``0.431`` One_FG < ``0.55`` sudden death on the
  same opening-drive row);
* the format selected per (season, season_type), including the postseason
  cuts the fixture has no game for;
* ``vegas_wp == wp`` inside overtime and ONLY inside overtime;
* regulation rows are untouched, row order is preserved, ``Drive_Diff`` is
  scoped to each game's OVERTIME rows (regulation drives change nothing);
* spring-football leagues get null overtime WP (shootout overtime,
  ``rules/xfl.yaml`` ``xfl-2020-overtime-five-round-shootout``);
* a missing input WARNS instead of silently skipping;
* the real-data gate on five whole overtime games: parity with nflverse on
  the rows where every format reduces to sudden death (and that it FAILS
  without the overlay), the rule-right direction of the opening-drive gap,
  and the both-possess / One_FG forms recomputed on real 2025 / 2014 rows.
"""

from __future__ import annotations

import tempfile
import warnings
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nfl.ep_wp import (
    _EP_CLASS_NAMES,
    OT_ANSWERED_TD_WIN_SHARE,
    OT_TOUCHBACK_COLUMNS,
    OT_WP_EP_CLASSES,
    _apply_ot_wp_overlay,
    _ot_touchback_rescore,
    enrich_nfl_pbp,
)
from sportsdataverse.nfl.model_vars import (
    OT_BOTH_POSSESS,
    OT_MODIFIED_SUDDEN_DEATH,
    OT_SUDDEN_DEATH,
    OVERTIME_FORMATS,
    overtime_format,
)

# EP class probabilities held fixed so the closed forms have exact expected
# values.  Order matches _EP_CLASS_NAMES.
_TD, _OPP_TD, _FG, _OPP_FG, _SAF, _OPP_SAF, _NS = 0.31, 0.19, 0.22, 0.14, 0.02, 0.01, 0.11
_TB_TD, _WIN_BACK = 0.40, 0.55  # the touchback re-score seam
_SUDDEN_DEATH = _FG + _TD + _SAF  # 0.55
_ONE_FG = _TD + _FG * _WIN_BACK  # 0.431
_BOTH_FIRST = _TD * (1 - OT_ANSWERED_TD_WIN_SHARE * _TB_TD) + _FG * _WIN_BACK + _SAF  # 0.389


def _ot_frame(
    *,
    season: int = 2023,
    season_type: str = "REG",
    drives: tuple[float, ...] = (21.0, 22.0, 23.0),
    score_differential: tuple[float, ...] | None = None,
    qtr: tuple[float, ...] | None = None,
) -> pl.DataFrame:
    """A minimal post-``calculate_win_probability`` frame, one game, N rows.

    Carries the touchback re-score seam columns so the forms are exact and no
    EP model runs.
    """
    n = len(drives)
    score_differential = score_differential if score_differential is not None else (0.0,) * n
    qtr = qtr if qtr is not None else (5.0,) * n
    assert len(score_differential) == n and len(qtr) == n
    return pl.DataFrame(
        {
            "game_id": [f"{season}_01_AAA_BBB"] * n,
            "season": [season] * n,
            "season_type": [season_type] * n,
            "qtr": list(qtr),
            "drive": list(drives),
            "score_differential": list(score_differential),
            "td_prob": [_TD] * n,
            "opp_td_prob": [_OPP_TD] * n,
            "fg_prob": [_FG] * n,
            "opp_fg_prob": [_OPP_FG] * n,
            "safety_prob": [_SAF] * n,
            "opp_safety_prob": [_OPP_SAF] * n,
            "no_score_prob": [_NS] * n,
            OT_TOUCHBACK_COLUMNS[0]: [_TB_TD] * n,
            OT_TOUCHBACK_COLUMNS[1]: [_WIN_BACK] * n,
            # Placeholder booster output the overlay must overwrite.  Deliberately
            # far from any plausible closed-form value so the "did it move"
            # assertion cannot pass by coincidence.
            "wp": [0.999] * n,
            "vegas_wp": [0.001] * n,
        }
    )


# ---------------------------------------------------------------------------
# Format selection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("season", "season_type", "name", "period"),
    [
        (2009, "POST", OT_SUDDEN_DEATH, 900),
        (2010, "POST", OT_MODIFIED_SUDDEN_DEATH, 900),  # nfl-2010-overtime-modified-sudden-death-postseason
        (2011, "REG", OT_SUDDEN_DEATH, 900),
        (2012, "REG", OT_MODIFIED_SUDDEN_DEATH, 900),  # nfl-2012-overtime-modified-sudden-death-regular-season
        (2016, "REG", OT_MODIFIED_SUDDEN_DEATH, 900),
        (2017, "REG", OT_MODIFIED_SUDDEN_DEATH, 600),  # nfl-2017-overtime-ten-minutes
        (2021, "POST", OT_MODIFIED_SUDDEN_DEATH, 900),
        (2022, "POST", OT_BOTH_POSSESS, 900),  # nfl-2022-overtime-both-possess-postseason
        (2024, "REG", OT_MODIFIED_SUDDEN_DEATH, 600),
        (2024, "POST", OT_BOTH_POSSESS, 900),
        (2025, "REG", OT_BOTH_POSSESS, 600),  # nfl-2025-overtime-both-possess-regular-season
        (2026, "REG", OT_BOTH_POSSESS, 600),
    ],
)
def test_overtime_format_by_season_and_type(season, season_type, name, period):
    row = overtime_format(season, season_type)
    assert (row.name, row.period_seconds) == (name, period)


def test_overtime_formats_table_cites_the_catalog_and_is_newest_first():
    for rows in OVERTIME_FORMATS.values():
        assert [r.first_season for r in rows] == sorted((r.first_season for r in rows), reverse=True)
        assert rows[-1].first_season == 0
        assert all(r.rule_id.startswith("nfl-") for r in rows)


def test_unknown_season_type_is_scored_as_regular_season():
    assert overtime_format(2023, "WC").name == overtime_format(2023, "REG").name


# ---------------------------------------------------------------------------
# The closed forms (hand-checked)
# ---------------------------------------------------------------------------


def test_the_three_classes_are_the_posteam_scores_next_classes():
    assert OT_WP_EP_CLASSES == ("fg_prob", "td_prob", "safety_prob")
    assert set(OT_WP_EP_CLASSES) <= set(_EP_CLASS_NAMES)


def test_hand_checked_numbers():
    assert _SUDDEN_DEATH == pytest.approx(0.55)
    assert _ONE_FG == pytest.approx(0.431)
    assert _BOTH_FIRST == pytest.approx(0.389)
    assert _BOTH_FIRST < _ONE_FG < _SUDDEN_DEATH


@pytest.mark.parametrize(("season", "season_type"), [(2005, "REG"), (2011, "REG"), (2009, "POST")])
def test_pure_sudden_death_on_every_row(season, season_type):
    out = _apply_ot_wp_overlay(
        _ot_frame(
            season=season, season_type=season_type, drives=(21.0, 22.0, 23.0), score_differential=(0.0, -3.0, 0.0)
        )
    )
    assert np.allclose(out["wp"].to_numpy(), _SUDDEN_DEATH, atol=1e-12)


@pytest.mark.parametrize(("season", "season_type"), [(2012, "REG"), (2024, "REG"), (2010, "POST"), (2021, "POST")])
def test_modified_sudden_death_is_nflfastr_one_fg_on_the_opening_drive_and_the_trailing_by_three_answer(
    season, season_type
):
    out = _apply_ot_wp_overlay(
        _ot_frame(
            season=season,
            season_type=season_type,
            drives=(21.0, 21.0, 22.0, 22.0, 23.0),
            score_differential=(0.0, 0.0, -3.0, 0.0, 0.0),
        )
    )
    assert out["wp"].to_list() == pytest.approx([_ONE_FG, _ONE_FG, _ONE_FG, _SUDDEN_DEATH, _SUDDEN_DEATH])


@pytest.mark.parametrize(("season", "season_type"), [(2025, "REG"), (2022, "POST"), (2026, "REG")])
def test_both_possess_forms(season, season_type):
    """Opening drive, its try after a TD, the answer trailing by 7 / by 3, tied after an empty opener, dd >= 2."""
    out = _apply_ot_wp_overlay(
        _ot_frame(
            season=season,
            season_type=season_type,
            drives=(21.0, 21.0, 22.0, 22.0, 22.0, 23.0),
            score_differential=(0.0, 6.0, -7.0, -3.0, 0.0, 0.0),
        )
    )
    expected = [
        _BOTH_FIRST,
        1 - OT_ANSWERED_TD_WIN_SHARE * _TB_TD,  # 0.80: the try after the opening TD
        _TD * OT_ANSWERED_TD_WIN_SHARE,  # 0.155: answering, needs a TD
        _ONE_FG,  # answering trailing by 3
        _SUDDEN_DEATH,  # tied after an empty opening possession
        _SUDDEN_DEATH,  # both have possessed
    ]
    assert out["wp"].to_list() == pytest.approx(expected)


def test_a_first_possession_touchdown_leaves_wp_below_one_under_both_possess():
    """The rule the format exists for: the opening TD no longer ends the game."""
    out = _apply_ot_wp_overlay(_ot_frame(season=2025, drives=(21.0, 21.0), score_differential=(0.0, 6.0)))
    assert out["wp"][1] < 1.0
    assert out["wp"][1] == pytest.approx(1 - OT_ANSWERED_TD_WIN_SHARE * _TB_TD)


def test_the_opening_drive_form_varies_by_format_only():
    """Same row, three eras: the format is the only thing that moves the value."""
    vals = {
        name: _apply_ot_wp_overlay(_ot_frame(season=s, season_type=t))["wp"][0]
        for name, (s, t) in {"sd": (2011, "REG"), "mod": (2024, "REG"), "both": (2025, "REG")}.items()
    }
    assert vals["sd"] == pytest.approx(_SUDDEN_DEATH)
    assert vals["mod"] == pytest.approx(_ONE_FG)
    assert vals["both"] == pytest.approx(_BOTH_FIRST)


def test_a_frame_without_season_type_is_regular_season():
    df = _ot_frame(season=2024).drop("season_type")
    assert _apply_ot_wp_overlay(df)["wp"][0] == pytest.approx(_ONE_FG)


# ---------------------------------------------------------------------------
# Drive_Diff scoping, frame independence, scope, league gate, no-op guard
# ---------------------------------------------------------------------------


def test_the_form_does_not_depend_on_which_rows_share_the_frame():
    """Deterministic per game: another game's rows cannot move a value.

    This is the property upstream's batch-global ``First_Drive`` lacks.
    """
    alone = _apply_ot_wp_overlay(_ot_frame())["wp"].to_list()
    other = _ot_frame(drives=(25.0, 26.0, 27.0)).with_columns(pl.lit("2023_02_CCC_DDD").alias("game_id"))
    together = _apply_ot_wp_overlay(pl.concat([_ot_frame(), other]))["wp"].to_list()
    assert together[:3] == alone
    assert together[3:] == alone


def test_regulation_drive_numbers_in_the_frame_change_nothing():
    """``Drive_Diff`` reads the first OVERTIME drive of the game, not the game's first drive.

    A real frame carries the whole game, so a per-``game_id`` minimum over all
    rows would read the opening REGULATION drive and the One_FG / both-possess
    forms would never fire — the defect the first cut shipped with, invisible
    to an overtime-only fixture.
    """
    ot = _ot_frame(drives=(21.0, 22.0, 23.0))
    reg = _ot_frame(drives=(1.0, 2.0, 3.0), qtr=(1.0, 1.0, 2.0))
    with_reg = _apply_ot_wp_overlay(pl.concat([reg, ot]))
    assert with_reg["wp"].to_list()[:3] == [0.999, 0.999, 0.999]
    assert with_reg["wp"].to_list()[3:] == _apply_ot_wp_overlay(ot)["wp"].to_list()
    assert with_reg["wp"][3] == pytest.approx(_ONE_FG)  # the opening-drive form fired


def test_vegas_wp_equals_wp_inside_overtime():
    out = _apply_ot_wp_overlay(_ot_frame())
    assert np.allclose(out["wp"].to_numpy(), out["vegas_wp"].to_numpy(), atol=0.0, rtol=0.0)


def test_regulation_rows_are_untouched():
    out = _apply_ot_wp_overlay(_ot_frame(qtr=(4.0, 5.0, 5.0)))
    assert out["wp"][0] == 0.999
    assert out["vegas_wp"][0] == 0.001
    assert out["wp"][2] == pytest.approx(_SUDDEN_DEATH, abs=1e-12)


def test_a_frame_with_no_overtime_is_returned_unchanged():
    df = _ot_frame(qtr=(1.0, 2.0, 4.0))
    assert _apply_ot_wp_overlay(df).equals(df)


def test_row_order_is_preserved():
    df = pl.concat(
        [
            _ot_frame(drives=(1.0, 2.0), qtr=(1.0, 2.0)),
            _ot_frame(drives=(21.0, 22.0, 23.0)),
            _ot_frame(drives=(4.0,), qtr=(4.0,)),
        ]
    ).with_columns(pl.arange(0, 6).alias("_probe"))
    out = _apply_ot_wp_overlay(df)
    assert out["_probe"].to_list() == list(range(6))
    assert out["qtr"].to_list() == [1.0, 2.0, 5.0, 5.0, 5.0, 4.0]


def test_overlay_actually_moves_overtime_wp():
    """The silent-no-op assertion: assert the OUTPUT changed, not that it ran."""
    df = _ot_frame()
    out = _apply_ot_wp_overlay(df)
    assert bool(((out["wp"] - df["wp"]).abs() > 0.01).all())
    assert bool(((out["vegas_wp"] - df["vegas_wp"]).abs() > 0.01).all())


def test_supplied_seam_columns_are_kept_and_computed_ones_are_dropped():
    assert set(OT_TOUCHBACK_COLUMNS) <= set(_apply_ot_wp_overlay(_ot_frame()).columns)


@pytest.mark.parametrize("league", ["xfl", "ufl", "cfl"])
def test_spring_football_overtime_wp_is_null(league):
    """Shootout overtime (xfl-2020-overtime-five-round-shootout): no NFL form applies."""
    out = _apply_ot_wp_overlay(_ot_frame(qtr=(4.0, 5.0, 5.0)), league=league)
    assert out["wp"].to_list()[1:] == [None, None]
    assert out["vegas_wp"].to_list()[1:] == [None, None]
    assert out["wp"][0] == 0.999 and out["vegas_wp"][0] == 0.001


@pytest.mark.parametrize("missing", ["wp", "vegas_wp", "fg_prob", "td_prob", "safety_prob"])
def test_missing_input_warns_rather_than_silently_skipping(missing):
    df = _ot_frame().drop(missing)
    with pytest.warns(RuntimeWarning, match=f"overtime WP overlay skipped.*{missing}"):
        out = _apply_ot_wp_overlay(df)
    assert out.equals(df)


@pytest.mark.parametrize("missing", ["drive", "score_differential", "game_id"])
def test_missing_format_input_warns_and_falls_back_to_sudden_death(missing):
    df = _ot_frame(season=2025).drop(missing)
    with pytest.warns(RuntimeWarning, match=f"overtime WP overlay: missing.*{missing}"):
        out = _apply_ot_wp_overlay(df)
    assert np.allclose(out["wp"].to_numpy(), _SUDDEN_DEATH, atol=1e-12)


# ---------------------------------------------------------------------------
# Real-data gate — five whole overtime games, one per rules era
# ---------------------------------------------------------------------------

_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_ep_wp" / "overtime_games.parquet"

#: The fixture's five games and the format each plays under (all regular season).
_FIXTURE_FORMATS = {
    "2010_01_ATL_PIT": OT_SUDDEN_DEATH,
    "2014_01_BUF_CHI": OT_MODIFIED_SUDDEN_DEATH,
    "2019_01_DET_ARI": OT_MODIFIED_SUDDEN_DEATH,
    "2023_01_BUF_NYJ": OT_MODIFIED_SUDDEN_DEATH,
    "2025_02_NYG_DAL": OT_BOTH_POSSESS,
}

#: Parity thresholds on the overtime rows where every format reduces to the
#: sudden-death form (52 of the fixture's 96 paired rows: every 2010 row,
#: and the rows after both teams have possessed elsewhere).  Set 2026-10-10
#: from the observed MAE **0.0144**, bias **+0.0028**, r **0.9958** with the
#: overlay in place, strictly inside the un-overlaid values on the same rows
#: (MAE **0.1815**, r **0.6384**) so they detect a regression rather than
#: restate the present; ``test_the_gate_bites_without_the_overlay`` proves it.
#: The thresholds are the pre-format gate's, unchanged — the format-aware
#: overlay did not move the rows it did not mean to move.
OT_FIXTURE_MAE_CEILING = 0.035
OT_FIXTURE_ABS_BIAS_CEILING = 0.030
OT_FIXTURE_R_FLOOR = 0.98
#: Rows on a format form (One_FG / both-possess; 44 rows) sit BELOW nflverse by
#: construction — nflverse's dead One_FG branch leaves it on sudden death there.
#: Observed 2026-10-10: bias **-0.1433**, 90.9 % of rows below nflverse.
OT_FORMAT_ROWS_MAX_BIAS = -0.08
OT_FORMAT_ROWS_MIN_SHARE_BELOW = 0.85


def _enrich(df: pl.DataFrame, models_dir: str) -> pl.DataFrame:
    """Run the production nflverse path over the fixture (no network).

    ``models_dir`` points at an empty directory so the 34 MB xYAC model is
    skipped gracefully instead of downloaded.
    """
    base = df.drop([c for c in ("ep", "epa", "wp", "vegas_wp", "wpa", *_EP_CLASS_NAMES) if c in df.columns])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return enrich_nfl_pbp(base, method="lead_diff", add_fourth_down=False, models_dir=models_dir)


def _paired_ot(df: pl.DataFrame, out: pl.DataFrame) -> pl.DataFrame:
    """Overtime rows with the oracle attached, the sudden-death form and ``Drive_Diff``."""
    return (
        out.with_columns(df["nflverse_wp"], df["nflverse_vegas_wp"])
        .with_columns(
            (pl.col("td_prob") + pl.col("fg_prob") + pl.col("safety_prob")).alias("sd_form"),
            (pl.col("drive") - pl.when(pl.col("qtr") > 4).then(pl.col("drive")).min().over("game_id")).alias("dd"),
        )
        .filter((pl.col("qtr") >= 5) & pl.col("wp").is_finite() & pl.col("nflverse_wp").is_finite())
    )


def _scores(ot: pl.DataFrame, col: str, oracle: str) -> tuple[int, float, float, float]:
    a, b = ot[col].to_numpy(), ot[oracle].to_numpy()
    d = a - b
    return len(d), float(np.abs(d).mean()), float(d.mean()), float(np.corrcoef(a, b)[0, 1])


_ON_SD_FORM = (pl.col("wp") - pl.col("sd_form")).abs() < 1e-9


@pytest.fixture(scope="module")
def ot_games() -> pl.DataFrame:
    if not _FIXTURE.exists():  # pragma: no cover - fixture ships with the repo
        pytest.skip(f"missing fixture {_FIXTURE}")
    return pl.read_parquet(_FIXTURE)


@pytest.fixture(scope="module")
def enriched(ot_games) -> pl.DataFrame:
    with tempfile.TemporaryDirectory() as td:
        return _enrich(ot_games, td)


def test_fixture_games_resolve_to_the_expected_formats(ot_games):
    seen = ot_games.group_by("game_id").agg(pl.col("season").first(), pl.col("season_type").first())
    assert set(seen["game_id"].to_list()) == set(_FIXTURE_FORMATS)
    for gid, season, stype in seen.iter_rows():
        assert overtime_format(int(season), str(stype)).name == _FIXTURE_FORMATS[gid], gid


def test_real_overtime_matches_nflverse_where_every_format_is_sudden_death(ot_games, enriched):
    ot = _paired_ot(ot_games, enriched)
    sd = ot.filter(_ON_SD_FORM)
    assert sd.height >= 45, sd.height
    assert set(sd.filter(pl.col("game_id") == "2010_01_ATL_PIT")["game_id"]) == {"2010_01_ATL_PIT"}
    assert ot.filter(pl.col("game_id") == "2010_01_ATL_PIT").filter(~_ON_SD_FORM).height == 0
    for col, oracle in (("wp", "nflverse_wp"), ("vegas_wp", "nflverse_vegas_wp")):
        n, mae, bias, r = _scores(sd, col, oracle)
        assert mae <= OT_FIXTURE_MAE_CEILING, f"{col} MAE {mae:.4f}"
        assert abs(bias) <= OT_FIXTURE_ABS_BIAS_CEILING, f"{col} bias {bias:+.4f}"
        assert r >= OT_FIXTURE_R_FLOOR, f"{col} r {r:.4f}"


def test_real_opening_drives_from_2012_sit_below_nflverse(ot_games, enriched):
    """The rule-right divergence, in the direction the rule implies.

    From 2012 a first-possession field goal (and from 2025 a touchdown) no
    longer ends the game, so the opening-drive posteam's chances are lower
    than the sudden-death form nflverse publishes.
    """
    ot = _paired_ot(ot_games, enriched)
    fmt_rows = ot.filter(~_ON_SD_FORM)
    assert fmt_rows.height >= 40, fmt_rows.height
    assert fmt_rows.filter(pl.col("game_id") == "2010_01_ATL_PIT").height == 0
    d = (fmt_rows["wp"] - fmt_rows["nflverse_wp"]).to_numpy()
    assert d.mean() <= OT_FORMAT_ROWS_MAX_BIAS, f"bias {d.mean():+.4f}"
    assert (d < 0).mean() >= OT_FORMAT_ROWS_MIN_SHARE_BELOW, f"share below {(d < 0).mean():.3f}"
    # Every scored opening-drive play of the modified / both-possess games is below the sudden-death form.
    opening = ot.filter((pl.col("dd") == 0) & pl.col("posteam").is_not_null() & (pl.col("play_type") != "no_play"))
    for gid, g in opening.group_by("game_id", maintain_order=True):
        below = (g["wp"] - g["sd_form"]).to_numpy() < -1e-9
        if _FIXTURE_FORMATS[gid[0]] == OT_SUDDEN_DEATH:
            assert not below.any(), gid
        else:
            assert below.all(), (gid, below.mean())


def test_real_both_possess_and_one_fg_forms_recompute_on_real_rows(ot_games, enriched):
    """The closed forms hold exactly on the real 2025 (both-possess) and 2014 (One_FG) opening drives."""
    ot = _paired_ot(ot_games, enriched)
    tb = _ot_touchback_rescore(ot, pl.col("qtr") > 4, None)
    tb_td, win_back = pl.col(OT_TOUCHBACK_COLUMNS[0]), pl.col(OT_TOUCHBACK_COLUMNS[1])
    td, fg, saf = pl.col("td_prob"), pl.col("fg_prob"), pl.col("safety_prob")
    opening = tb.filter(
        (pl.col("dd") == 0) & (pl.col("score_differential") == 0) & pl.col("down").is_not_null()
    ).with_columns(
        (td * (1 - OT_ANSWERED_TD_WIN_SHARE * tb_td) + fg * win_back + saf).alias("both_form"),
        (td + fg * win_back).alias("one_fg_form"),
    )
    g25 = opening.filter(pl.col("game_id") == "2025_02_NYG_DAL")
    g14 = opening.filter(pl.col("game_id") == "2014_01_BUF_CHI")
    assert g25.height >= 5 and g14.height >= 4
    assert np.allclose(g25["wp"].to_numpy(), g25["both_form"].to_numpy(), atol=1e-9)
    assert np.allclose(g14["wp"].to_numpy(), g14["one_fg_form"].to_numpy(), atol=1e-9)
    # and the both-possess form is strictly below One_FG on the same rows (the TD no longer ends it)
    assert bool((g25["both_form"] < g25["one_fg_form"]).all())


def test_real_overtime_vegas_wp_equals_wp(ot_games, enriched):
    """nflverse holds this on 96.6 % of published 2025 overtime rows, against
    0.033 % in regulation.  We hold it exactly, because we never run the spread
    model in overtime."""
    out = enriched
    ot = out.filter((pl.col("qtr") >= 5) & pl.col("wp").is_not_null() & pl.col("vegas_wp").is_not_null())
    assert ot.height >= 90
    assert bool((ot["wp"] - ot["vegas_wp"]).abs().lt(1e-12).all())

    reg = out.filter((pl.col("qtr") <= 4) & pl.col("wp").is_not_null() & pl.col("vegas_wp").is_not_null())
    assert float((reg["wp"] - reg["vegas_wp"]).abs().lt(1e-12).mean()) < 0.01


def test_the_gate_bites_without_the_overlay(ot_games, enriched, tmp_path, monkeypatch):
    """A no-op overlay must FAIL the gate on the same rows — otherwise the gate proves nothing."""
    import sportsdataverse.nfl.ep_wp as _ep_wp

    sd_rows = _paired_ot(ot_games, enriched).filter(_ON_SD_FORM).select("game_id", "play_id")
    monkeypatch.setattr(_ep_wp, "_apply_ot_wp_overlay", lambda d, **kwargs: d)
    raw = _paired_ot(ot_games, _enrich(ot_games, str(tmp_path))).join(sd_rows, on=["game_id", "play_id"], how="inner")
    n, mae, bias, r = _scores(raw, "wp", "nflverse_wp")
    assert n >= 45
    assert mae > OT_FIXTURE_MAE_CEILING, f"un-overlaid MAE {mae:.4f} would have passed the ceiling"
    assert r < OT_FIXTURE_R_FLOOR, f"un-overlaid r {r:.4f} would have passed the floor"

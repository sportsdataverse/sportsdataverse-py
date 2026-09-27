"""Failing-first test for the summary/leaderboard ``_n`` sample-size descriptions.

Covers both generators that compose descriptions for the new ``_n`` columns
(F6 backfill; the sample sizes themselves come from cfbfastR-cfb-data#30/#33):

* ``gen_cfb_summary_descriptions.py`` -- the team grid
  (``load_cfb_team_summaries`` / ``_weekly``), where ``_n`` is the count behind
  a mean/ratio metric for one team-side/phase split.
* ``gen_cfb_player_percentile_descriptions.py`` -- the three player leaderboard
  loaders (``load_cfb_passing`` / ``_rushing`` / ``_receiving``), where ``_n``
  is the count behind a per-player rate.
"""

from tools.codegen.gen_cfb_player_percentile_descriptions import PLAYER_N_OF, n_desc
from tools.codegen.gen_cfb_summary_descriptions import describe


def test_team_grid_n_columns_name_their_denominator():
    assert describe("EPAplay_off_pass_n") == (
        "Sample size behind EPAplay_off_pass: the number of plays it is computed over on pass plays, "
        "with the team on offense. Null when the team has no rows in that split; read it as 0."
    )
    assert describe("EPAgame_def_n") == (
        "Sample size behind EPAgame_def: the number of games it is computed over, "
        "with the team on defense (i.e. allowed to opponents). "
        "Null when the team has no rows in that split; read it as 0."
    )
    assert describe("EPAplay_margin_n") is None  # a margin has no single sample
    assert (
        describe("EPAplay_off_rank")
        == "National rank of the team's EPA per play with the team on offense, where 1 is best."
    )


def test_player_n_columns_name_their_denominator():
    noun = PLAYER_N_OF["load_cfb_passing"]["EPAplay"]
    assert noun == "dropbacks (attempts, sacks and interceptions)"
    assert n_desc("EPAplay", noun, "passer") == (
        "Sample size behind EPAplay: the number of dropbacks (attempts, sacks and interceptions) "
        "the passer's value is computed over. 0 where EPAplay is null."
    )


def test_cfb_passer_rate_n_columns_contrast_with_the_nfl_dropback_convention():
    """CFB success_n / yardsplay_n count completions + incompletions (INTs and
    sacks excluded), which differs from the NFL twin's dropback-based count --
    the description must say so (program amendment 14)."""
    for base in ("success", "yardsplay"):
        noun = PLAYER_N_OF["load_cfb_passing"][base]
        assert "dropback" not in noun.lower()
        assert "interception" in noun.lower() and "sack" in noun.lower()
        desc = n_desc(base, noun, "passer", note="The NFL loader's equivalent counts dropbacks instead.")
        assert desc.endswith("The NFL loader's equivalent counts dropbacks instead.")


def test_cfb_comppct_n_counts_interceptions_but_not_sacks():
    noun = PLAYER_N_OF["load_cfb_passing"]["comppct"]
    assert "interception" in noun.lower()
    assert "sack" in noun.lower()


def test_weekly_team_summaries_is_a_describe_target():
    from tools.codegen.gen_cfb_summary_descriptions import main

    import inspect

    assert "load_cfb_team_summaries_weekly" in inspect.getsource(main)

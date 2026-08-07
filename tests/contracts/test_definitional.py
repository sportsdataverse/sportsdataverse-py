import polars as pl

from tools.validation.checks import definitional
from tools.validation.findings import CheckContext, Severity


def _ctx(dataset="cfb_model_pbp", domain="cfb", **kw):
    base = dict(domain=domain, dataset=dataset, schema={}, join_keys=("game_id",))
    base.update(kw)
    return CheckContext(**base)


def _findings_by_rule(findings):
    return {f.locator["rule"]: f for f in findings}


def test_unknown_dataset_has_no_rules():
    frame = pl.DataFrame({"rush": [True], "pass": [True]})
    assert definitional.run("no_such_dataset", frame, _ctx(dataset="no_such_dataset")) == []


def test_rule_with_missing_columns_is_skipped_not_failed():
    # only rush/pass present: every other rule must skip silently
    frame = pl.DataFrame({"rush": [False], "pass": [True]})
    assert definitional.run("cfb_model_pbp", frame, _ctx()) == []


def test_rush_pass_both_true_is_error_with_count_and_sample():
    frame = pl.DataFrame(
        {
            "game_id": [1, 1, 2],
            "rush": [True, False, True],
            "pass": [True, False, True],
        }
    )
    by_rule = _findings_by_rule(definitional.run("cfb_model_pbp", frame, _ctx()))
    f = by_rule["rush_pass_mutually_exclusive"]
    assert f.severity is Severity.ERROR
    assert f.metric == 2.0
    assert f.sample is not None and f.sample[0]["game_id"] == 1  # join key attached
    assert "rush" in f.sample[0]


def test_null_inputs_do_not_fire_identity_rules():
    # epa null on row 0, ep_after null on row 1 -> identity not evaluable, no finding
    frame = pl.DataFrame(
        {
            "epa": [None, 2.0, 0.5],
            "ep_after": [1.0, None, 1.5],
            "ep_before": [0.5, 0.5, 1.0],
        }
    )
    assert definitional.run("cfb_model_pbp", frame, _ctx()) == []


def test_epa_identity_violation_is_warn_needs_judgment():
    frame = pl.DataFrame({"epa": [9.0], "ep_after": [1.0], "ep_before": [0.5]})
    by_rule = _findings_by_rule(definitional.run("cfb_model_pbp", frame, _ctx()))
    f = by_rule["epa_is_ep_after_minus_ep_before"]
    assert f.severity is Severity.WARN and f.needs_judgment


def test_wpa_identity_violation_is_error():
    frame = pl.DataFrame({"wpa": [0.9], "wp_after": [0.6], "wp_before": [0.5]})
    by_rule = _findings_by_rule(definitional.run("cfb_model_pbp", frame, _ctx()))
    f = by_rule["wpa_is_wp_after_minus_wp_before"]
    assert f.severity is Severity.ERROR


def test_game_play_number_window_rule_respects_game_boundaries():
    # decreasing within game 1 fires; the drop across the 1->2 boundary must not
    frame = pl.DataFrame(
        {
            "game_id": [1, 1, 1, 2, 2],
            "game_play_number": [1, 3, 2, 1, 2],
        }
    )
    by_rule = _findings_by_rule(definitional.run("cfb_model_pbp", frame, _ctx()))
    assert by_rule["game_play_number_strictly_increasing"].metric == 1.0

    clean = pl.DataFrame({"game_id": [1, 1, 2], "game_play_number": [1, 2, 1]})
    assert definitional.run("cfb_model_pbp", clean, _ctx()) == []


def test_passing_down_definition_both_directions():
    # row0: down 2 & 9 flagged False (missed) / row1: down 1 flagged True (spurious)
    frame = pl.DataFrame(
        {
            "rush": [False, True],
            "pass": [True, False],
            "passing_down": [False, True],
            "start.down": [2, 1],
            "start.distance": [9, 10],
        }
    )
    by_rule = _findings_by_rule(definitional.run("cfb_model_pbp", frame, _ctx()))
    assert by_rule["passing_down_definition"].metric == 2.0


def test_nfl_td_subtype_rules():
    frame = pl.DataFrame(
        {
            "pass_touchdown": [1, 1, 0],
            "rush_touchdown": [1, 0, 0],
            "return_touchdown": [0, 0, 0],
            "touchdown": [1, 0, 0],  # row0: two subtypes; row1: subtype without touchdown
        }
    )
    ctx = _ctx(dataset="nfl_model_pbp", domain="nfl", join_keys=("game_id", "play_id"))
    by_rule = _findings_by_rule(definitional.run("nfl_model_pbp", frame, ctx))
    assert by_rule["td_subtypes_at_most_one"].metric == 1.0
    assert by_rule["td_subtype_implies_touchdown"].metric == 1.0


def test_nfl_name_id_pair_mismatch_fires_both_ways():
    frame = pl.DataFrame(
        {
            "passer_player_name": ["P.Mahomes", None, None],
            "passer_player_id": [None, "00-0033873", None],  # rows 0+1 half-populated
        }
    )
    ctx = _ctx(dataset="nfl_model_pbp", domain="nfl", join_keys=())
    by_rule = _findings_by_rule(definitional.run("nfl_model_pbp", frame, ctx))
    assert by_rule["passer_name_id_populated_together"].metric == 2.0


def test_nfl_passer_iff_pass_attempt_fires_both_directions():
    frame = pl.DataFrame(
        {
            "pass_attempt": [1, 0, 1, 0],
            "passer_player_name": [None, "P.Mahomes", "J.Allen", None],  # rows 0+1 violate
        }
    )
    ctx = _ctx(dataset="nfl_model_pbp", domain="nfl", join_keys=())
    by_rule = _findings_by_rule(definitional.run("nfl_model_pbp", frame, ctx))
    assert by_rule["passer_iff_pass_attempt"].metric == 2.0


def test_nfl_receiver_null_on_incomplete_is_allowed():
    # throwaway: incomplete pass with no target — must NOT fire
    frame = pl.DataFrame(
        {
            "pass_attempt": [1],
            "complete_pass": [0],
            "receiver_player_name": [None],
        }
    )
    ctx = _ctx(dataset="nfl_model_pbp", domain="nfl", join_keys=())
    assert definitional.run("nfl_model_pbp", frame, ctx) == []


def test_nfl_name_in_desc_literal_not_regex():
    # dots in "P.Mahomes" must match literally, not as regex wildcards
    frame = pl.DataFrame(
        {
            "rusher_player_name": ["P.Mahomes", "J.Allen"],
            "desc": ["P.Mahomes scrambles for 5 yards.", "K.Murray kneels."],  # row1 missing
        }
    )
    ctx = _ctx(dataset="nfl_model_pbp", domain="nfl", join_keys=())
    by_rule = _findings_by_rule(definitional.run("nfl_model_pbp", frame, ctx))
    assert by_rule["rusher_name_appears_in_desc"].metric == 1.0


def test_cfb_passer_on_non_pass_play_is_warn_needs_judgment():
    frame = pl.DataFrame({"pass": [False], "passer_player_name": ["Q.Ewers"]})
    by_rule = _findings_by_rule(definitional.run("cfb_model_pbp", frame, _ctx(join_keys=())))
    f = by_rule["passer_only_on_pass_plays"]
    assert f.severity is Severity.WARN and f.needs_judgment


def test_nfl_id_format_catches_float_artifact_and_empty():
    frame = pl.DataFrame(
        {
            "passer_player_id": ["00-0033873", "33873.0", "", None],  # rows 1+2 violate
        }
    )
    ctx = _ctx(dataset="nfl_model_pbp", domain="nfl", join_keys=())
    by_rule = _findings_by_rule(definitional.run("nfl_model_pbp", frame, ctx))
    assert by_rule["passer_player_id_format"].metric == 2.0


def test_nfl_game_id_format():
    frame = pl.DataFrame({"game_id": ["2024_01_BAL_KC", "401628319"]})  # ESPN-style id violates
    ctx = _ctx(dataset="nfl_model_pbp", domain="nfl", join_keys=())
    by_rule = _findings_by_rule(definitional.run("nfl_model_pbp", frame, ctx))
    assert by_rule["game_id_format"].metric == 1.0


def test_clean_string_rule_flags_empty_and_padded_not_null():
    frame = pl.DataFrame({"passer_player_name": ["Q.Ewers ", "", None, "J.Allen"]})
    ctx = _ctx(dataset="nfl_model_pbp", domain="nfl", join_keys=())
    by_rule = _findings_by_rule(definitional.run("nfl_model_pbp", frame, ctx))
    assert by_rule["passer_player_name_clean_string"].metric == 2.0  # null + clean rows pass


def test_cfb_drive_id_numeric_string_format():
    frame = pl.DataFrame({"drive.id": ["4016283191", "4016283191.0", None]})
    by_rule = _findings_by_rule(definitional.run("cfb_model_pbp", frame, _ctx(join_keys=())))
    assert by_rule["drive.id_format"].metric == 1.0


def test_nfl_posteam_participant_rules_allow_null_posteam():
    frame = pl.DataFrame(
        {
            "posteam": [None, "KC", "DEN"],  # null legit; DEN not a participant
            "defteam": ["BAL", "BAL", "BAL"],
            "home_team": ["KC", "KC", "KC"],
            "away_team": ["BAL", "BAL", "BAL"],
        }
    )
    ctx = _ctx(dataset="nfl_model_pbp", domain="nfl", join_keys=())
    by_rule = _findings_by_rule(definitional.run("nfl_model_pbp", frame, ctx))
    assert by_rule["posteam_is_a_participant"].metric == 1.0
    assert "defteam_is_a_participant" not in by_rule


def test_nfl_clock_hierarchy_skips_overtime():
    frame = pl.DataFrame(
        {
            "qtr": [5, 2],
            "quarter_seconds_remaining": [600, 100],
            "half_seconds_remaining": [600, 50],  # row1 violates in regulation
            "game_seconds_remaining": [0, 1850],
        }
    )
    ctx = _ctx(dataset="nfl_model_pbp", domain="nfl", join_keys=())
    by_rule = _findings_by_rule(definitional.run("nfl_model_pbp", frame, ctx))
    f = by_rule["clock_hierarchy_regulation"]
    assert f.metric == 1.0  # only the qtr=2 row; the OT row is out of scope

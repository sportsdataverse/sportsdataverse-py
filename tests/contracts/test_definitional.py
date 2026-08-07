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


def test_epa_rule_skipped_without_overlay_scope_columns():
    # the scoped contract rule needs text/type.text/period/etc; a frame
    # with only the numeric columns must skip it, not crash or fire
    frame = pl.DataFrame({"epa": [9.0], "ep_after": [1.0], "ep_before": [0.5]})
    assert definitional.run("cfb_model_pbp", frame, _ctx()) == []


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


def test_null_dtype_column_skips_string_rules_instead_of_crashing():
    # an all-null series polars types as Null; str.contains on it raises
    # InvalidOperationError (not SchemaError) on 1.42 — the fallback must
    # skip the rule, not crash run()
    frame = pl.DataFrame({"passer_player_id": [None, None]})
    ctx = _ctx(dataset="nfl_model_pbp", domain="nfl", join_keys=())
    assert definitional.run("nfl_model_pbp", frame, ctx) == []


def test_range_rule_flags_both_bounds_and_allows_null():
    frame = pl.DataFrame({"yds_rushed": [-100, -99, 99, 100, None]})  # rows 0+3 violate
    by_rule = _findings_by_rule(definitional.run("cfb_pbp", frame, _ctx(dataset="cfb_pbp", join_keys=())))
    assert by_rule["yds_rushed_range"].metric == 2.0


def test_cfb_pbp_penalty_subtype_requires_flag():
    frame = pl.DataFrame(
        {
            "penalty_declined": [True, True, False],
            "penalty_flag": [False, True, False],  # row0 violates
        }
    )
    by_rule = _findings_by_rule(definitional.run("cfb_pbp", frame, _ctx(dataset="cfb_pbp", join_keys=())))
    assert by_rule["penalty_declined_requires_penalty_flag"].metric == 1.0


def test_cfb_pbp_yds_penalty_numeric_string_is_warn():
    frame = pl.DataFrame({"yds_penalty": ["15", "-10", "U (", "k 8", None]})  # rows 2+3
    by_rule = _findings_by_rule(definitional.run("cfb_pbp", frame, _ctx(dataset="cfb_pbp", join_keys=())))
    f = by_rule["yds_penalty_numeric_string"]
    assert f.metric == 2.0 and f.severity is Severity.WARN and f.needs_judgment


def test_cfb_pbp_epa_contract_excludes_documented_overlays():
    # row0: end-of-half overlay (excluded); row1: penalty-in-text overlay
    # (excluded); row2: ordinary play violating the identity (fires)
    frame = pl.DataFrame(
        {
            "EPA": [-1.5, 0.9, 2.0],
            "EP_end": [1.0, 1.0, 1.0],
            "EP_start": [1.5, 0.5, 0.5],
            "end_of_half": [True, False, False],
            "scoring_play": [False, False, False],
            "penalty_in_text": [False, True, False],
            "type.text": ["Rush", "Rush", "Rush"],
            "kickoff_play": [False, False, False],
        }
    )
    by_rule = _findings_by_rule(definitional.run("cfb_pbp", frame, _ctx(dataset="cfb_pbp", join_keys=())))
    f = by_rule["epa_snapshot_identity_outside_overlays"]
    assert f.metric == 1.0 and f.severity is Severity.ERROR


def test_cfb_model_pbp_epa_contract_derived_exclusions():
    # game 1: play 1 ordinary violating (fires); play 2 last-of-half violating
    # (excluded); play 3 in period 3 penalty-text violating (excluded)
    frame = pl.DataFrame(
        {
            "game_id": [1, 1, 1],
            "game_play_number": [1, 2, 3],
            "period": [1, 2, 3],
            "epa": [9.0, 9.0, 9.0],
            "ep_after": [1.0, 1.0, 1.0],
            "ep_before": [0.5, 0.5, 0.5],
            "text": ["rush for 5 yds", "rush for 5 yds", "Penalty on the play, declined"],
            "type.text": ["Rush", "Rush", "Rush"],
        }
    )
    by_rule = _findings_by_rule(definitional.run("cfb_model_pbp", frame, _ctx()))
    assert by_rule["epa_snapshot_identity_outside_overlays"].metric == 1.0


def test_nfl_penalty_desc_rules():
    frame = pl.DataFrame(
        {
            "penalty": [1, 0, 1],
            "desc": ["rush, PENALTY holding", "PENALTY declined", "clean rush"],  # r1 WARN, r2 ERROR
        }
    )
    ctx = _ctx(dataset="nfl_model_pbp", domain="nfl", join_keys=())
    by_rule = _findings_by_rule(definitional.run("nfl_model_pbp", frame, ctx))
    assert by_rule["penalty_implies_desc_mention"].metric == 1.0
    f = by_rule["desc_penalty_mention_without_flag"]
    assert f.metric == 1.0 and f.severity is Severity.WARN


def test_nfl_kick_distance_rules():
    frame = pl.DataFrame(
        {
            "kick_distance": [45, None, 50],
            "punt_attempt": [0, 0, 0],
            "kickoff_attempt": [1, 1, 0],
            "field_goal_attempt": [0, 0, 0],  # row1: kickoff missing distance; row2: distance w/o kick
        }
    )
    ctx = _ctx(dataset="nfl_model_pbp", domain="nfl", join_keys=())
    by_rule = _findings_by_rule(definitional.run("nfl_model_pbp", frame, ctx))
    assert by_rule["kick_distance_populated_on_kickoff"].metric == 1.0
    assert by_rule["kick_distance_requires_kick_play"].metric == 1.0


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

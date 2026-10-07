"""Compose returns-table descriptions for the 16 NFL Pro (pro.nfl.com) Next Gen Stats tables.

The NGS field names are cryptic (``avgTTT``, ``croeNd``, ``bhPct``) and pro.nfl.com publishes
no column glossary, so nothing here is guessed from a name alone. Every description rests on
one of three checks run against the complete 2025 REG captures in
``sdv-internal-refs/nfl/nflpro/captures/secured/``:

* **nflverse crosswalk** -- the column's values equal a ``load_nfl_nextgen_stats`` column for
  the same players (``avg_ttt`` = ``avg_time_to_throw``, ``x_cmp`` = ``expected_completion_percentage``,
  ``cpoe`` = ``completion_percentage_above_expectation``, ``tw_att_pct`` = ``aggressiveness``,
  ``ay_att`` / ``ay_tgt`` = ``avg_intended_air_yards``, ``avg_sep`` = ``avg_separation``,
  ``catch`` = ``catch_percentage``, ``yac_rec`` = ``avg_yac``, ``rating`` = ``passer_rating``).
* **arithmetic identity** -- the column equals a formula of its siblings on every row
  (``cmp_pct = cmp/att``, ``cpoe = cmp_pct - x_cmp``, ``*_pg = */gp``, ``qbp_r = qbp/db`` on
  passers and ``qbp/pr`` on defenders, ``snap_pct = snap/team_snap``, ``total = pass + run``,
  ``epa = epa_pass + epa_rush``, ``total_takeaways = interception + fumble_recovered``,
  ``o_touch = rush_att + rec_rec``, ``o_opp = rush_att + rec_tgt``, ``fp_po_* = fp_*/o_opp``, ...).
* **envelope / identity field** -- read straight off the capture (``nfl_id``, ``team_id``,
  ``week_slug``, ``fapi_game_id``, ``is_home``, ``final_score``, ``game_result``).

Columns that none of the three checks reach (``avg_ttp``, ``avg_tts``, ``in_t_pct``,
``under_pct``, ``twf_pct``, ``bh_pct``, ``yacpr_nd``, ``t_stop``, ``h_stop``, ``rd``, ``cov``,
``cov_nd``, ``rec_tgt_quick``, ``o_snap3rd``, ...) are deliberately left blank -- a wrong
description survives review and is read downstream as fact.

Run:  uv run python tools/codegen/gen_nflpro_descriptions.py > <out.yaml>
Then: uv run python tools/codegen/merge_column_descriptions.py <out.yaml>
"""

from __future__ import annotations

import glob
import os
import sys

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCHEMA_DIR = os.path.join(ROOT, "tools", "codegen", "schemas", "native", "nflpro")

_NGS = "per Next Gen Stats"

# Identity / scope fields shared by every player table (read off the capture envelope).
_IDENTITY: dict[str, str] = {
    "nfl_id": "NFL player id (``nflId``) as a string; the key the wrappers accept as ``nfl_id``.",
    "display_name": "Player's full display name (e.g. 'Aaron Rodgers').",
    "short_name": "Player's abbreviated name, first initial and surname (e.g. 'A.Rodgers').",
    "headshot": "NFL headshot image URL template; the ``{formatInstructions}`` token must be replaced with a Cloudinary transform (e.g. ``t_headshot_desktop``) before use.",
    "team_id": "NFL team id as a zero-padded string (e.g. '0200', '3430'); casting it to a number drops the leading zero.",
    "jersey_number": "Jersey number the player wore in the season.",
    "position": "Roster position abbreviation (e.g. QB, WR, CB).",
    "position_group": "Position group the roster position rolls up to (e.g. QB, RB, WR, TE, DB).",
    "ngs_position": "Position Next Gen Stats assigns from tracking data; null for players NGS has not classified.",
    "ngs_position_group": "Position group of ngs_position; null when ngs_position is null.",
    "gp": "Games played in the season (and season type) -- the denominator of every ``*_pg`` per-game column.",
    "gs": "Games started in the season.",
    "tg": "Games the player's team played while he was on the roster; equals gp in sampled data.",
    "total_tg": "Total games the player's team played in the season type (17 for a full regular season).",
    "qp": "Whether the passer meets the league qualifying-attempts threshold (the ``qualified`` filter).",
    "qr": "Whether the player meets the league qualifying threshold for the table (the ``qualified`` filter).",
    "qd": "Whether the defender meets the league qualifying threshold for the table (the ``qualified`` filter).",
}

# Game-scope fields present on every ``*_week`` / ``fantasy_game`` table.
_GAME: dict[str, str] = {
    "week_slug": "Week slug of the game (e.g. 'WEEK_1', 'WEEK_18'); the week scope of the row.",
    "game_id": "NFL game id as an integer (e.g. 2024090900, the date followed by a two-digit sequence).",
    "fapi_game_id": "NFL Football API (FAPI) UUID of the game, the id api.nfl.com uses for the same game.",
    "opponent_team_id": "Opponent's NFL team id as a zero-padded string.",
    "is_home": "Whether the player's team was the home team in the game.",
    "final_score": "Final score as 'own-opponent' (e.g. '19-32' for a 32-19 loss).",
    "game_result": "Result from the player's team's side: 'W', 'L' or 'T'.",
}

_PASSING: dict[str, str] = {
    "cmp": "Pass completions, season total.",
    "att": "Pass attempts, season total.",
    "yds": "Passing yards, season total.",
    "td": "Passing touchdowns, season total.",
    "int": "Interceptions thrown, season total (a count, not a per-play flag).",
    "rating": "NFL passer rating for the season (the standard 0-158.3 formula; equals nflverse ``passer_rating``).",
    "ypa": "Yards per pass attempt (yds / att).",
    "cmp_pct": "Completion percentage as a fraction (cmp / att, e.g. 0.63).",
    "sack": "Times sacked, season total (a count, not a per-play flag).",
    "x_cmp": "Expected completion percentage as a fraction, "
    + _NGS
    + " (nflverse ``expected_completion_percentage`` / 100).",
    "cpoe": "Completion percentage over expected as a fraction: cmp_pct - x_cmp (nflverse ``completion_percentage_above_expectation`` / 100).",
    "db": "Dropbacks, season total (attempts plus sacks and scrambles); the denominator of qbp_r and epa_db.",
    "epa": "Total expected points added on the passer's dropbacks.",
    "epa_db": "Expected points added per dropback (epa / db).",
    "avg_ttt": "Average time to throw in seconds, snap to release, " + _NGS + " (nflverse ``avg_time_to_throw``).",
    "qbp": "Dropbacks on which the passer was pressured, season total.",
    "qbp_r": "Pressure rate: share of dropbacks on which the passer was pressured (qbp / db).",
    "blitz_r": "Blitz rate: share of the passer's dropbacks on which the defense blitzed, " + _NGS + ".",
    "drop": "Passes dropped by the passer's receivers, season total.",
    "drop_r": "Drop rate: drops per pass attempt (drop / att).",
    "ay": "Total intended air yards on pass attempts; ay_att is this per attempt.",
    "yac": "Total yards after the catch gained on the passer's completions.",
    "x_yac": "Total expected yards after the catch on the passer's completions, " + _NGS + ".",
    "yac_pct": "Share of passing yards gained after the catch (yac / yds).",
    "ay_att": "Average intended air yards per pass attempt (nflverse ``avg_intended_air_yards``).",
    "avg_sep": "Average separation in yards between the targeted receiver and the nearest defender at pass arrival on the passer's targets, "
    + _NGS
    + ".",
    "deep_att_pct": "Share of pass attempts NGS classifies as deep throws.",
    "tw_att_pct": "Aggressiveness: share of pass attempts into a tight window (defender within a yard of the receiver), "
    + _NGS
    + " (nflverse ``aggressiveness`` / 100).",
    "pa_db_pct": "Share of dropbacks that used play action.",
    "cmp_pg": "Completions per game (cmp / gp).",
    "att_pg": "Pass attempts per game (att / gp).",
    "yds_pg": "Passing yards per game (yds / gp).",
    "td_pg": "Passing touchdowns per game (td / gp).",
    "int_pg": "Interceptions thrown per game (int / gp).",
    "sack_pg": "Sacks taken per game (sack / gp).",
    "db_pg": "Dropbacks per game (db / gp).",
    "epa_pg": "Expected points added per game (epa / gp).",
    "qbp_pg": "Pressured dropbacks per game (qbp / gp).",
    "drop_pg": "Receiver drops per game (drop / gp).",
    "tw_att_pg": "Tight-window pass attempts per game; tw_att_pct = tw_att_pg / att_pg.",
}

_RUSHING: dict[str, str] = {
    "att": "Rush attempts, season total.",
    "yds": "Rushing yards, season total.",
    "td": "Rushing touchdowns, season total.",
    "ypc": "Yards per carry (yds / att).",
    "epa": "Total expected points added on the player's rush attempts.",
    "epa_att": "Expected points added per rush attempt (epa / att).",
    "x_ry": "Expected rushing yards, season total, " + _NGS + " (nflverse ``expected_rush_yards``); yds = x_ry + ryoe.",
    "x_ypc": "Expected yards per carry (x_ry / att).",
    "ryoe": "Rushing yards over expected, season total (yds - x_ry; nflverse ``rush_yards_over_expected``).",
    "ryoe_att": "Rushing yards over expected per attempt (ryoe / att).",
    "yaco": "Yards after contact, season total.",
    "yaco_att": "Yards after contact per attempt (yaco / att).",
    "ybco": "Yards before contact, season total.",
    "ybco_att": "Yards before contact per attempt (ybco / att).",
    "success": "Success rate: share of rush attempts graded successful, as a fraction.",
    "fum": "Fumbles on rush attempts, season total.",
    "lost": "Fumbles lost on rush attempts, season total.",
    "rush10_p_yds": "Rush attempts that gained 10 or more yards, season total.",
    "rush15_p_mph": "Rush attempts on which the ball carrier reached 15+ mph, " + _NGS + ".",
    "rush20_p_mph": "Rush attempts on which the ball carrier reached 20+ mph, " + _NGS + ".",
    "eff": "Rushing efficiency: distance travelled per rushing yard gained (lower is more direct), "
    + _NGS
    + " (nflverse ``efficiency``).",
    "st_box_pct": "Share of rush attempts against a stacked box (8 or more defenders), " + _NGS + ".",
    "att_pg": "Rush attempts per game (att / gp).",
    "yds_pg": "Rushing yards per game (yds / gp).",
    "td_pg": "Rushing touchdowns per game (td / gp).",
    "epa_pg": "Expected points added per game (epa / gp).",
    "x_ry_pg": "Expected rushing yards per game (x_ry / gp).",
    "ryoe_pg": "Rushing yards over expected per game (ryoe / gp).",
    "yaco_pg": "Yards after contact per game (yaco / gp).",
    "ybco_pg": "Yards before contact per game (ybco / gp).",
    "fum_pg": "Fumbles per game (fum / gp).",
    "lost_pg": "Fumbles lost per game (lost / gp).",
    "rush10_p_yds_pg": "Rushes of 10+ yards per game (rush10_p_yds / gp).",
    "rush15_p_mph_pg": "Rushes reaching 15+ mph per game (rush15_p_mph / gp).",
    "rush20_p_mph_pg": "Rushes reaching 20+ mph per game (rush20_p_mph / gp).",
}

_RECEIVING: dict[str, str] = {
    "rt": "Routes run, season total.",
    "tgt": "Targets, season total.",
    "rec": "Receptions, season total.",
    "yds": "Receiving yards, season total.",
    "td": "Receiving touchdowns, season total.",
    "int": "Interceptions thrown on passes targeting the player, season total.",
    "rating": "Passer rating on throws targeting the player (the standard formula applied to his targets).",
    "catch": "Catch rate as a fraction (rec / tgt; nflverse ``catch_percentage`` / 100).",
    "x_catch": "Expected catch rate as a fraction, " + _NGS + "; catch = x_catch + croe.",
    "croe": "Catch rate over expected as a fraction (catch - x_catch).",
    "yds_rec": "Yards per reception (yds / rec).",
    "yds_rt": "Yards per route run (yds / rt).",
    "epa": "Total expected points added on the player's targets.",
    "epa_tgt": "Expected points added per target (epa / tgt).",
    "epa_rt": "Expected points added per route run (epa / rt).",
    "drop": "Drops, season total.",
    "drop_tgt": "Drop rate: drops per target (drop / tgt).",
    "yac": "Yards after the catch, season total.",
    "x_yac": "Expected yards after the catch, season total, " + _NGS + "; yac = x_yac + yacoe.",
    "yacoe": "Yards after the catch over expected, season total (yac - x_yac).",
    "yac_rec": "Yards after the catch per reception (yac / rec; nflverse ``avg_yac``).",
    "avg_sep": "Average separation in yards from the nearest defender at pass arrival, "
    + _NGS
    + " (nflverse ``avg_separation``).",
    "ay": "Total intended air yards on the player's targets.",
    "ay_tgt": "Average intended air yards per target (ay / tgt; nflverse ``avg_intended_air_yards``).",
    "tgt_rt": "Target rate: targets per route run (tgt / rt).",
    "avg_rt_dep": "Average route depth in yards, " + _NGS + ".",
    "ez_tgt": "End-zone targets, season total.",
    "ez_rec": "End-zone receptions, season total.",
    "deep_tgt_pct": "Share of targets NGS classifies as deep.",
    "tw_pct": "Share of targets thrown into a tight window (defender within a yard), " + _NGS + ".",
    "rt_pg": "Routes run per game (rt / gp).",
    "tgt_pg": "Targets per game (tgt / gp).",
    "rec_pg": "Receptions per game (rec / gp).",
    "yds_pg": "Receiving yards per game (yds / gp).",
    "td_pg": "Receiving touchdowns per game (td / gp).",
    "int_pg": "Interceptions on the player's targets per game (int / gp).",
    "epa_pg": "Expected points added per game (epa / gp).",
    "drop_pg": "Drops per game (drop / gp).",
    "yac_pg": "Yards after the catch per game (yac / gp).",
    "x_yac_pg": "Expected yards after the catch per game (x_yac / gp).",
    "yacoe_pg": "Yards after the catch over expected per game (yacoe / gp).",
    "ay_pg": "Intended air yards per game (ay / gp).",
    "ez_tgt_pg": "End-zone targets per game (ez_tgt / gp).",
    "ez_rec_pg": "End-zone receptions per game (ez_rec / gp).",
}

# Nearest-defender (``_nd``) coverage stats shared by the two defense tables.
_NEAREST: dict[str, str] = {
    "tgt_nd": "Targets on which the player was the nearest defender, season total, " + _NGS + ".",
    "rec_nd": "Receptions allowed as the nearest defender, season total.",
    "rec_yds_nd": "Receiving yards allowed as the nearest defender, season total.",
    "rec_td_nd": "Receiving touchdowns allowed as the nearest defender, season total.",
    "int": "Interceptions made, season total (a count, not a per-play flag).",
    "pass_rating_nd": "Passer rating allowed on targets where the player was the nearest defender.",
    "catch_nd": "Catch rate allowed as the nearest defender (rec_nd / tgt_nd), as a fraction.",
    "croe_nd": "Catch rate over expected allowed as the nearest defender (actual minus expected catch rate), "
    + _NGS
    + ".",
    "tgt_epa_nd": "Total expected points added allowed on targets where the player was the nearest defender.",
    "tgt_r_nd": "Target rate as the nearest defender: share of coverage snaps on which the receiver he covered was targeted.",
    "sep": "Average separation in yards allowed at pass arrival as the nearest defender, " + _NGS + ".",
    "game_snap": "Defensive snaps the player played, season total; equals snap on the overview table.",
    "team_snap": "Defensive snaps the player's team played, season total; the denominator of snap_pct.",
}

_DEFENSE_OVERVIEW: dict[str, str] = {
    **_NEAREST,
    "snap": "Defensive snaps played, season total.",
    "snap_pct": "Share of the team's defensive snaps the player played (snap / team_snap).",
    "pr": "Pass-rush snaps, season total; the denominator of qbp_r.",
    "tck": "Tackles, season total.",
    "qbp": "Quarterback pressures, season total.",
    "qbp_r": "Pressure rate: pressures per pass-rush snap (qbp / pr).",
    "sack": "Sacks, season total (a count, not a per-play flag).",
}

_TEAM: dict[str, str] = {
    "team_id": "NFL team id as a zero-padded string (e.g. '0200'); casting it to a number drops the leading zero.",
    "gp": "Games played in the season (and season type); the denominator of every per-game column.",
    "total": "Offensive plays, season total (pass + run).",
    "pass": "Pass plays, season total.",
    "run": "Run plays, season total.",
    "pass_pct": "Share of plays that were passes (pass / total).",
    "ppg": "Points per game.",
    "yds": "Yards from scrimmage, season total (pass_yds + rush_yds).",
    "ypg": "Yards per game (yds / gp).",
    "ypp": "Yards per play (yds / total).",
    "td": "Touchdowns, season total.",
    "epa": "Total expected points added over all plays (epa_pass + epa_rush).",
    "epa_pp": "Expected points added per play (epa / total).",
    "pass_yds": "Net passing yards, season total.",
    "pass_ypg": "Passing yards per game (pass_yds / gp).",
    "pass_ypp": "Passing yards per pass play (pass_yds / pass).",
    "sacked_yds": "Yards lost to sacks, season total.",
    "sacked_ypg": "Sack yards lost per game (sacked_yds / gp).",
    "epa_pass": "Total expected points added on pass plays.",
    "epa_pass_pp": "Expected points added per pass play (epa_pass / pass).",
    "rush_yds": "Rushing yards, season total.",
    "rush_ypg": "Rushing yards per game (rush_yds / gp).",
    "rush_ypp": "Rushing yards per run play (rush_yds / run).",
    "epa_rush": "Total expected points added on run plays.",
    "epa_rush_pp": "Expected points added per run play (epa_rush / run).",
    "ryoe": "Rushing yards over expected, season total, " + _NGS + ".",
    "ttt": "Average time to throw in seconds on pass plays, " + _NGS + ".",
    "qbp": "Quarterback pressures on pass plays, season total.",
    "qbp_pct": "Pressure rate: share of pass plays with a quarterback pressure.",
}
_TEAM_DEFENSE: dict[str, str] = {
    **{k: v.replace("Offensive plays", "Defensive plays faced") for k, v in _TEAM.items()},
    "pass_td": "Passing touchdowns allowed, season total.",
    "rush_td": "Rushing touchdowns allowed, season total.",
    "interception": "Interceptions made, season total.",
    "forced_fumble": "Fumbles forced, season total.",
    "fumble_recovered": "Opponent fumbles recovered, season total.",
    "defensive_touchdown": "Defensive touchdowns scored, season total.",
    "total_takeaways": "Takeaways, season total (interception + fumble_recovered).",
}
for _k in (
    "ppg",
    "yds",
    "ypg",
    "ypp",
    "td",
    "epa",
    "epa_pp",
    "pass_yds",
    "pass_ypg",
    "pass_ypp",
    "epa_pass",
    "epa_pass_pp",
    "rush_yds",
    "rush_ypg",
    "rush_ypp",
    "epa_rush",
    "epa_rush_pp",
    "ryoe",
    "ttt",
    "qbp",
    "qbp_pct",
    "sacked_yds",
    "sacked_ypg",
    "pass",
    "run",
    "pass_pct",
):
    _TEAM_DEFENSE[_k] = _TEAM_DEFENSE[_k].rstrip(".") + ", from the defense's side (allowed / faced)."

# Fantasy tables: nflfantasy-style prefixes pass_ / rush_ / rec_ / kick_ / misc_ / o_ / fp_.
_FANTASY: dict[str, str] = {
    "o_snap": "Offensive snaps played.",
    "o_tm_snap": "Offensive snaps the player's team played; the denominator of pt_pct.",
    "o_snap_pg": "Offensive snaps per game (o_snap / gp).",
    "pt_pct": "Playing-time share: offensive snaps played over the team's offensive snaps (o_snap / o_tm_snap).",
    "pass_cmp": "Pass completions.",
    "pass_att": "Pass attempts.",
    "pass_yd": "Passing yards.",
    "pass_td": "Passing touchdowns.",
    "pass_int": "Interceptions thrown.",
    "pass_two_pt_conv": "Two-point conversions passed.",
    "pass_db": "Dropbacks.",
    "pass_cmp_pct": "Completion percentage as a fraction (pass_cmp / pass_att).",
    "pass_exp_cmp_pct": "Expected completion percentage as a fraction, "
    + _NGS
    + "; pass_cmp_pct = pass_exp_cmp_pct + pass_cpoe.",
    "pass_cpoe": "Completion percentage over expected as a fraction (pass_cmp_pct - pass_exp_cmp_pct).",
    "pass_rating": "NFL passer rating (the standard 0-158.3 formula).",
    "pass_avg_ttt": "Average time to throw in seconds, " + _NGS + ".",
    "pass_ay_pa": "Average intended air yards per pass attempt.",
    "pass_deep_att_pct": "Share of pass attempts NGS classifies as deep throws.",
    "pass_yac_pct": "Share of passing yards gained after the catch.",
    "pass_qbp": "Dropbacks on which the passer was pressured.",
    "pass_qbp_pct": "Pressure rate: share of dropbacks with pressure (pass_qbp / pass_db).",
    "pass_sack": "Times sacked (a count, not a per-play flag).",
    "pass_sack_pg": "Sacks taken per game (pass_sack / gp).",
    "pass_cmp_pg": "Completions per game (pass_cmp / gp).",
    "pass_att_pg": "Pass attempts per game (pass_att / gp).",
    "pass_yd_pg": "Passing yards per game (pass_yd / gp).",
    "pass_td_pg": "Passing touchdowns per game (pass_td / gp).",
    "pass_int_pg": "Interceptions thrown per game (pass_int / gp).",
    "pass_db_pg": "Dropbacks per game (pass_db / gp).",
    "rush_att": "Rush attempts.",
    "rush_yd": "Rushing yards (rush_exp_yd + rush_ryoe).",
    "rush_td": "Rushing touchdowns.",
    "rush_two_pt_conv": "Two-point conversions rushed.",
    "rush_exp_yd": "Expected rushing yards, " + _NGS + ".",
    "rush_ryoe": "Rushing yards over expected (rush_yd - rush_exp_yd).",
    "rush_att_pg": "Rush attempts per game (rush_att / gp).",
    "rush_yd_pg": "Rushing yards per game (rush_yd / gp).",
    "rush_yd_pa": "Rushing yards per attempt (rush_yd / rush_att).",
    "rush_yaco": "Rushing yards after contact.",
    "rush_ybco": "Rushing yards before contact.",
    "rush_yaco_pa": "Yards after contact per rush attempt (rush_yaco / rush_att).",
    "rush_ybco_pa": "Yards before contact per rush attempt (rush_ybco / rush_att).",
    "rush_stuffed": "Rush attempts stuffed (stopped at or behind the line of scrimmage).",
    "rush_td_pg": "Rushing touchdowns per game (rush_td / gp).",
    "scr_rush_att": "Scramble rush attempts (quarterback runs off a dropback).",
    "scr_rush_yd": "Scramble rushing yards.",
    "scr_rush_td": "Scramble rushing touchdowns.",
    "scr_rush_pct": "Scramble rate: scrambles per dropback (scr_rush_att / pass_db).",
    "design_rush_att": "Designed rush attempts (called runs, excluding scrambles).",
    "design_rush_yd": "Designed-rush yards.",
    "design_rush_td": "Designed-rush touchdowns.",
    "rush_rz_att": "Rush attempts inside the red zone.",
    "rush_gl_att": "Rush attempts at the goal line.",
    "rush10_plus_yd": "Rush attempts that gained 10 or more yards.",
    "rec_rt": "Routes run.",
    "rec_tgt": "Targets.",
    "rec_rec": "Receptions.",
    "rec_yd": "Receiving yards.",
    "rec_td": "Receiving touchdowns.",
    "rec_two_pt_conv": "Two-point conversions received.",
    "rec_rt_pg": "Routes run per game (rec_rt / gp).",
    "rec_tgt_pg": "Targets per game (rec_tgt / gp).",
    "rec_rec_pg": "Receptions per game (rec_rec / gp).",
    "rec_yd_pg": "Receiving yards per game (rec_yd / gp).",
    "rec_td_pg": "Receiving touchdowns per game (rec_td / gp).",
    "rec_catch_pct": "Catch rate as a fraction (rec_rec / rec_tgt).",
    "rec_ay_share": "Share of the team's intended air yards thrown to the player.",
    "rec_tgt_rate": "Target rate: targets per route run (rec_tgt / rec_rt).",
    "rec_tgt_share": "Share of the team's targets thrown to the player.",
    "rec_rt_part_pct": "Route participation: share of the team's dropbacks on which the player ran a route.",
    "rec_tgt_play_act": "Targets on play-action dropbacks.",
    "rec_ez_tgt": "End-zone targets.",
    "rec_ez_rec": "End-zone receptions.",
    "rec_rz_tgt": "Targets inside the red zone.",
    "rec_ay_tgt": "Total intended air yards on targets (rec_ay_rec + rec_ay_unrealized).",
    "rec_ay_rec": "Air yards on completed targets (realized air yards).",
    "rec_ay_unrealized": "Air yards on incomplete targets (unrealized air yards).",
    "rec_ay_pt": "Average intended air yards per target.",
    "rec_tgt_ay10_plus": "Targets of 10 or more intended air yards.",
    "rec_yd_p_rt": "Receiving yards per route run (rec_yd / rec_rt).",
    "rec_yd_pt": "Receiving yards per target (rec_yd / rec_tgt).",
    "rec_yd_pr": "Receiving yards per reception (rec_yd / rec_rec).",
    "rec_yac": "Yards after the catch.",
    "rec_exp_yac": "Expected yards after the catch, " + _NGS + ".",
    "rec_yacoe": "Yards after the catch over expected (rec_yac - rec_exp_yac).",
    "kick_xp_att": "Extra-point attempts.",
    "kick_xp_made": "Extra points made.",
    "kick_fg_att": "Field-goal attempts.",
    "kick_fg_made": "Field goals made.",
    "kick_fg_miss": "Field goals missed.",
    "kick_fg_made_less40": "Field goals made from under 40 yards.",
    "kick_fg_made40_to49": "Field goals made from 40-49 yards.",
    "kick_fg_made50_to59": "Field goals made from 50-59 yards.",
    "kick_fg_made60_plus": "Field goals made from 60 yards or more.",
    "misc_kickoff_ret_td": "Kickoff-return touchdowns.",
    "misc_punt_ret_td": "Punt-return touchdowns.",
    "misc_fum_rec_td": "Fumble-recovery touchdowns.",
    "misc_fum_lost": "Fumbles lost.",
    "misc_fum": "Fumbles.",
    "misc_int_ret_td": "Interception-return touchdowns.",
    "misc_fum_ret_td": "Fumble-return touchdowns.",
    "misc_blk_punt_fg_ret_td": "Touchdowns on blocked-punt or blocked-field-goal returns.",
    "misc_two_pt_ret": "Two-point conversion returns (defensive two-point scores).",
    "misc_one_pt_safety": "One-point safeties.",
    "o_touch": "Touches: rush attempts plus receptions (rush_att + rec_rec).",
    "o_opp": "Opportunities: rush attempts plus targets (rush_att + rec_tgt).",
    "o_opp_pg": "Opportunities per game (o_opp / gp).",
    "o_miss_tkl_forced": "Missed tackles forced on touches.",
    "o_miss_tkl_forced_pct": "Missed tackles forced per touch (o_miss_tkl_forced / o_touch).",
    "o_tm_db": "Team dropbacks.",
    "o_tm_pass_pct": "Team pass rate: team dropbacks over team offensive snaps (o_tm_db / o_tm_snap).",
    "o_tm_ppg": "Team points per game.",
    "o_tm_yd_pg": "Team yards per game.",
    "rz_opp": "Red-zone opportunities: red-zone rushes plus red-zone targets (rush_rz_att + rec_rz_tgt).",
    "fp_std": "Fantasy points, standard (non-PPR) scoring.",
    "fp_half_ppr": "Fantasy points, half-PPR scoring.",
    "fp_ppr": "Fantasy points, full-PPR scoring.",
    "fp_pass": "Fantasy points from passing.",
    "fp_rush": "Fantasy points from rushing.",
    "fp_rec_std": "Fantasy points from receiving, standard scoring.",
    "fp_rec_half_ppr": "Fantasy points from receiving, half-PPR scoring.",
    "fp_rec_ppr": "Fantasy points from receiving, full-PPR scoring.",
    "fp_kick": "Fantasy points from kicking.",
    "fp_misc": "Fantasy points from return, fumble-recovery and other miscellaneous scoring.",
    "fp_pg_std": "Fantasy points per game, standard scoring (fp_std / gp).",
    "fp_pg_half_ppr": "Fantasy points per game, half-PPR scoring (fp_half_ppr / gp).",
    "fp_pgppr": "Fantasy points per game, full-PPR scoring (fp_ppr / gp).",
    "fp_pos_rk_std": "Positional fantasy rank, standard scoring (1 = top scorer at the position).",
    "fp_pos_rk_half_ppr": "Positional fantasy rank, half-PPR scoring (1 = top scorer at the position).",
    "fp_pos_rk_ppr": "Positional fantasy rank, full-PPR scoring (1 = top scorer at the position).",
    "fp_pos_rk_lbl_std": "Positional rank label, standard scoring (e.g. 'QB1', 'RB12').",
    "fp_pos_rk_lbl_half_ppr": "Positional rank label, half-PPR scoring (e.g. 'QB1', 'RB12').",
    "fp_pos_rk_lbl_ppr": "Positional rank label, full-PPR scoring (e.g. 'QB1', 'RB12').",
    "fp_ps_std": "Fantasy points per offensive snap, standard scoring (fp_std / o_snap).",
    "fp_ps_half_ppr": "Fantasy points per offensive snap, half-PPR scoring (fp_half_ppr / o_snap).",
    "fp_psppr": "Fantasy points per offensive snap, full-PPR scoring (fp_ppr / o_snap).",
    "fp_p_rt_std": "Fantasy points per route run, standard scoring.",
    "fp_p_rt_half_ppr": "Fantasy points per route run, half-PPR scoring.",
    "fp_p_rt_ppr": "Fantasy points per route run, full-PPR scoring.",
    "fp_pt_std": "Fantasy points per touch, standard scoring.",
    "fp_pt_half_ppr": "Fantasy points per touch, half-PPR scoring.",
    "fp_ptppr": "Fantasy points per touch, full-PPR scoring.",
    "fp_po_std": "Fantasy points per opportunity, standard scoring (fp_std / o_opp).",
    "fp_po_half_ppr": "Fantasy points per opportunity, half-PPR scoring (fp_half_ppr / o_opp).",
    "fp_poppr": "Fantasy points per opportunity, full-PPR scoring (fp_ppr / o_opp).",
}


def _top_finishes(season: bool) -> dict[str, str]:
    out = {}
    scoring = {"std": "standard", "half_ppr": "half-PPR", "ppr": "full-PPR"}
    for n, pos in (
        (5, "qb"),
        (12, "qb"),
        (12, "rb"),
        (24, "rb"),
        (12, "wr"),
        (24, "wr"),
        (36, "wr"),
        (5, "te"),
        (12, "te"),
        (5, "k"),
        (12, "k"),
    ):
        for sc, label in scoring.items():
            what = f"a top-{n} {pos.upper()} by fantasy points"
            out[f"top{n}_{pos}_wk_{sc}"] = (
                f"Weeks the player finished as {what}, {label} scoring, season count."
                if season
                else f"Whether the player finished the week as {what}, {label} scoring (1/0)."
            )
    return out


_TABLES: dict[str, dict[str, str]] = {
    "players_offense_passing_season": {**_IDENTITY, **_PASSING},
    "players_offense_passing_week": {**_IDENTITY, **_GAME, **_PASSING},
    "players_offense_rushing_season": {**_IDENTITY, **_RUSHING},
    "players_offense_rushing_week": {**_IDENTITY, **_GAME, **_RUSHING},
    "players_offense_receiving_season": {**_IDENTITY, **_RECEIVING},
    "players_offense_receiving_week": {**_IDENTITY, **_GAME, **_RECEIVING},
    "defense_overview_season": {**_IDENTITY, **_DEFENSE_OVERVIEW},
    "defense_overview_week": {**_IDENTITY, **_GAME, **_DEFENSE_OVERVIEW},
    "defense_nearest_season": {**_IDENTITY, **_NEAREST},
    "defense_nearest_week": {**_IDENTITY, **_GAME, **_NEAREST},
    "team_offense_overview_season": _TEAM,
    "team_offense_overview_week": {**_TEAM, **_GAME},
    "team_defense_overview_season": _TEAM_DEFENSE,
    "team_defense_overview_week": {**_TEAM_DEFENSE, **_GAME},
    "fantasy_season": {**_IDENTITY, **_FANTASY, **_top_finishes(season=True)},
    "fantasy_game": {**_IDENTITY, **_GAME, **_FANTASY, **_top_finishes(season=False)},
}


def _week_scope(text: str) -> str:
    """A ``*_week`` / ``fantasy_game`` row is one game, so 'season total' reads 'in the game'."""
    return text.replace(", season total", " in the game").replace(" season count", " count")


def main() -> None:
    out: dict[str, dict[str, str]] = {}
    for f in sorted(glob.glob(os.path.join(SCHEMA_DIR, "*.yaml"))):
        d = yaml.safe_load(open(f, encoding="utf-8")) or {}
        schema = d.get("schema")
        table = _TABLES.get(schema)
        if not table:
            continue
        weekly = schema.endswith("_week") or schema == "fantasy_game"
        cols = [c["name"] for c in d.get("columns") or []]
        block = {c: (_week_scope(table[c]) if weekly else table[c]) for c in cols if c in table}
        if schema.startswith("fantasy_"):
            # the fantasy dicts name the stat only; say which scope the number covers
            scope = " in the game." if weekly else ", season total."
            block = {c: (d[:-1] + scope if len(d) < 15 else d) for c, d in block.items()}
        out[schema] = block
    yaml.safe_dump(out, sys.stdout, sort_keys=True, allow_unicode=True, width=120)


if __name__ == "__main__":
    main()

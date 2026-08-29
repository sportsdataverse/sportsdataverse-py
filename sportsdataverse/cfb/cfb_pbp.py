from __future__ import annotations

import json
import logging
import os
import re
import time
import unicodedata
from functools import reduce
from importlib.resources import files as _resource_files

import numpy as np
import pandas as pd
import polars as pl
from xgboost import Booster, DMatrix

#: ESPN's OWN verdict that a play did not count. This is the reliable negation
#: signal and it is deliberately preferred over deriving negation ourselves from
#: the infraction name, for two reasons measured on the release (see
#: ``dev/penalty-analysis/DESIGN.md``):
#:
#: * a play can carry two fouls on the same side where one is declined and the
#:   other accepted, so "declined" in the text does NOT mean the play stood; and
#: * a live-ball foul plus a dead-ball foul can BOTH be accepted, negating the
#:   play *and* adding yardage.
#:
#: ESPN's marker already reflects the net enforcement outcome across every
#: penalty on the play. Plays carrying it have a ``down_repeated`` rate of
#: 0.727-0.782 against a 0.155 no-penalty baseline.
_PENALTY_NEGATED_TEXT = r"(?i)no play|nullified by penalty"

#: Infractions that WIPE OUT the play, measured by how often the down is
#: replayed among plays with penalty text and no explicit outcome marker
#: (no-penalty baseline 0.155): false start 0.976, ineligible downfield 0.968,
#: delay of game 0.919, offside/encroachment 0.761, offensive holding 0.738,
#: illegal formation/shift/motion 0.627.
_PENALTY_NEGATING_FOUL = (
    r"(?i)false start|ineligible|delay of game|offside|encroachment|neutral zone"
    r"|holding|illegal (formation|shift|motion|substitution)|clipping|chop block"
)

#: Infractions after which the play STANDS and the down advances: intentional
#: grounding 0.045 (it is a loss of down, so the down advancing is correct) and
#: illegal forward pass 0.175 -- both at or below the 0.155 baseline.
_PENALTY_PLAY_STANDS_FOUL = r"(?i)intentional grounding|illegal forward pass"

#: Infractions carrying an AUTOMATIC FIRST DOWN. These are deliberately NOT
#: classified. A negated play resets the down to 1 rather than repeating it, so
#: the replay signal cannot distinguish "wiped out the play" from "dead-ball
#: foul after a play that stood" -- pass interference measures 0.445 purely as
#: an artefact of that. ~2,400 leaderboard-reaching plays and roughly +2,650 EPA
#: sit here; they resolve to ``unknown`` until a valid instrument exists.
#: Guessing is how cfbfastR-cfb-data#30 shipped.
_PENALTY_AUTO_FIRST_DOWN_FOUL = r"(?i)pass interference|personal foul|face ?mask|roughing|targeting|unsportsmanlike"


def _cfb_resource_filename(package: str, resource: str) -> str:
    """Drop-in replacement for the deprecated ``pkg_resources.resource_filename``.

    Uses :func:`importlib.resources.files` (stdlib, available since Python 3.9)
    and resolves the path eagerly. setuptools 81+ removed ``pkg_resources``,
    which made the legacy import emit a UserWarning at module load time and
    (eventually) break entirely.
    """
    return str(_resource_files(package).joinpath(resource))


def _n_capture_groups(pattern: str) -> int:
    """Count capturing groups in *pattern* (skips ``\\(`` literals and ``(?...`` groups)."""
    n = 0
    i = 0
    while i < len(pattern):
        c = pattern[i]
        if c == "\\":
            i += 2
            continue
        if c == "(" and not (i + 1 < len(pattern) and pattern[i + 1] == "?"):
            n += 1
        i += 1
    return n


def _extract_player_name(text_expr: pl.Expr, pattern: str) -> pl.Expr:
    """Extract a player name from play text, robust to multi-alternative patterns.

    polars ``str.extract`` defaults to capture group 1. When *pattern* is an
    alternation whose branches each carry their own capture group (e.g.
    ``(A )run |(A )rush ``), the matched branch's name may live in group 2, 3,
    ... -- so the default group-1 extract returns ``None``. ESPN play text uses
    "rush"/"on-side"/"Punt by {name}"/"returned by {name}" phrasings that match
    such non-first alternatives, which is why pre-2014 games (which carry no
    structured ``participants[]`` array to overwrite the regex output) had null
    rusher / punter / kicker / returner names. This coalesces across every
    capture group so the matched branch's name is returned regardless of its
    position. Single-group patterns fall through to the plain group-1 extract.
    """
    n = _n_capture_groups(pattern)
    if n <= 1:
        return text_expr.str.extract(pattern, 1)
    groups = text_expr.str.extract_groups(pattern)
    return pl.coalesce([groups.struct.field(str(i)) for i in range(1, n + 1)])


#: Formation tags ESPN prepends to play text. The name captures are windowed
#: (e.g. ``(.{0,30} )pass ``), so a prefix short enough to fit inside the window
#: is swallowed whole into the name -- "No Huddle-Shotgun #1 C.Parker" is 29
#: characters -- and a longer one is captured TRUNCATED, starting mid-token
#: ("dle-Shotgun #5 R.Marshall", "le-Shotgun #20 N.Laughlin"). Either way it
#: reaches the box score as a phantom player, so this cannot anchor on the whole
#: tag: it consumes everything through the LAST formation token, which handles a
#: partial leading fragment as well as the intact prefix. Greedy on purpose --
#: "No Huddle-Shotgun" carries two tokens and only the last one ends the prefix.
#:
#: The keyword must END a word. v0.1.3 shipped this with a trailing ``[\s\-]*``,
#: which matches the empty string, on the stated assumption that "no real surname
#: contains huddle or shotgun". Huddleston does: "Rakeem Cato sacked by Parrish
#: Huddleston" is 2014 text, and the pattern turned that name into "ston". Rust
#: regex has no lookaround, so the word end is spelled as a required separator --
#: which also fixes a second defect, the bracketed form "[No Huddle, Shotgun], "
#: previously leaving a stray "]" on the front of the name.
_FORMATION_PREFIX = r"(?i)^.*(?:huddle|shotgun)[\s\-,\]]+"

#: ESPN renders the jersey inline ("#1 C.Parker"). It is never part of a name,
#: and leaving it on splits a player across rows when only some plays fall back
#: to the regex. Three digits: 2025's vendor feed uses #99 and higher.
_JERSEY_PREFIX = r"^\s*#\d{1,3}\s+"

#: 2025's vendor template opens each play with the game clock -- "(15:00) #36
#: T.Morrison kickoff 65 yards to the SAC00" -- and a capture that starts at the
#: beginning of the text takes it along. Stripped BEFORE the jersey, since the
#: clock precedes it. Published 2025 carries 3,682 kickoff_player_name values of
#: the form "(15:00) #36 T.Morrison", plus rusher, passer and interception names.
_CLOCK_PREFIX = r"^\s*\(\d{1,2}:\d{2}\)\s*"


def _strip_presentational_tokens(name_expr: pl.Expr) -> pl.Expr:
    """Remove formation tags and the jersey number from an extracted name.

    The play text carries presentational tokens that are not part of anyone's
    name. ``cleaned_text`` already strips them for the verb-anchored parsers,
    but the player-name captures read raw ``text`` -- so whatever survives the
    capture window lands in the box score verbatim.

    This only affects the regex FALLBACK: when ESPN supplies a participant the
    join overwrites the name entirely. It matters where ESPN does not -- that
    feed had a null passer on 96 of 210 plays for game 401896383, which is how
    "No Huddle-Shotgun #1 C.Parker" reached the passing table as a third
    quarterback alongside the same player's real line.
    """
    return (
        name_expr.str.replace(_CLOCK_PREFIX, "")
        .str.replace(_FORMATION_PREFIX, "")
        .str.replace(_CLOCK_PREFIX, "")
        .str.replace(_JERSEY_PREFIX, "")
        .str.strip_chars()
    )


#: A play-text artifact masquerading as a name if it contains one of these as a
#: whole word (real surnames effectively never do). Used to null garbage
#: extractions like "bea loss of" / "for a loss" before the roster id-join.
_PLAYER_NAME_GARBAGE = re.compile(
    r"(?i)\b(loss|gain|yards?|incomplete|penalty|fumbled|sacked|touchdown|kickoff|punt|return|hurried by|QB)\b"
)

#: ESPN's pre-2014 play text renders a name as ``"Player Name (TEAM)"`` --
#: e.g. ``"Bryan Randall (VT) pass incomplete to the right side."``. The rusher
#: capture strips the parenthetical but the passer capture keeps it, and
#: :func:`_norm_player_name` then folds it into the join key
#: (``"matt ryan bc" != "matt ryan"``), so every 2004 passer failed the roster
#: id-join. Stripping a trailing ``(ABBR)`` at the shared cleanup chokepoint
#: fixes the emitted name column AND the join in one place.
_PLAYER_NAME_TEAM_SUFFIX = re.compile(r"\s*\([A-Za-z][\w&'.\- ]{0,11}\)\s*$")

#: Narrative tails that bleed into a captured name where the extraction regex
#: has no stopword boundary: pass-direction phrases (``"Russell Wilson deep
#: out"``, ``"Dominique Davis screen"``) and missing-space concatenations in
#: ESPN's own text (``"Raynard Hornetackled by"``). Anchored to end-of-string
#: and matched without a leading word boundary so the concatenated form sheds
#: the tail and keeps the surname.
_PLAYER_NAME_TAIL = re.compile(
    r"(?i)\s*(?:tackled\s+by|pushed\s+out\s+of\s+bounds|ran\s+out\s+of\s+bounds"
    r"|deep\s+(?:left|right|middle|out)|short\s+(?:left|right|middle)"
    r"|sideline|crossing|screen|middle|scramble)\s*\.?\s*$"
)


def _norm_player_name(name) -> str:
    """Normalize a player name for roster matching.

    ASCII-folds, lowercases, drops punctuation + generational suffixes
    (Jr/Sr/II/III/IV/V), and collapses whitespace. Returns ``""`` for empty or
    the ``"Team"`` sentinel so they never match a roster entry.
    """
    if not name or name == "Team":
        return ""
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    s = s.lower().strip().rstrip(".")
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", "", s)
    s = re.sub(r"[^a-z0-9 ]", "", s)
    return re.sub(r"\s+", " ", s).strip()


from sportsdataverse.cfb.model_vars import (
    defense_score_vec,
    end_change_vec,
    ep_class_to_score_mapping,
    ep_end_columns,
    ep_final_names,
    ep_start_columns,
    ep_start_touchback_columns,
    int_vec,
    kickoff_turnovers,
    kickoff_vec,
    normalplay,
    offense_score_vec,
    penalty,
    punt_vec,
    qbr_vars,
    scores_vec,
    turnover_vec,
    wp_end_columns,
    wp_final_names,
    wp_naive_end_columns,
    wp_naive_final_names,
    wp_naive_start_columns,
    wp_naive_start_touchback_columns,
    wp_start_columns,
    wp_start_touchback_columns,
)
from sportsdataverse.dl_utils import download

ep_model_file = _cfb_resource_filename("sportsdataverse", "cfb/models/ep_model.ubj")
wp_spread_file = _cfb_resource_filename("sportsdataverse", "cfb/models/wp_spread.ubj")
wp_naive_file = _cfb_resource_filename("sportsdataverse", "cfb/models/wp_naive.ubj")
qbr_model_file = _cfb_resource_filename("sportsdataverse", "cfb/models/qbr_model.ubj")
cp_model_file = _cfb_resource_filename("sportsdataverse", "cfb/models/cfb_cp_model.ubj")
xpass_model_file = _cfb_resource_filename("sportsdataverse", "cfb/models/xpass_model.ubj")

ep_model = Booster({"nthread": 4})  # init model
ep_model.load_model(ep_model_file)

wp_model = Booster({"nthread": 4})  # init model
wp_model.load_model(wp_spread_file)

# Spread-free win-probability booster (12-feat = wp_final_names minus spread_time).
wp_naive_model = Booster({"nthread": 4})  # init model
wp_naive_model.load_model(wp_naive_file)

qbr_model = Booster({"nthread": 4})  # init model
qbr_model.load_model(qbr_model_file)

# Completion-probability booster (8-feat, binary:logistic) -> cp / cpoe.
cp_model = Booster({"nthread": 4})  # init model
cp_model.load_model(cp_model_file)

# Expected-pass booster (7-feat, binary:logistic) -> xpass / pass_oe.
xpass_model = Booster({"nthread": 4})  # init model
xpass_model.load_model(xpass_model_file)

# Faithful feature-name orders the boosters were trained with. The DMatrix
# is built from a pandas frame whose columns are renamed to these exact
# names so xgboost validates feature_names alignment.
CP_FEATURES = [
    "down",
    "distance",
    "yards_to_goal",
    "score_diff",
    "seconds_remaining",
    "is_home",
    "period",
    "passing_down",
]
XPASS_FEATURES = [
    "down",
    "distance",
    "yards_to_goal",
    "pos_score_diff",
    "TimeSecsRem",
    "era",
    "period",
]

logger = logging.getLogger("sdv.cfb_pbp")
logger.addHandler(logging.NullHandler())


def _wp_predict(play_df, model, names, tb_cols, start_cols, end_cols):
    """Project the WP feature columns, rename to the booster's feature names, and
    predict the start-touchback / start / end win-probabilities. Shared by the
    spread (13-feat) and naive (12-feat) models — the only differences are the
    column-source lists and feature-name list passed in.
    """
    tb = play_df[tb_cols]
    tb.columns = names
    start = play_df[start_cols]
    start.columns = names
    end = play_df[end_cols]
    end.columns = names
    return (
        model.predict(DMatrix(tb)),
        model.predict(DMatrix(start)),
        model.predict(DMatrix(end)),
    )


def _apply_wp_derivation(play_df, wp_before_raw, wp_touchback_raw, wp_after_raw, suffix="", wp_after_flip_raw=None):
    """Apply the win-probability game-logic derivation to a set of raw model
    predictions, writing suffixed output columns. With ``suffix=""`` this emits
    the canonical ``wp_before`` / ``wp_after`` / ``wpa`` (+ home/away/def) columns;
    with ``suffix="_naive"`` it emits the spread-free analogues. The adjustment
    chain (kickoff touchback, end-of-half, change-of-possession, end-of-game) is
    model-independent, so spread and naive share it verbatim.
    """
    wb = f"wp_before{suffix}"
    wt = f"wp_touchback{suffix}"
    wa = f"wp_after{suffix}"
    dwb = f"def_wp_before{suffix}"
    hwb = f"home_wp_before{suffix}"
    awb = f"away_wp_before{suffix}"
    lwb = f"lead_wp_before{suffix}"
    lwb2 = f"lead_wp_before2{suffix}"
    dwa = f"def_wp_after{suffix}"
    hwa = f"home_wp_after{suffix}"
    awa = f"away_wp_after{suffix}"
    wpa = f"wpa{suffix}"
    # B5: the penalty-assessed-on-kickoff play takes the touchback wp_before too
    # (0.36-live L5095), alongside the existing kickoff_vec substitution. The flag is
    # only present on the full-pipeline frame; guard on its presence so the helper
    # stays callable on a minimal synthetic frame (kickoff-only fallback).
    touchback_mask = pl.col("type.text").is_in(kickoff_vec)
    if "penalty_assessed_on_kickoff" in play_df.columns:
        touchback_mask = touchback_mask.or_(pl.col("penalty_assessed_on_kickoff") == True)
    # Game-ender win-prob (the two status_type_completed branches below) must reflect
    # the END-possession team's actual final score, not pos_score_diff_end. On a
    # possession-flipping final play whose type is absent from end_change_vec (notably
    # a SAFETY), pos_score_diff_end stays in the pos_team perspective while the
    # home/away mapping keys off end.pos_team.id -> the losing team wrongly gets 1.0.
    # Deriving the winner from the absolute end score relative to end.pos_team.id is
    # perspective-consistent with that mapping and changes no model feature.
    # Guard: synthetic test frames omit end.homeScore/end.awayScore; fall back to
    # pos_score_diff_end so those tests remain valid (status_type_completed=False
    # means the game-ender branches never fire on synthetic data anyway).
    if "end.homeScore" in play_df.columns and "end.awayScore" in play_df.columns:
        # B7: stated from the START-possession team's point of view, because that is
        # the perspective wp_after carries everywhere else (see the mapping below).
        # The game-ender branches are the only ones that would otherwise be written in
        # the END team's terms, and on a possession-flipping final play -- a safety, a
        # pick six -- the two differ.
        end_team_score_diff = (
            pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
            .then(pl.col("end.homeScore").cast(pl.Int64) - pl.col("end.awayScore").cast(pl.Int64))
            .otherwise(pl.col("end.awayScore").cast(pl.Int64) - pl.col("end.homeScore").cast(pl.Int64))
        )
    else:
        end_team_score_diff = pl.col("pos_score_diff_end")
    return (
        play_df.with_columns(
            pl.lit(wp_before_raw).alias(wb),
            pl.lit(wp_touchback_raw).alias(wt),
            pl.lit(wp_after_raw).alias(wa),
            # kept aside: the model's own end-state WP, for the one row that has no
            # next play to borrow from (B14 below)
            pl.lit(wp_after_raw).alias(f"_wa_raw{suffix}"),
            pl.lit(wp_after_flip_raw if wp_after_flip_raw is not None else wp_after_raw).alias(f"_wa_flip{suffix}"),
        )
        .with_columns(
            pl.when(touchback_mask).then(pl.col(wt)).otherwise(pl.col(wb)).alias(wb),
        )
        .with_columns(
            (1 - pl.col(wb)).alias(dwb),
        )
        .with_columns(
            pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
            .then(pl.col(wb))
            .otherwise(pl.col(dwb))
            .alias(hwb),
            pl.when(pl.col("start.pos_team.id") != pl.col("homeTeamId"))
            .then(pl.col(wb))
            .otherwise(pl.col(dwb))
            .alias(awb),
        )
        .with_columns(
            pl.col(wb).shift(-1).alias(lwb),
            pl.col(wb).shift(-2).alias(lwb2),
        )
        .with_columns(
            pl.when(pl.col("type.text").is_in(["Timeout"]))
            .then(pl.col(wb))
            .when(
                (pl.col("status_type_completed") == True)
                .and_(
                    (pl.col("lead_play_type").is_null()).or_(
                        pl.col("game_play_number") == pl.col("game_play_number").max(),
                    ),
                )
                .and_(end_team_score_diff > 0),
            )
            .then(1.0)
            .when(
                (pl.col("status_type_completed") == True)
                .and_(
                    (pl.col("lead_play_type").is_null()).or_(
                        pl.col("game_play_number") == pl.col("game_play_number").max(),
                    ),
                )
                .and_(end_team_score_diff < 0),
            )
            .then(0.0)
            .when(
                (pl.col("end_of_half") == True)
                .and_(pl.col("start.pos_team.id") == pl.col("lead_pos_team"))
                .and_(pl.col("type.text") != "Timeout"),
            )
            .then(pl.col(lwb))
            .when(
                (pl.col("end_of_half") == True)
                .and_(pl.col("start.pos_team.id") != pl.col("end.pos_team.id"))
                .and_(pl.col("type.text") != "Timeout"),
            )
            .then(1 - pl.col(lwb))
            .when(
                (pl.col("end_of_half") == True)
                .and_(pl.col("start.pos_team_receives_2H_kickoff") == False)
                .and_(pl.col("type.text") == "Timeout"),
            )
            .then(pl.col(wa))
            .when(
                (pl.col("lead_play_type").is_in(["End Period", "End of Half"])).and_(
                    pl.col("change_of_pos_team") == False,
                ),
            )
            .then(pl.col(lwb))
            .when(
                (pl.col("lead_play_type").is_in(["End Period", "End of Half"])).and_(
                    pl.col("change_of_pos_team") == True,
                ),
            )
            .then(1 - pl.col(lwb))
            .when((pl.col("kickoff_onside") == True).and_(pl.col("change_of_pos_team") == True))
            .then(pl.col(wa))
            # B6: the flip below converts lead_wp_before -- which is stated in the
            # NEXT play's possession perspective -- into this row's perspective. That
            # is only a conversion when the next play really is the other team's
            # possession. It usually is: on a punt or a turnover, lead_pos_team is the
            # end possession team, so 1 - lead_wp_before is correct, and 97.0% of the
            # 36,591 rows reaching this branch across 2022-24 are that case.
            #
            # The other 2.4% (867 rows) have lead_pos_team == start.pos_team.id -- the
            # next row belongs to the SAME possession this row started with, so
            # lead_wp_before is already in this row's perspective and flipping it
            # returns the complement. A penalty on a kickoff that forces a re-kick is
            # the clearest generator: the re-kick row carries the kicking team again,
            # and 30.6% of re-kick kickoff penalties came out flipped against 0.25% of
            # the rest. Punts (390) and missed field goals (94) dominate by count.
            #
            # Left uncorrected these are not small errors. 515 of the 867 published a
            # |WPA| above 0.5 -- wp 0.020 -> 0.982 on an unsportsmanlike-conduct flag --
            # and the subset's mean WPA sat at +0.145 where WPA should centre on zero.
            # Taking lead_wp_before unflipped leaves 6 above 0.5 and a mean of +0.011.
            .when(
                (pl.col("start.pos_team.id") != pl.col("end.pos_team.id"))
                .and_(pl.col("scoringPlay") == False)
                .and_(pl.col("lead_pos_team") == pl.col("start.pos_team.id")),
            )
            .then(pl.col(lwb))
            .when((pl.col("start.pos_team.id") != pl.col("end.pos_team.id")).and_(pl.col("scoringPlay") == False))
            .then(1 - pl.col(lwb))
            .when((pl.col("start.pos_team.id") != pl.col("end.pos_team.id")).and_(pl.col("scoringPlay") == True))
            .then(pl.col(lwb))
            .otherwise(pl.col(wa))
            .alias(wa),
        )
        .with_columns(
            # B14: the most recent play of a game IN PROGRESS. Every possession-change
            # branch above borrows the NEXT play's wp_before, and the last row of a
            # live game has no next play yet, so wp_after -- and with it wpa and the
            # home/away exports -- came out null and rendered as 0.0% on the site,
            # on exactly the row people are looking at ("4th & 1, turnover on downs:
            # WP after 0.0%, WPA 0.0%"). The game-ender branches do not apply; the
            # game is not complete.
            #
            # The raw end-state prediction is NOT usable here as it stands. Its
            # features are end-perspective except end.pos_score_diff, which is the
            # START team's, so on a possession change the model scores an
            # inconsistent row -- measured against the lead-derived value on six
            # games across 2005-2026, raw and its complement both miss by 0.15-0.79.
            # Restating the score differential as -end.pos_score_diff makes the row
            # consistent, and the complement of THAT lands within 0.001-0.03 on
            # ordinary turnovers (the residual misses are Timeouts and re-kick
            # penalties, where the lead-derived value is the questionable side).
            # Rows with no possession change keep the raw value, as .otherwise does.
            pl.when(
                pl.col(wa)
                .is_null()
                .and_(pl.col(lwb).is_null())
                .and_(pl.col("status_type_completed") != True)
                .and_(pl.col(f"_wa_flip{suffix}").is_not_null()),
            )
            .then(
                pl.when(pl.col("start.pos_team.id") != pl.col("end.pos_team.id"))
                .then(1 - pl.col(f"_wa_flip{suffix}"))
                .otherwise(pl.col(f"_wa_raw{suffix}")),
            )
            .otherwise(pl.col(wa))
            .alias(wa),
        )
        .drop([f"_wa_raw{suffix}", f"_wa_flip{suffix}"])
        .with_columns(
            (1 - pl.col(wa)).alias(dwa),
        )
        .with_columns(
            # B7: wp_after is stated in the START-possession team's perspective, so the
            # home/away split must key off start.pos_team.id. It keyed off
            # end.pos_team.id -- the same team on most plays, the OTHER team on every
            # possession change -- so home_wp_after and away_wp_after came out
            # complemented on exactly those rows.
            #
            # Measured against the identity that home WP at the end of a play equals
            # home WP at the start of the next, over published 2023-24:
            #
            #   group                       n         end-map   start-map
            #   possession unchanged        289,741    0.0061     0.0061   (same team)
            #   changed + non-scoring        24,332    0.6797     0.0099
            #   changed + scoring               654    0.6131     0.1475
            #   changed + onside kick            11    0.6824     0.1271
            #   end of half                   1,859    0.0929     0.0072
            #
            # 17,125 of the 24,332 possession-change non-scoring rows were off by more
            # than 0.5. wpa never showed it: that is wp_after minus wp_before, and both
            # sit in the same perspective, so the error cancels there and surfaces only
            # in the home/away exports.
            pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
            .then(pl.col(wa))
            .otherwise(pl.col(dwa))
            .alias(hwa),
            pl.when(pl.col("start.pos_team.id") != pl.col("homeTeamId"))
            .then(pl.col(wa))
            .otherwise(pl.col(dwa))
            .alias(awa),
        )
        .with_columns(
            (pl.col(wa) - pl.col(wb)).alias(wpa),
        )
    )


# ---------------------------------------------------------------------------
# Module-level pure-function helpers (no network, no class state)
# ---------------------------------------------------------------------------

_OVERTURNED_RE = re.compile(r"\(Original Play:.*?\)\s*$", re.IGNORECASE | re.DOTALL)


def _strip_overturned_text(text):
    """Drop the negated ``(Original Play: …)`` clause from reviewed/overturned plays.

    ESPN appends the *reversed* play description in a trailing
    ``(Original Play: …)`` parenthetical after ``CALL OVERTURNED``. Any
    fumble/recovery parsing must run on the kept (ruled) portion only, or a
    reversed fumble gets counted as a real turnover (spec finding #17).
    """
    if not text:
        return text
    return _OVERTURNED_RE.sub("", text).strip()


_RECOVERY_ABBREV_RE = re.compile(r"recovered by\s+([A-Z&]{2,})\b")

# Penalty-detail labels that indicate a DEFENSIVE foul (charged to def_pos_team).
# NOTE: "Pass Interference" is included because the upstream __setup_penalty_data
# emits the generic "Pass Interference" label for BOTH offensive and defensive PI
# (the generic branch fires before the offensive/defensive-specific branches), so
# bare "Pass Interference" cannot be assumed offensive. A later task refines penalty
# attribution by parsing the "PENALTY {TEAM_ABBR}" token from the play text.
#: Foul types that move the ball in ONE direction at least 95% of the time, measured
#: on 19,608 labelled no-play penalties across 2022-25 (label = the sign of
#: start.yardsToEndzone - end.yardsToEndzone, which on a no-play row is the
#: enforcement itself). Names that state their side are included on identity.
#:
#: Deliberately absent: bare "Pass Interference" (88/12 def -- both forms exist),
#: "Encroachment" (OFFENSIVE in NCAA usage, per the note on _DEFENSIVE_PENALTIES),
#: "Delay of Game" (89% off), "Tripping" (88% off), "Face Mask" (78/22),
#: "Holding" (80/20), "Personal Foul" and "Unsportsmanlike Conduct" (both mixed).
_PENALTY_SIGN_DEF = frozenset(
    {
        "Offside",
        "Targeting",
        "Roughing the Passer",
        "Roughing the Kicker",
        "Roughing the Holder",
        "Roughing the Snapper",
        "Substitution Infraction",
        "Neutral Zone Infraction",
        "Defensive Holding",
        "Defensive Pass Interference",
        "Defensive Offside",
        "12 Men on the Field",
    },
)
_PENALTY_SIGN_OFF = frozenset(
    {
        "False Start",
        "Illegal Formation",
        "Ineligible Downfield",
        "Illegal Shift",
        "Illegal Snap",
        "Illegal Block",
        "Illegal Motion",
        "Offensive Holding",
        "Chop Block",
        "Offensive Pass Interference",
        "Intentional Grounding",
    },
)


_DEFENSIVE_PENALTIES = frozenset(
    {
        "Defensive Holding",
        "Defensive Pass Interference",
        "Defensive Offside",
        "Roughing the Passer",
        "Roughing the Kicker",
        "Roughing the Holder",
        "Roughing the Snapper",
        "12 Men on the Field",
        "Neutral Zone Infraction",
        # NOTE: "Encroachment" is NOT here -- in NCAA usage encroachment is an
        # OFFENSIVE foul (lineman in the neutral zone), unlike the NFL's
        # defensive usage. The 2025-season taxonomy corroborates (10 OFF / 3
        # DEF among text-resolved rows).
        "Targeting",
        "Pass Interference",
    },
)


def _parse_recovery_abbrev(text):
    """Return the uppercase team abbreviation that recovered the ball, or None.

    Operates on text that has already had overturned clauses stripped.
    """
    if not text:
        return None
    m = _RECOVERY_ABBREV_RE.search(text)
    return m.group(1).upper() if m else None


def _parse_recovery_abbrevs(text):
    """Return the ordered list of uppercase team abbreviations that recovered the ball.

    A single play can contain multiple ``recovered by {TEAM}`` clauses when the ball
    changes hands more than once (e.g. offense fumbles, defense recovers and returns,
    defense fumbles, offense recovers). Each clause is one change of possession; walking
    them in order yields the possession chain used to charge a fumble-lost per change.
    Operates on text that has already had overturned clauses stripped.
    """
    if not text:
        return []
    return [m.upper() for m in _RECOVERY_ABBREV_RE.findall(text)]


#: (regex, direction). Direction controls the word-shrink order in the
#: matcher: form 1 puts the team FIRST after "PENALTY" (shrink from the left,
#: "BAYLOR Pass Interference" -> "BAYLOR"); form 2 puts the team LAST before
#: "Penalty," (shrink from the right, "for a TD Vanderbilt Penalty," ->
#: "Vanderbilt"). Form 2's inner words also admit "(" so "Miami (OH) Penalty,"
#: captures whole.
_PENALTY_TOKEN_RES = [
    (re.compile(r"PENALTY[,:]?\s+(?:on\s+)?([A-Z][\w.&'-]*(?:\s+[A-Z][\w.&'-]*){0,2})"), "prefix"),
    (re.compile(r"([A-Z(][\w.&'()-]*(?:\s+[A-Z(][\w.&'()-]*){0,2})\s+Penalty,"), "suffix"),
]

_SQUASH_RE = re.compile(r"[^A-Z0-9]")


def _squash_team(s):
    """Uppercase and strip non-alphanumerics for team-token comparison."""
    return _SQUASH_RE.sub("", s.upper()) if s else ""


def _is_char_subseq(needle: str, hay: str) -> bool:
    """True when ``needle``'s characters appear in ``hay`` in order.

    Handles ESPN's vowel-dropped / truncated team spellings ("WESTRN MICHIGAN",
    "WESTMICH" vs "Western Michigan") that neither equality nor prefix match.
    """
    it = iter(hay)
    return all(c in it for c in needle)


def _score_team_match(tok: str, cands: list) -> int:
    """0 = no match, 1 = subsequence/alias, 2 = prefix either way, 3 = exact."""
    best = 0
    for c in cands:
        if not c:
            continue
        if tok == c:
            return 3
        if c.startswith(tok) or tok.startswith(c):
            best = max(best, 2)
        elif len(tok) >= 3 and _is_char_subseq(tok, c):
            # consonant-skeleton vendor codes ("TLN" = Tulane, "MTN" = Middle
            # Tennessee); length >= 3, and the binary home-vs-away contest
            # still requires a strict winner
            best = max(best, 1)
    return best


def _team_alias_initialisms(name):
    """Derived initialism aliases for a school name (weak-tier candidates).

    Vendor texts use university-style initialisms the payload never carries:
    "UT" for Texas, "VU" for Vanderbilt, "OSU" for Ohio State. Generate both
    U+initial and initial+U for single-word names (skipping names that are
    already all-caps initialisms like "TCU" -- prefixing those invents the
    OPPONENT's alias) and word-initials / initials+U / U+initials for
    multi-word names. Matched at the weak tier so real abbreviation matches
    always win.
    """
    if not name:
        return []
    words = name.split()
    out = []
    if len(words) == 1:
        if not name.isupper():
            # both orders: "UT" (Texas), "VU" (Vanderbilt), "LU" (Liberty)
            out.append("U" + _squash_team(name)[:1])
            out.append(_squash_team(name)[:1] + "U")
    else:
        initials = "".join(w[0] for w in words if w).upper()
        out.extend([initials, initials + "U", "U" + initials])
    return out


def _team_candidates(row):
    """The home/away candidate strings a text team token is scored against.

    Shared by the penalized-team resolver and the enforcement-spot resolver so the
    two cannot drift apart on which spellings ESPN is allowed to use.
    """
    home = [
        _squash_team(row["homeTeamAbbrev"]),
        _squash_team(row["homeTeamName"]),
        _squash_team(row["homeTeamNameAlt"]),
        _squash_team((row["homeTeamName"] or "") + (row["homeTeamMascot"] or "")),
    ]
    away = [
        _squash_team(row["awayTeamAbbrev"]),
        _squash_team(row["awayTeamName"]),
        _squash_team(row["awayTeamNameAlt"]),
        _squash_team((row["awayTeamName"] or "") + (row["awayTeamMascot"] or "")),
    ]
    return home, away, _team_alias_initialisms(row["homeTeamName"]), _team_alias_initialisms(row["awayTeamName"])


def _resolve_team_side(tok, home, away, home_alias, away_alias):
    """Score one squashed team token against both sides; "home" / "away" / None.

    Attribution is a binary choice per game, so the token only has to match one
    side better than the other. A tie falls to the weak alias tier, and an
    unbroken tie returns None rather than guessing -- a wrong side is worse than
    no side, since it mirrors the resulting field position.
    """
    if len(tok) < 2:
        return None
    # "UMass" -> "MASS", "UFL" -> "FL": the university-style U prefix appears in
    # text for schools whose payload spellings never carry it.
    for t in [tok] + ([tok[1:]] if tok.startswith("U") and len(tok) >= 4 else []):
        hs = _score_team_match(t, home)
        aws = _score_team_match(t, away)
        if hs == aws:
            # weak tier: derived initialism aliases, exact-match only
            hs = int(t in home_alias)
            aws = int(t in away_alias)
        if hs > aws:
            return "home"
        if aws > hs:
            return "away"
    return None


#: The enforcement spot: "to the FlaSt 23", "to LOUISVILLE38", "to the 50 yard line".
#: The team class must exclude digits -- a greedy [A-Za-z0-9]* swallows the leading
#: digit of the yardline, turning "to the FLORIDAST13" into team "FLORIDAST1" at the
#: 3-yard line. That single character cost 85 points of agreement while developing
#: this (12.4% -> 97.3% against end.yardsToEndzone on no-play penalties).
_PENALTY_SPOT_RE = re.compile(r"(?i)\bto\s+(?:the\s+)?([A-Za-z][A-Za-z.'&-]*)\s*(\d{1,2})\b")
_PENALTY_SPOT_MID_RE = re.compile(r"(?i)\bto\s+the\s+50\s+yard\s+line\b")


def _spot_from_text(txt, row):
    """The LAST "to the <TEAM> <NN>" in ``txt``, as ``"<side>:<yardline>"`` or None.

    Shared by the penalty-enforcement and kick-return resolvers. The last mention is
    the answer in both: a play can name several spots and the final one is where the
    ball came to rest.
    """
    if not txt:
        return None
    matches = list(_PENALTY_SPOT_RE.finditer(txt))
    mid = list(_PENALTY_SPOT_MID_RE.finditer(txt))
    # Midfield competes on POSITION, not precedence. Returning "mid:50" for any
    # midfield clause ignored a later team-qualified spot, so "to the 50 yard line ...
    # to TEAM 35" resolved to the 50 rather than where the ball actually ended.
    #
    # The comparison is >= because the two patterns match the SAME text at the same
    # offset: _PENALTY_SPOT_RE's "the" is optional, so it also reads "to the 50" as
    # team "the" at yardline 50. On a tie the midfield reading is the right one -- the
    # alternative is a team token that resolves to nobody.
    if mid and (not matches or mid[-1].start() >= matches[-1].start()):
        return "mid:50"
    if not matches:
        return None
    tok_raw, yardline = matches[-1].group(1), matches[-1].group(2)
    home, away, home_alias, away_alias = _team_candidates(row)
    side = _resolve_team_side(_squash_team(tok_raw), home, away, home_alias, away_alias)
    return None if side is None else f"{side}:{int(yardline)}"


#: A kickoff/punt row whose text reports where the return finished. Both era forms
#: end the same way -- "returned by Nick Santa Cruz for 24 yards to the TTB 24."
#: (2004-2013), ", Trey Sanders return for 15 yds to the Camp 20" (2014+), and
#: "#27 J.Norman return 22 yards to the EMU22" (2025) -- so the spot regex serves
#: all three.
#: Rows that are not snaps and may sit between a play and the one that follows it.
_ADMIN_ROW_RE = r"(?i)^(?:timeout|end period|end of (?:half|game)|official)"

_RETURN_RE = re.compile(r"(?i)\breturn")
#: ...on a KICKOFF row specifically. Without this the resolver also fired on fumble
#: recoveries, whose text carries "returned to the WEST 45" and matches _RETURN_RE.
#: Two such rows changed in the backtest (401099813 p14 and p84). They may well be
#: the same defect -- both agreed with the next play, which is what the gate tests --
#: but the text-versus-field adjudication behind part (e) was measured on kickoff
#: returns only, so shipping fumble returns on it would be asserting more than was
#: checked.
#: KICKOFFS ONLY, and that scope is load bearing.
#:
#: Punt and interception returns look like candidates -- adjudicated against the
#: next real play on 2025, the text wins 93-26 on punts and 57-8 on interceptions.
#: But widening to them was backtested and REJECTED: on the 26 rows it moved, the
#: EPA distribution got worse rather than better (rows above |EPA| 4 went 1 -> 7,
#: maximum 4.22 -> 5.02), and spot checks disagree with the text as well as the
#: field -- "punt 58 yards to the LOU28 #5 C.Lacy return 21 yards to the LOU49"
#: was rewritten to 80 when the return plainly ends around 51.
#:
#: The next-play agreement that gate tests is evidently satisfiable by a value that
#: is still wrong on these classes, so they stay out until that is understood.
#: Fumble returns are out regardless: there the stored field beats the text 27-9.
_RETURN_CLASS_RE = re.compile(r"(?i)kickoff")
#: A return that reaches the endzone, or one wiped out, does not report a spot the
#: EP model should read: the scoring conventions and the nullification both put the
#: end state somewhere the text does not describe.
_RETURN_EXCLUDE_RE = re.compile(r"(?i)touchdown|nullif")


def _parse_return_spot(row):
    """Resolve where a kick return finished, as ``"<side>:<yardline>"`` or None.

    Same contract as :func:`_parse_penalty_spot`, reading ``text`` rather than
    ``penalty_text``. The LAST spot in the text is the answer -- a return can be
    followed by an enforcement that moves the ball again, and the final mention is
    where it came to rest.
    """
    txt = row["text"]
    if not txt or not _RETURN_CLASS_RE.search(txt):
        return None
    if not _RETURN_RE.search(txt) or _RETURN_EXCLUDE_RE.search(txt):
        return None
    return _spot_from_text(txt, row)


def _parse_penalty_spot(row):
    """Resolve the penalty's enforcement spot to ``"<side>:<yardline>"`` or None.

    ``side`` is "home", "away", or "mid" for the 50. The caller turns that into a
    distance to the endzone once it knows which side has the ball -- the spot alone
    is ambiguous, and reading it as a distance is the bug this exists to prevent
    (a 15-yard flag pinning a team on its own 20 published as +5.52 EPA).

    The LAST spot in the clause is the enforcement result: the richer format states
    both ("15 yards from OU 47 to NEB38"), and the from-spot is where the ball was.
    """
    return _spot_from_text(row["penalty_text"], row)


def _parse_penalty_team_side(row):
    """Resolve the PENALIZED team from play text to ``"home"`` / ``"away"`` / None.

    Attribution is a binary choice per game, so the extracted team token only
    has to match ONE side's candidate strings (abbreviation, school name, alt
    name, name+mascot) better than the other's. Ambiguous or unmatched tokens
    return None so the caller can fall back to the penalty-label heuristic
    instead of guessing wrong. Measured on the published 2025 season: 98.6%
    of penalty rows resolve, with rule-identity precision (False Start 99.0%
    offense, Roughing the Passer 99.3% defense, Defensive Offside 100%).
    """
    text = row["text"]
    if not text:
        return None
    home, away, home_alias, away_alias = _team_candidates(row)
    for rx, direction in _PENALTY_TOKEN_RES:
        for m in rx.finditer(text):
            words = m.group(1).split()
            for k in range(len(words), 0, -1):
                run = words[:k] if direction == "prefix" else words[-k:]
                tok = _squash_team(" ".join(run))
                if len(tok) < 2:
                    continue
                # also try the U-university prefix stripped ("UMass" -> "MASS")
                side = _resolve_team_side(tok, home, away, home_alias, away_alias)
                if side is not None:
                    return side
    return None


def _abbr_compat(abbr_col: pl.Expr, team_u: pl.Expr) -> pl.Expr:
    """Prefix-tolerant team-abbreviation match, as a polars expression.

    ESPN ships two abbreviation forms for some teams -- the play text uses one
    (e.g. "recovered by BUF", "caught at BUF35") while ``homeTeamAbbrev`` /
    ``awayTeamAbbrev`` carry another ("BUFF"). Treat them as the same team when
    they are equal or when either is a prefix of the other. Both operands are
    assumed already upper-cased; in a two-team game cross-opponent prefix
    collisions are effectively nonexistent.
    """
    return (abbr_col == team_u) | team_u.str.starts_with(abbr_col) | abbr_col.str.starts_with(team_u)


def _reorder_late_inserts(plays_df: pl.DataFrame) -> pl.DataFrame:
    """Move plays ESPN inserted late (2014+ feed) back to where their sequence says they belong.

    ESPN occasionally adds a play after the fact with a fresh, *later* ``id`` but the
    ``sequenceNumber`` and clock of its true position, so id order shows it 30+ rows late
    and the EP/WP lag reads a foreign play's end state (401856766 p130: EPA +7.30 for a
    7-yard run). A row is a late insert when its sequence drops below the running maximum
    AND its clock is earlier than the previous row's. It moves to just after the last
    unflagged row with a smaller sequence, and only when that neighbourhood is its own
    drive (the target row or the one after it). Measured on the published seasons: 2025
    286 better / 18 worse neighbourhoods, 2014-24 <=3 worse per season; pre-2014
    ``sequenceNumber`` is not chronological and the detector is catastrophic there, so
    the pass is gated to season >= 2014 (ledger section 3.3e).
    """
    needed = {"season", "drive.id", "start.adj_TimeSecsRem"}
    if not needed <= set(plays_df.columns) or plays_df.height < 3:
        return plays_df
    if (plays_df["season"].cast(pl.Int32, strict=False).max() or 0) < 2014:
        return plays_df
    df = plays_df.with_row_index("_pos").with_columns(
        _seq=pl.col("sequenceNumber").cast(pl.Int64, strict=False),
        _t=pl.col("start.adj_TimeSecsRem").cast(pl.Float64, strict=False),
        _drv=pl.col("drive.id").cast(pl.Utf8),
        _per=pl.col("period.number").cast(pl.Int32, strict=False),
    )
    df = df.with_columns(
        # null anywhere (first row's shift, a null sequence) means "not late", and the
        # row must stay a valid target candidate rather than vanish from both filters
        _late=(
            (pl.col("_seq") < pl.col("_seq").cum_max())
            & (pl.col("_per") < 5)
            & (pl.col("_t") > pl.col("_t").shift(1) + 5)
            & ~pl.col("type.text").str.contains("(?i)timeout|end")
        ).fill_null(False),
    )
    if not df["_late"].any():
        return plays_df
    fixed = df.filter(~pl.col("_late")).select("_pos", "_seq", "_drv")
    late = df.filter(pl.col("_late")).select("_pos", "_seq", "_drv")
    target = (
        late.join(fixed.rename({"_pos": "_tpos", "_seq": "_tseq", "_drv": "_tdrv"}), how="cross")
        .filter(pl.col("_tseq") < pl.col("_seq"))
        # the target is the row with the GREATEST sequence below this one, not the
        # last-positioned one: an unflaggable misfiled row with a low sequence can sit
        # far later in id order (401754582, seq 81 filed at Q2 0:00) and must not attract
        # the moving row.
        .sort(["_tseq", "_tpos"])
        .group_by("_pos")
        .agg(pl.col("_tpos").last(), pl.col("_tdrv").last(), pl.col("_drv").first())
        .join(
            df.select((pl.col("_pos") - 1).alias("_tpos"), pl.col("_drv").alias("_ndrv")),
            on="_tpos",
            how="left",
        )
        .filter((pl.col("_tdrv") == pl.col("_drv")) | (pl.col("_ndrv") == pl.col("_drv")))
        .select("_pos", "_tpos")
    )
    if target.height == 0:
        return plays_df
    return (
        df.join(target, on="_pos", how="left")
        .with_columns(
            _key=pl.when(pl.col("_tpos").is_not_null())
            .then(pl.col("_tpos").cast(pl.Float64) + 0.5)
            .otherwise(pl.col("_pos").cast(pl.Float64))
        )
        # Several late inserts can share one target slot (401856766: seq 91 and 92
        # both belong after seq 90); the sequence decides among them.
        .sort(["_key", "_seq"], maintain_order=True)
        .drop("_pos", "_seq", "_t", "_drv", "_per", "_late", "_tpos", "_key")
    )


def _sort_plays_ot_aware(plays_df: pl.DataFrame) -> pl.DataFrame:
    """Chronological play sort with a 2023+ ESPN overtime correction.

    Regulation plays sort by ``(id, start.adj_TimeSecsRem)``. From 2023 ESPN slots
    every overtime play into the same ``period.number`` rather than adding new
    periods, and the clock-derived ``adj_TimeSecsRem`` collapses in OT -- so OT
    plays (``period.number >= 5``) are instead ordered by ``sequenceNumber`` and
    appended after regulation. Ported from 0.36-live ``__helper_cfb_sort_plays__``
    (commit ``a3dff20``); no-op for games without OT.
    """
    plays_df = plays_df.sort(["id", "start.adj_TimeSecsRem"])
    if "period.number" not in plays_df.columns or "sequenceNumber" not in plays_df.columns:
        return plays_df
    plays_df = _reorder_late_inserts(plays_df)
    period = pl.col("period.number").cast(pl.Int32, strict=False)
    ot = plays_df.filter(period >= 5)
    if ot.height == 0:
        return plays_df
    non_ot = plays_df.filter((period < 5).or_(period.is_null()))
    ot = ot.sort(pl.col("sequenceNumber").cast(pl.Int64, strict=False))
    return pl.concat([non_ot, ot])


def _espn_num(value):
    """Best-effort numeric cast of an ESPN ``displayValue``.

    Returns ``int`` for whole numbers, ``float`` for decimals, and the original value
    unchanged when it is not purely numeric (e.g. ``'8-37'``, ``'31:24'``, ``'16/32'``).
    """
    if not isinstance(value, str):
        return value
    v = value.strip()
    if v.lstrip("-").isdigit():
        return int(v)
    try:
        f = float(v)
    except ValueError:
        return value
    # Reject non-finite floats ("inf"/"nan") so the box stays valid JSON.
    return f if (f == f and f not in (float("inf"), float("-inf"))) else value


def _parse_espn_team_box(boxscore):
    """Parse ESPN's official team box statistics into a per-team dict keyed by team id.

    ESPN's team box (``summary['boxscore']['teams']``) is the authoritative source for
    countable team totals -- turnovers, fumbles lost, interceptions, total/passing/rushing
    yards, penalties, first downs, possession time, etc. It is surfaced verbatim (numbers
    cast where clean) so downstream totals can come straight from ESPN rather than the
    lossy play-by-play derivation. Hyphenated combos are also split into integer fields:
    ``totalPenaltiesYards`` ('8-37') -> ``penalties``/``penalty_yards``; ``completionAttempts``
    ('16-32') -> ``completions``/``pass_attempts``.
    """
    out = {}
    for t in (boxscore or {}).get("teams", []) or []:
        team = t.get("team", {}) or {}
        tid = team.get("id")
        if tid is None:
            continue
        rec = {
            "team_id": int(tid),
            "abbreviation": team.get("abbreviation"),
            "display_name": team.get("displayName"),
            "home_away": t.get("homeAway"),
        }
        for st in t.get("statistics", []) or []:
            name = st.get("name")
            dv = st.get("displayValue")
            if not name:
                continue
            if name == "totalPenaltiesYards" and isinstance(dv, str) and "-" in dv:
                p, _, y = dv.partition("-")
                rec["penalties"] = _espn_num(p)
                rec["penalty_yards"] = _espn_num(y)
            elif name == "completionAttempts" and isinstance(dv, str) and "-" in dv:
                c, _, a = dv.partition("-")
                rec["completions"] = _espn_num(c)
                rec["pass_attempts"] = _espn_num(a)
            rec[name] = _espn_num(dv)
        out[int(tid)] = rec
    return out


def _parse_espn_player_box(boxscore):
    """Parse ESPN's official per-player box into a flat list of rows.

    Each row carries ``team_id`` / ``team_abbreviation`` / ``category`` / ``athlete_id`` /
    ``athlete`` plus the category's stat keys mapped to the athlete's values (e.g. passing
    ``completions/passingAttempts``, ``passingYards``, ``interceptions``; defensive
    ``sacks``, ``tacklesForLoss``, ``passesDefended``; ``fumbles`` / ``fumblesLost`` /
    ``fumblesRecovered``; ``puntReturns`` / ``kickReturns`` ...). ESPN's authoritative
    player stats with clean display names.
    """
    rows = []
    for pg in (boxscore or {}).get("players", []) or []:
        team = pg.get("team", {}) or {}
        tid = team.get("id")
        tab = team.get("abbreviation")
        for cat in pg.get("statistics", []) or []:
            cname = cat.get("name")
            keys = cat.get("keys") or []
            for a in cat.get("athletes", []) or []:
                ath = a.get("athlete", {}) or {}
                stats = a.get("stats") or []
                row = {
                    "team_id": int(tid) if tid is not None else None,
                    "team_abbreviation": tab,
                    "category": cname,
                    "athlete_id": ath.get("id"),
                    "athlete": ath.get("displayName"),
                }
                for k, v in zip(keys, stats):
                    row[k] = _espn_num(v)
                rows.append(row)
    return rows


def _fill_missing(df: "pl.DataFrame") -> "pl.DataFrame":
    """Fill nulls for aggregation: 0.0 for numerics, False for booleans.

    ``DataFrame.fill_null(0.0)`` is a SILENT NO-OP on Boolean columns -- polars
    leaves boolean nulls untouched when the fill value is a float, with no error
    and no warning. Any `.mean()` on such a column then averages over the
    non-null rows only, which for a flag that is null-where-absent means it
    averages over exactly the True rows and returns 1.0.

    That is how `rushing_power_rate` shipped as 1.0 for every team in every
    season: `power_rush_attempt` is null on the 159,513 non-power plays of 2024
    and True on 3,437, so the rate read 1.0 instead of 3437/63017 = 0.055.
    A rate pinned at exactly 1.0 is visible; the same bug on a less extreme flag
    would not be, which is why the fill is centralized here rather than patched
    at the one call site that happened to be caught.
    """
    return df.with_columns(pl.col(pl.Boolean).fill_null(False)).fill_null(0.0)


class CFBPlayProcess(object):
    """Process ESPN college-football play-by-play feeds into a tidy game-level dictionary.

    Wraps the ESPN ``playbyplay`` / ``summary`` endpoints (or a local JSON dump)
    and pipes the result through a chain of feature-engineering steps --
    down/distance, play-type flags, EPA, WPA, QBR, drive aggregation, and an
    advanced box score. Use ``run_processing_pipeline()`` for the full feature
    set or ``run_cleaning_pipeline()`` for a lighter clean.

    Example:
        End-to-end pipeline against the live ESPN endpoint::

            from sportsdataverse.cfb import CFBPlayProcess
            proc = CFBPlayProcess(gameId=401628334)
            proc.espn_cfb_pbp()
            result = proc.run_processing_pipeline()
            len(result["plays"])

        Offline replay from a JSON dump::

            proc = CFBPlayProcess(gameId=401628334, path_to_json="./pbp_dump")
            proc.cfb_pbp_disk()
            result = proc.run_processing_pipeline()
    """

    gameId = 0
    # logger = None
    ran_pipeline = False
    ran_cleaning_pipeline = False
    raw = False
    path_to_json = "/"
    return_keys = None

    def __init__(
        self,
        gameId=0,
        raw=False,
        path_to_json="/",
        return_keys=None,
        odds_override=None,
        game_roster=None,
        participants=None,
        join_participants=True,
        **kwargs,
    ):
        """CFBPlayProcess.

        Args:
            gameId: ESPN game id.
            raw: if True, espn_cfb_pbp() returns the (allowlisted) summary verbatim.
            path_to_json: directory for cfb_pbp_disk() offline loads.
            return_keys: optional subset of result keys to return.
            odds_override: optional dict {gameSpread, overUnder, homeFavorite,
                gameSpreadAvailable} that short-circuits odds resolution (sets
                odds_source="injected") so offline rebuilds never hit the live
                core-odds endpoint or fall back to defaults. Validated + coerced here.
            game_roster: optional pre-fetched game roster (the list of athlete
                records from :func:`~sportsdataverse.cfb.cfb_game_rosters.espn_cfb_game_rosters`,
                or the ``{"data": [...]}`` wrapper). Used by ``__attach_player_ids``
                to resolve a roster-backed ``{type}_player_id`` for each extracted
                ``{type}_player_name`` on games that lack a structured
                ``participants[]`` array (pre-2014). Passing it makes offline
                rebuilds fetch-free; when omitted the live path fetches the roster
                on demand only if needed.
            join_participants: when True (default) the pipeline coalesces ESPN
                per-play participant names over the regex-extracted names and
                resolves a roster-backed ``{type}_player_id`` -- both of which hit
                the network (the participants/playbyplay endpoints and the game
                roster). Set False (``CFBPlayProcess(..., join_participants=False)``)
                to skip those lookups for a ~20x faster, network-free run. EPA / WPA /
                CPOE are unaffected (the models key on game state, not player
                identity); the cost is that ``{type}_player_id`` columns go null and
                names fall back to regex-from-text instead of clean ESPN displays.

        Attributes:
            odds_source: provenance of the resolved spread —
                "summary_pickcenter" | "core_odds_api" | "default" | "injected".
        """
        if odds_override is not None:
            if not isinstance(odds_override, dict):
                raise ValueError(
                    "odds_override must be a dict with keys {gameSpread, overUnder, homeFavorite, gameSpreadAvailable}",
                )
            required = {"gameSpread", "overUnder", "homeFavorite", "gameSpreadAvailable"}
            missing = required.difference(odds_override)
            if missing:
                raise ValueError(f"odds_override is missing required keys: {sorted(missing)}")
            odds_override = {
                "gameSpread": float(odds_override["gameSpread"]),
                "overUnder": float(odds_override["overUnder"]),
                "homeFavorite": bool(odds_override["homeFavorite"]),
                "gameSpreadAvailable": bool(odds_override["gameSpreadAvailable"]),
            }
        self.gameId = int(gameId)
        # self.logger = logger
        self.ran_pipeline = False
        self.ran_cleaning_pipeline = False
        self.raw = raw
        self.path_to_json = path_to_json
        self.return_keys = return_keys
        self.odds_source = None
        self.odds_override = odds_override
        self.game_roster = game_roster
        self.participants = participants
        self.join_participants = bool(join_participants)

    def espn_cfb_pbp(self, **kwargs):
        """espn_cfb_pbp() - Pull the game by id. Data from API endpoints: `college-football/playbyplay`,
        `college-football/summary`

        Args:
            game_id (int): Unique game_id, can be obtained from cfb_schedule().
            raw (bool): If True, returns the raw json from the API endpoint. If False, returns a
            cleaned dictionary of datasets.

        Returns:
            Dict: Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts",
             "videos", "playByPlaySource", "standings", "leaders", "timeouts", "homeTeamSpread", "overUnder",
             "pickcenter", "againstTheSpread", "odds", "predictor", "winprobability", "espnWP",
             "gameInfo", "season"

        Example:
            Quick start::

                from sportsdataverse.cfb import CFBPlayProcess
                game = CFBPlayProcess(gameId=401628334)
                pbp = game.espn_cfb_pbp()
                print(list(pbp.keys()))

            Pull only the raw ESPN summary payload (skip cleaning)::

                raw_pbp = CFBPlayProcess(gameId=401628334, raw=True).espn_cfb_pbp()

            Pipeline next step (run the full processing pipeline for advanced features)::

                game = CFBPlayProcess(gameId=401628334)
                game.espn_cfb_pbp()
                processed = game.run_processing_pipeline()  # adds EPA, WPA, box score

            See Also:
                * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB PBP
                * `nflverse <https://nflverse.nflverse.com>`_ -- companion data ecosystem for the NFL
        """
        cache_buster = int(time.time() * 1000)
        pbp_txt = {"timeouts": {}}
        # summary endpoint for pickcenter array
        summary_url = f"http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event={self.gameId}&{cache_buster}"
        summary_resp = download(url=summary_url, **kwargs)
        summary = summary_resp.json()
        incoming_keys_expected = [
            "boxscore",
            "format",
            "gameInfo",
            "drives",
            "leaders",
            "broadcasts",
            "predictor",
            "pickcenter",
            "againstTheSpread",
            "odds",
            "winprobability",
            "header",
            "scoringPlays",
            "videos",
            "standings",
            "injuries",
            "gameNotes",
        ]
        dict_keys_expected = ["boxscore", "format", "gameInfo", "drives", "predictor", "header", "standings"]
        # array_keys_expected = [
        #     "leaders",
        #     "broadcasts",
        #     "pickcenter",
        #     "againstTheSpread",
        #     "odds",
        #     "winprobability",
        #     "scoringPlays",
        #     "videos",
        # ]
        if self.raw == True:
            logging.debug(f"{self.gameId}: raw cfb_pbp data requested, returning keys: {summary.keys()}")
            # reorder keys in raw format, appending empty keys which are defined later to the end
            pbp_json = {}
            for k in incoming_keys_expected:
                if k in summary.keys():
                    pbp_json[k] = summary[k]
                else:
                    pbp_json[k] = {} if k in dict_keys_expected else []
            return pbp_json

        logging.debug(f"{self.gameId}: full cfb_pbp data requested, returning keys: {summary.keys()}")
        for k in incoming_keys_expected:
            if k in summary.keys():
                pbp_txt[k] = summary[k]
            else:
                pbp_txt[k] = {} if k in dict_keys_expected else []
        for k in [
            "scoringPlays",
            "standings",
            "videos",
            "broadcasts",
            "pickcenter",
            "againstTheSpread",
            "odds",
            "predictor",
            "winprobability",
            "gameInfo",
            "leaders",
            "drives",
        ]:
            if k in summary.keys():
                pbp_txt[k] = summary[k]
            else:
                pbp_txt[k] = {} if k in dict_keys_expected else []
        for k in ["news", "shop"]:
            pbp_txt.pop(f"{k}", None)
        self.json = pbp_txt

        return self.json

    def cfb_pbp_disk(self):
        """Load a previously cached ESPN summary JSON for this game from disk.

        Reads ``{path_to_json}/{gameId}.json`` where ``path_to_json`` was passed
        to the :class:`CFBPlayProcess` constructor.

        Returns:
            dict: Parsed JSON contents, also stored on ``self.json``.

        Example:
            Quick start::

                from sportsdataverse.cfb import CFBPlayProcess
                game = CFBPlayProcess(gameId=401628334, path_to_json="./cache")
                pbp = game.cfb_pbp_disk()
                print(list(pbp.keys()))
        """
        with open(os.path.join(self.path_to_json, f"{self.gameId}.json")) as json_file:
            pbp_txt = json.load(json_file)
            self.json = pbp_txt
        return self.json

    def cfb_pbp_json(self, **kwargs):
        """Return the JSON payload currently attached to this :class:`CFBPlayProcess`
        instance.

        Returns:
            dict: The cached JSON payload (``self.json``).

        Example:
            Quick start::

                from sportsdataverse.cfb import CFBPlayProcess
                game = CFBPlayProcess(gameId=401628334)
                game.espn_cfb_pbp()
                cached = game.cfb_pbp_json()
        """
        self.json = json
        return self.json

    def __helper_cfb_pbp_drives(self, pbp_txt):
        pbp_txt, init = self.__helper_cfb_pbp(pbp_txt)

        pbp_txt["plays"] = pl.DataFrame()
        # negotiating the drive meta keys into columns after unnesting drive plays
        # concatenating the previous and current drives categories when necessary
        if (
            "drives" in pbp_txt.keys()
            and pbp_txt.get("header").get("competitions")[0].get("playByPlaySource") != "none"
        ):
            pbp_txt = self.__helper_cfb_pbp_features(pbp_txt, init)
        else:
            pbp_txt["drives"] = {}
        return pbp_txt

    def __helper_cfb_pbp_features(self, pbp_txt, init):
        pbp_txt["plays"] = pd.DataFrame()
        for key in pbp_txt.get("drives").keys():
            logging.debug(f"{self.gameId}: drives key - {key}")
            prev_drives = pd.json_normalize(
                data=pbp_txt.get("drives").get(f"{key}"),
                record_path="plays",
                meta=[
                    "id",
                    "displayResult",
                    "isScore",
                    ["team", "shortDisplayName"],
                    ["team", "displayName"],
                    ["team", "name"],
                    ["team", "abbreviation"],
                    "yards",
                    "offensivePlays",
                    "result",
                    "description",
                    "shortDisplayResult",
                    ["timeElapsed", "displayValue"],
                    ["start", "period", "number"],
                    ["start", "period", "type"],
                    ["start", "yardLine"],
                    ["start", "clock", "displayValue"],
                    ["start", "text"],
                    ["end", "period", "number"],
                    ["end", "period", "type"],
                    ["end", "yardLine"],
                    ["end", "clock", "displayValue"],
                ],
                meta_prefix="drive.",
                errors="ignore",
            )
            pbp_txt["plays"] = pd.concat([pbp_txt["plays"], prev_drives], axis=0, ignore_index=True)
        pbp_txt["plays"] = pl.from_pandas(pbp_txt["plays"])
        pbp_txt["timeouts"] = {
            init["homeTeamId"]: {"1": [], "2": []},
            init["awayTeamId"]: {"1": [], "2": []},
        }

        logging.debug(f"{self.gameId}: plays_df length - {len(pbp_txt['plays'])}")
        if len(pbp_txt["plays"]) == 0:
            return pbp_txt
        if (len(pbp_txt["plays"]) < 50) and (
            pbp_txt.get("header").get("competitions")[0].get("status").get("type").get("completed") == True
        ):
            logging.debug(f"{self.gameId}: appear to be too few plays ({len(pbp_txt['plays'])}) for a completed game")
            return pbp_txt
        if (len(pbp_txt["plays"]) > 500) and (
            pbp_txt.get("header").get("competitions")[0].get("status").get("type").get("completed") == True
        ):
            logging.debug(f"{self.gameId}: appear to be too many plays ({len(pbp_txt['plays'])}) for a completed game")
            return pbp_txt
        # Sparse old games can omit nested per-play objects entirely: pd.json_normalize
        # only emits a `start.*`/`end.*`/`period.*`/`clock.*`/`type.*` column when at least
        # one play carries that object, so a column the chain below dereferences via
        # pl.col(...) unconditionally may not exist and would raise ColumnNotFoundError at
        # plan time (before any fill_null / when-otherwise could substitute). Materialize any
        # missing column as a Null literal so the existing casts/fills downstream handle it
        # exactly as a present-but-null value. String-typed source columns are created as a
        # String-null because the chain runs `.str.*` ops on them (split/contains/to_lowercase),
        # which raise on an untyped Null column; the numeric/bool columns stay untyped Null so
        # their explicit downstream `.cast(...)` owns the final dtype.
        _required_play_cols = [
            "period.number",
            "clock.displayValue",
            "type.text",
            "id",
            "sequenceNumber",
            "text",
            "scoringPlay",
            "start.team.id",
            "start.down",
            "start.distance",
            "start.yardsToEndzone",
            "start.yardLine",
            "start.downDistanceText",
            "end.team.id",
            "end.yardLine",
        ]
        _string_play_cols = {"clock.displayValue", "type.text", "text", "start.downDistanceText"}
        _missing = [c for c in _required_play_cols if c not in pbp_txt["plays"].columns]
        if _missing:
            pbp_txt["plays"] = pbp_txt["plays"].with_columns(
                [(pl.lit(None, dtype=pl.String) if c in _string_play_cols else pl.lit(None)).alias(c) for c in _missing]
            )
        pbp_txt["plays"] = (
            pbp_txt["plays"]
            .with_columns(
                game_id=pl.lit(int(self.gameId)),
                season=pbp_txt.get("header").get("season").get("year"),
                seasonType=pbp_txt.get("header").get("season").get("type"),
                week=pbp_txt.get("header").get("week"),
                status_type_completed=pbp_txt.get("header")
                .get("competitions")[0]
                .get("status")
                .get("type")
                .get("completed"),
                homeTeamId=pl.lit(init["homeTeamId"]),
                awayTeamId=pl.lit(init["awayTeamId"]),
                homeTeamName=pl.lit(str(init["homeTeamName"])),
                awayTeamName=pl.lit(str(init["awayTeamName"])),
                homeTeamMascot=pl.lit(str(init["homeTeamMascot"])),
                awayTeamMascot=pl.lit(str(init["awayTeamMascot"])),
                homeTeamAbbrev=pl.lit(str(init["homeTeamAbbrev"])),
                awayTeamAbbrev=pl.lit(str(init["awayTeamAbbrev"])),
                homeTeamNameAlt=pl.lit(str(init["homeTeamNameAlt"])),
                awayTeamNameAlt=pl.lit(str(init["awayTeamNameAlt"])),
                gameSpread=pl.lit(float(np.asarray(init["gameSpread"]).item())).abs(),
                homeFavorite=pl.lit(bool(np.asarray(init["homeFavorite"]).item())),
                gameSpreadAvailable=pl.lit(init["gameSpreadAvailable"]),
                overUnder=pl.lit(float(np.asarray(init["overUnder"]).item())),
            )
            .with_columns(
                homeTeamSpread=pl.when(pl.col("homeFavorite") == True)
                .then(pl.col("gameSpread"))
                .otherwise(-1 * pl.col("gameSpread")),
            )
            .with_columns(
                pl.col("period.number").cast(pl.Int32),
                # Clock is always "MM:SS" → exactly 2 fields. Polars 1.x deprecated
                # `n_field_strategy` (it has no effect when `upper_bound` is set);
                # `upper_bound=2` alone is the modern, warning-free signature.
                pl.col("clock.displayValue").str.split(":").list.to_struct(upper_bound=2).alias("clock.mm"),
            )
            .with_columns(pl.col("clock.mm").struct.rename_fields(["clock.minutes", "clock.seconds"]))
            .unnest("clock.mm")
            .with_columns(
                pl.col("clock.minutes").cast(pl.Int32),
                pl.col("clock.seconds").cast(pl.Int32),
                half=pl.when(pl.col("period.number") <= 2).then(1).otherwise(2),
            )
            .with_columns(lag_half=pl.col("half").shift(1), lead_half=pl.col("half").shift(-1))
            .with_columns(
                pl.when(pl.col("period.number").is_in([1, 3]))
                .then(900 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
                .otherwise(60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
                .alias("start.TimeSecsRem"),
                pl.when(pl.col("period.number") == 1)
                .then(2700 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
                .when(pl.col("period.number") == 2)
                .then(1800 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
                .when(pl.col("period.number") == 3)
                .then(900 + 60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
                .otherwise(60 * pl.col("clock.minutes") + pl.col("clock.seconds"))
                .alias("start.adj_TimeSecsRem"),
                pl.col("id").cast(pl.Int64),
                pl.col("sequenceNumber").cast(pl.Int32),
            )
        )
        pbp_txt["plays"] = _sort_plays_ot_aware(pbp_txt["plays"])

        # drop play text dupes intelligently, even if they have different play_id values
        pbp_txt["plays"] = (
            pbp_txt["plays"]
            .with_columns(
                pl.col("text").cast(str),
                orig_play_type=pl.col("type.text"),
                lead_text=pl.col("text").shift(-1),
                lead_start_team=pl.col("start.team.id").shift(-1),
                lead_start_yardsToEndzone=pl.col("start.yardsToEndzone").shift(-1),
                lead_start_down=pl.col("start.down").shift(-1),
                lead_start_distance=pl.col("start.distance").shift(-1),
                lead_scoringPlay=pl.col("scoringPlay").shift(-1),
                text_dupe=pl.lit(False),
            )
            .with_columns(
                text_dupe=pl.when(
                    (pl.col("start.team.id") == pl.col("lead_start_team"))
                    .and_(pl.col("start.down") == pl.col("lead_start_down"))
                    .and_(pl.col("start.yardsToEndzone") == pl.col("lead_start_yardsToEndzone"))
                    .and_(pl.col("start.distance") == pl.col("lead_start_distance"))
                    .and_(pl.col("text") == pl.col("lead_text"))
                    .and_(pl.col("type.text") != "Timeout"),
                )
                .then(pl.lit(True))
                .when(
                    (pl.col("start.team.id") == pl.col("lead_start_team"))
                    .and_(pl.col("start.down") == pl.col("lead_start_down"))
                    .and_(pl.col("start.yardsToEndzone") == pl.col("lead_start_yardsToEndzone"))
                    .and_(pl.col("start.distance") == pl.col("lead_start_distance"))
                    .and_(pl.col("text").is_in(pl.col("lead_text").implode()))
                    .and_(pl.col("type.text") != "Timeout")
                    # Guard: an "End of <period/half/game>" marker inherits the preceding
                    # play's start state (team/down/distance/yardsToEndzone), so without
                    # this the loose is_in(lead_text) match spuriously flags the real
                    # play right before it (e.g. an end-of-half Hail Mary interception)
                    # as a duplicate and drops it. Never dedupe against an end-marker lead.
                    .and_(pl.col("lead_text").str.contains(r"(?i)end of|end period|end quarter") == False),
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False)),
            )
        )
        # The dupe rows are removed HERE, so the text_dupe column that reaches
        # the output is always False by construction -- it is the residue of a
        # filter that already ran, not a marker consumers can use to dedupe.
        # Rows with identical text but DIFFERENT start states (e.g. repeated
        # degenerate feed texts advancing the yardline) are deliberately kept:
        # they are distinct plays with bad text, not duplicates.
        pbp_txt["plays"] = pbp_txt["plays"].filter(pl.col("text_dupe") == False)
        pbp_txt["plays"] = pbp_txt["plays"].with_row_index("game_play_number", 1)
        pbp_txt["plays"] = (
            pbp_txt["plays"]
            .with_columns(
                pl.col("start.team.id").fill_null(strategy="forward").fill_null(strategy="backward").cast(pl.Int32),
            )
            # B3 (0.36-live 1bb28fe): capture the end-state-missing flag BEFORE filling,
            # then fill end.team.id from the NEXT play's start team (the team that took
            # over) before falling back to the current play's start team. ESPN drops the
            # end-state on some short-yardage / penalty plays; the bare current-play
            # fallback mis-attributes possession after a turnover/score, whereas the next
            # play's start team is the correct post-play possessor.
            .with_columns(
                # B13: a second shape of missing end state. Besides the null team and
                # the -1 sentinel, the 2025 vendor template ships an ALL-ZERO end --
                # yardLine 0, yardsToEndzone 0, down 0, distance 0, team present -- on
                # ordinary scrimmage plays: "R.Sharpe rush left for 41 yards gain to
                # the MSU20, 1ST DOWN". Nothing flagged it, the <= 0 clamp read the 0
                # as the goal line and stored 99, and a 41-yard run scored -2.52 EPA.
                # Published rows ending at 99 with down 0 that are not a score, a
                # turnover or an end of half: 88 in 2022, 6 in 2024, 103 in 2025,
                # averaging -1.6 to -3.9 EPA. Down 0 AND distance 0 never describe a
                # real snap's end; a score, a turnover and a kick each leave a real
                # state behind, and are excluded besides. Flagging it here lets part
                # (b) backfill from the next play like any other missing end.
                end_state_missing=pl.col("end.team.id").is_null()
                | (
                    (pl.col("end.down") == 0)
                    .and_(pl.col("end.distance") == 0)
                    .and_(pl.col("end.yardLine") == 0)
                    .and_(pl.col("end.yardsToEndzone") == 0)
                    .and_(pl.col("scoringPlay") == False)
                    # end.team.id is still a string here; start.team.id is not
                    .and_(pl.col("end.team.id").cast(pl.Utf8) == pl.col("start.team.id").cast(pl.Utf8))
                    .and_(
                        pl.col("type.text").str.contains(r"(?i)kickoff|punt|field goal|timeout|end period|end of")
                        == False
                    )
                ),
            )
            .with_columns(
                pl.col("end.team.id")
                .fill_null(value=pl.col("start.team.id").shift(-1))
                .fill_null(value=pl.col("start.team.id"))
                .cast(pl.Int32),
            )
            .with_columns(
                pl.col("start.team.id").cast(pl.Int32),
                pl.col("end.team.id").cast(pl.Int32),
                pl.col("homeTeamId").cast(pl.Int32),
                pl.col("awayTeamId").cast(pl.Int32),
                pl.when(pl.col("type.text").is_in(kickoff_vec).and_(pl.col("start.team.id") == init["homeTeamId"]))
                .then(pl.col("awayTeamId"))
                .when(pl.col("type.text").is_in(kickoff_vec).and_(pl.col("start.team.id") == init["awayTeamId"]))
                .then(pl.col("homeTeamId"))
                .otherwise(pl.col("start.team.id"))
                .alias("start.pos_team.id"),
            )
            .with_columns(
                pl.when(pl.col("start.pos_team.id") == init["homeTeamId"])
                .then(init["awayTeamId"])
                .otherwise(init["homeTeamId"])
                .alias("start.def_pos_team.id"),
                pl.when(pl.col("end.team.id") == init["homeTeamId"])
                .then(init["awayTeamId"])
                .otherwise(init["homeTeamId"])
                .alias("end.def_pos_team.id"),
                pl.col("end.team.id").alias("end.pos_team.id"),
            )
            .with_columns(
                pl.when(pl.col("start.pos_team.id") == init["homeTeamId"])
                .then(pl.col("homeTeamName"))
                .otherwise(pl.col("awayTeamName"))
                .alias("start.pos_team.name"),
                pl.when(pl.col("start.pos_team.id") == init["homeTeamId"])
                .then(pl.col("awayTeamName"))
                .otherwise(pl.col("homeTeamName"))
                .alias("start.def_pos_team.name"),
                pl.when(pl.col("end.pos_team.id") == init["homeTeamId"])
                .then(pl.col("homeTeamName"))
                .otherwise(pl.col("awayTeamName"))
                .alias("end.pos_team.name"),
                pl.when(pl.col("end.pos_team.id") == init["homeTeamId"])
                .then(pl.col("awayTeamName"))
                .otherwise(pl.col("homeTeamName"))
                .alias("end.def_pos_team.name"),
                pl.when(pl.col("start.pos_team.id") == init["homeTeamId"])
                .then(True)
                .otherwise(False)
                .alias("start.is_home"),
                pl.when(pl.col("end.pos_team.id") == init["homeTeamId"])
                .then(True)
                .otherwise(False)
                .alias("end.is_home"),
                pl.when(
                    (pl.col("type.text") == "Timeout").and_(
                        pl.col("text")
                        .str.to_lowercase()
                        .str.contains(str(init["homeTeamAbbrev"]).lower())
                        .or_(
                            pl.col("text").str.to_lowercase().str.contains(str(init["homeTeamAbbrev"]).lower()),
                            pl.col("text").str.to_lowercase().str.contains(str(init["homeTeamName"]).lower()),
                            pl.col("text").str.to_lowercase().str.contains(str(init["homeTeamMascot"]).lower()),
                            pl.col("text").str.to_lowercase().str.contains(str(init["homeTeamNameAlt"]).lower()),
                        ),
                    ),
                )
                .then(True)
                .otherwise(False)
                .alias("homeTimeoutCalled"),
                pl.when(
                    (pl.col("type.text") == "Timeout").and_(
                        pl.col("text")
                        .str.to_lowercase()
                        .str.contains(str(init["awayTeamAbbrev"]).lower())
                        .or_(
                            pl.col("text").str.to_lowercase().str.contains(str(init["awayTeamAbbrev"]).lower()),
                            pl.col("text").str.to_lowercase().str.contains(str(init["awayTeamName"]).lower()),
                            pl.col("text").str.to_lowercase().str.contains(str(init["awayTeamMascot"]).lower()),
                            pl.col("text").str.to_lowercase().str.contains(str(init["awayTeamNameAlt"]).lower()),
                        ),
                    ),
                )
                .then(True)
                .otherwise(False)
                .alias("awayTimeoutCalled"),
            )
        )

        pbp_txt["timeouts"][init["homeTeamId"]]["1"] = (
            pbp_txt["plays"]
            .filter((pl.col("homeTimeoutCalled") == True).and_(pl.col("period.number") <= 2))
            .get_column("id")
            .to_list()
        )
        pbp_txt["timeouts"][init["homeTeamId"]]["2"] = (
            pbp_txt["plays"]
            .filter((pl.col("homeTimeoutCalled") == True).and_(pl.col("period.number") > 2))
            .get_column("id")
            .to_list()
        )
        pbp_txt["timeouts"][init["awayTeamId"]]["1"] = (
            pbp_txt["plays"]
            .filter((pl.col("awayTimeoutCalled") == True).and_(pl.col("period.number") <= 2))
            .get_column("id")
            .to_list()
        )
        pbp_txt["timeouts"][init["awayTeamId"]]["2"] = (
            pbp_txt["plays"]
            .filter((pl.col("awayTimeoutCalled") == True).and_(pl.col("period.number") > 2))
            .get_column("id")
            .to_list()
        )
        # end_timeouts = pbp_txt["plays"].select(
        #     (
        #         3
        #         - pl.struct(["id", "period.number"]).apply(
        #             lambda x: (
        #                 sum(
        #                     (i <= x.struct.field("id")) & (x.struct.field("period.number") <= 2)
        #                     for i in pbp_txt["timeouts"][int(init["homeTeamId"])]["1"]
        #                 )
        #             )
        #             | (
        #                 sum(
        #                     (i <= x.struct.field("id")) & (x.struct.field("period.number") > 2)
        #                     for i in pbp_txt["timeouts"][int(init["homeTeamId"])]["2"]
        #                 )
        #             ),
        #             return_dtype=pl.Int64,
        #         )
        #     ).alias("end.homeTeamTimeouts"),
        #     (
        #         3
        #         - pl.struct(["id", "period.number"]).apply(
        #             lambda x: (
        #                 sum(
        #                     (i <= x.struct.field("id")) & (x.struct.field("period.number") <= 2)
        #                     for i in pbp_txt["timeouts"][int(init["awayTeamId"])]["1"]
        #                 )
        #             )
        #             | (
        #                 sum(
        #                     (i <= x.struct.field("id")) & (x.struct.field("period.number") > 2)
        #                     for i in pbp_txt["timeouts"][int(init["awayTeamId"])]["2"]
        #                 )
        #             ),
        #             return_dtype=pl.Int64,
        #         )
        #     ).alias("end.awayTeamTimeouts"),
        # )
        # pbp_txt["plays"] = pbp_txt["plays"].join(end_timeouts, on=["id", "period.number"], how="left")
        pbp_txt["plays"] = (
            pbp_txt["plays"]
            .with_columns(
                (
                    3
                    - pl.struct("id", "period.number").map_elements(
                        lambda x: (
                            (
                                sum(
                                    (i <= x["id"]) & (x["period.number"] <= 2)
                                    for i in pbp_txt["timeouts"][int(init["homeTeamId"])]["1"]
                                )
                            )
                            | (
                                sum(
                                    (i <= x["id"]) & (x["period.number"] > 2)
                                    for i in pbp_txt["timeouts"][int(init["homeTeamId"])]["2"]
                                )
                            )
                        ),
                        return_dtype=pl.Int64,
                    )
                ).alias("end.homeTeamTimeouts"),
                (
                    3
                    - pl.struct("id", "period.number").map_elements(
                        lambda x: (
                            (
                                sum(
                                    (i <= x["id"]) & (x["period.number"] <= 2)
                                    for i in pbp_txt["timeouts"][int(init["awayTeamId"])]["1"]
                                )
                            )
                            | (
                                sum(
                                    (i <= x["id"]) & (x["period.number"] > 2)
                                    for i in pbp_txt["timeouts"][int(init["awayTeamId"])]["2"]
                                )
                            )
                        ),
                        return_dtype=pl.Int64,
                    )
                ).alias("end.awayTeamTimeouts"),
            )
            .with_columns(
                pl.col("end.homeTeamTimeouts").shift(n=1, fill_value=3).alias("start.homeTeamTimeouts"),
                pl.col("end.awayTeamTimeouts").shift(n=1, fill_value=3).alias("start.awayTeamTimeouts"),
                pl.col("start.TimeSecsRem").shift(n=1).alias("end.TimeSecsRem"),
                pl.col("start.adj_TimeSecsRem").shift(n=1).alias("end.adj_TimeSecsRem"),
            )
            .with_columns(
                pl.when(pl.col("game_play_number") == 1)
                .then(pl.lit(1800))
                .when((pl.col("half") == 2) & (pl.col("lag_half") == 1))
                .then(pl.lit(1800))
                .otherwise(pl.col("end.TimeSecsRem"))
                .alias("end.TimeSecsRem"),
                pl.when(pl.col("game_play_number") == 1)
                .then(pl.lit(3600))
                .when((pl.col("half") == 2) & (pl.col("lag_half") == 1))
                .then(pl.lit(1800))
                .otherwise(pl.col("end.adj_TimeSecsRem"))
                .alias("end.adj_TimeSecsRem"),
                pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("start.homeTeamTimeouts"))
                .otherwise(pl.col("start.awayTeamTimeouts"))
                .alias("start.posTeamTimeouts"),
                pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("start.awayTeamTimeouts"))
                .otherwise(pl.col("start.homeTeamTimeouts"))
                .alias("start.defPosTeamTimeouts"),
                pl.when(pl.col("end.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("end.homeTeamTimeouts"))
                .otherwise(pl.col("end.awayTeamTimeouts"))
                .alias("end.posTeamTimeouts"),
                pl.when(pl.col("end.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("end.awayTeamTimeouts"))
                .otherwise(pl.col("end.homeTeamTimeouts"))
                .alias("end.defPosTeamTimeouts"),
                pl.when(
                    (pl.col("game_play_number") == 1).and_(
                        pl.col("type.text").is_in(kickoff_vec),
                        pl.col("start.pos_team.id") == pl.col("homeTeamId"),
                    ),
                )
                .then(pl.col("homeTeamId"))
                .otherwise(pl.col("awayTeamId"))
                .alias("firstHalfKickoffTeamId"),
                pl.col("period.number").alias("period"),
                pl.when(pl.col("start.team.id") == pl.col("homeTeamId"))
                .then(pl.lit(100) - pl.col("start.yardLine"))
                .otherwise(pl.col("start.yardLine"))
                .alias("start.yard"),
            )
            .with_columns(
                pl.when(pl.col("start.yardLine").is_null() == False)
                .then(pl.col("start.yardLine"))
                .otherwise(pl.col("start.yard"))
                .alias("start.yardLine"),
            )
            .with_columns(
                pl.when(pl.col("start.yardLine").is_null() == False)
                .then(pl.col("start.yardsToEndzone"))
                .otherwise(pl.col("start.yardLine"))
                .alias("start.yardsToEndzone"),
            )
            .with_columns(
                pl.when(pl.col("start.yardsToEndzone") == 0)
                .then(pl.col("start.yard"))
                .otherwise(pl.col("start.yardsToEndzone"))
                .alias("start.yardsToEndzone"),
                pl.when(pl.col("end.team.id") == pl.col("homeTeamId"))
                .then(pl.lit(100) - pl.col("end.yardLine"))
                .otherwise(pl.col("end.yardLine"))
                .alias("end.yard"),
            )
            .with_columns(
                pl.when((pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains(r"(?i)declined")))
                .then(pl.col("start.yard"))
                .otherwise(pl.col("end.yard"))
                .alias("end.yard"),
            )
            .with_columns(
                # ESPN uses -1 as a MISSING marker for end.yardsToEndzone, not as a
                # yardline. Guarding only on `end.yardLine is not null` let that
                # sentinel straight through, because ESPN populates end.yardLine
                # perfectly well on exactly those plays -- the fallback below never
                # fired for the case it exists to handle.
                #
                # 2016 week 2 shipped that way: 72 of 75 games carry -1 on ~every
                # play (SMU @ Baylor: 238 of 238), while start.yardsToEndzone has 81
                # distinct values and no sentinel, and end.yardLine has 83 distinct
                # values and no nulls. Downstream that produced end.yardsToEndzone
                # ~99, so EP_end was scored as if the offense were on its own 1-yard
                # line after every snap and EPA ran about -2.6/play. It reached the
                # published percentiles as a 1st-percentile early-down EPA of -2.97,
                # roughly six times every neighbouring season.
                #
                # A re-scrape does NOT fix this -- the committed raw is byte-identical
                # to what ESPN serves today. The recovery is local: `end.yard` is the
                # possession-adjusted yardline this branch already falls back to.
                pl.when((pl.col("end.yardLine").is_null() == False).and_(pl.col("end.yardsToEndzone") >= 0))
                .then(pl.col("end.yardsToEndzone"))
                .otherwise(pl.col("end.yard"))
                .alias("end.yardsToEndzone"),
                pl.when(
                    (pl.col("start.distance") == 0).and_(pl.col("start.downDistanceText").str.contains(r"(?i)goal")),
                )
                .then(pl.col("start.yardsToEndzone"))
                .otherwise(pl.col("start.distance"))
                .alias("start.distance"),
            )
            .with_columns(
                pl.when((pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains(r"(?i)declined")))
                .then(pl.col("start.yardsToEndzone"))
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
            )
            .with_columns(
                # B3 part (b) (0.36-live dc4224b): for end-state-missing plays whose NEXT
                # play is the same possessing team, backfill end.yardsToEndzone from that
                # next play's start yardline -- the actual resulting field position.
                # Pairs with the end.team.id next-play fill above.
                # The next REAL play, not the next row. A Timeout between this play and
                # the next snap carries stale state on the 2025 template -- "R.Sharpe
                # rush for 41 yards to the MSU20" was backfilled with 61, the Timeout
                # row's start, when the ensuing snap starts at 20. Same lookahead part
                # (d.ii) and part (e) use.
                pl.when(
                    (pl.col("end_state_missing") == True).and_(
                        pl.when(pl.col("type.text").shift(-1).str.contains(_ADMIN_ROW_RE))
                        .then(
                            pl.when(pl.col("type.text").shift(-2).str.contains(_ADMIN_ROW_RE))
                            .then(pl.col("start.pos_team.id").shift(-3))
                            .otherwise(pl.col("start.pos_team.id").shift(-2)),
                        )
                        .otherwise(pl.col("start.pos_team.id").shift(-1))
                        == pl.col("end.pos_team.id"),
                    ),
                )
                .then(
                    pl.when(pl.col("type.text").shift(-1).str.contains(_ADMIN_ROW_RE))
                    .then(
                        pl.when(pl.col("type.text").shift(-2).str.contains(_ADMIN_ROW_RE))
                        .then(pl.col("start.yardsToEndzone").shift(-3))
                        .otherwise(pl.col("start.yardsToEndzone").shift(-2)),
                    )
                    .otherwise(pl.col("start.yardsToEndzone").shift(-1)),
                )
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
            )
            .with_columns(
                # The all-zero shape (B13) zeroed the down and distance too, and the EP
                # model reads its end-state down flags from end.down: with all four
                # False it scored the 41-yard run's end at 3.53 where the identical
                # state at the next snap scored 4.72. Backfill both from the next real
                # play so the row's end IS the next row's start.
                _b_zero=(pl.col("end_state_missing") == True)
                .and_(pl.col("end.down") == 0)
                .and_(pl.col("end.distance") == 0),
                _b_nxt_down=pl.when(pl.col("type.text").shift(-1).str.contains(_ADMIN_ROW_RE))
                .then(
                    pl.when(pl.col("type.text").shift(-2).str.contains(_ADMIN_ROW_RE))
                    .then(pl.col("start.down").shift(-3))
                    .otherwise(pl.col("start.down").shift(-2)),
                )
                .otherwise(pl.col("start.down").shift(-1)),
                _b_nxt_dist=pl.when(pl.col("type.text").shift(-1).str.contains(_ADMIN_ROW_RE))
                .then(
                    pl.when(pl.col("type.text").shift(-2).str.contains(_ADMIN_ROW_RE))
                    .then(pl.col("start.distance").shift(-3))
                    .otherwise(pl.col("start.distance").shift(-2)),
                )
                .otherwise(pl.col("start.distance").shift(-1)),
            )
            .with_columns(
                pl.when(pl.col("_b_zero").and_(pl.col("_b_nxt_down").is_not_null()))
                .then(pl.col("_b_nxt_down"))
                .otherwise(pl.col("end.down"))
                .alias("end.down"),
                pl.when(pl.col("_b_zero").and_(pl.col("_b_nxt_dist").is_not_null()))
                .then(pl.col("_b_nxt_dist"))
                .otherwise(pl.col("end.distance"))
                .alias("end.distance"),
            )
            .drop(["_b_zero", "_b_nxt_down", "_b_nxt_dist"])
            .with_columns(
                # B3 part (c): the same repair for end states that are PRESENT but
                # mirrored. ESPN sometimes reports end.yardsToEndzone from the wrong
                # side of the field while its own end.yardLine and the next play's
                # start agree with each other -- e.g. game 401868040, "pass complete
                # ... for 34 yards to the VIL05" carries end.yardLine=5 with
                # end.yardsToEndzone=95. Part (b) cannot catch it because the end
                # state is not missing, and there is no same-row test that can:
                # yardsToEndzone equals yardLine on roughly half of plays and its
                # complement on the other half, depending which side of the fifty the
                # ball is on, so the pair alone proves nothing.
                #
                # Fed to the EP model, 95 scores the offense as if it were backed up
                # on its own 5 instead of at the goal line. That one input cost the
                # play about 6 points of EP, and the following play -- a penalty,
                # whose EPA deliberately folds in EP_between, the discontinuity since
                # the previous play ended -- inherited the whole error as a +6.45 EPA.
                #
                # The trigger is deliberately narrow. Possession must be unchanged
                # across the pair, so the two values describe the same ball; the value
                # must be the EXACT complement of the next start, which is the flip
                # signature (375 pairs in the captured corpus disagree at all, only 54
                # are mirrored); and the play must not be a kick or carry a penalty of
                # its own, since both legitimately move the ball between snaps. A
                # penalty on the NEXT play is fine -- it is enforced from that play's
                # own result and cannot move its starting spot.
                pl.when(
                    (pl.col("end.yardsToEndzone").is_null() == False)
                    .and_(pl.col("start.yardsToEndzone").shift(-1).is_null() == False)
                    .and_(pl.col("start.pos_team.id").shift(-1) == pl.col("end.pos_team.id"))
                    .and_(pl.col("start.pos_team.id") == pl.col("end.pos_team.id"))
                    .and_(pl.col("type.text").is_in(kickoff_vec) == False)
                    .and_(
                        # Era vocabulary again: "field goal" is the 2004-2013 spelling and
                        # modern text writes "23 yd FG GOOD", while the extra point moved
                        # from its own "extra point" wording into the scoring play's
                        # trailing "(Kicker Name Kick)". Without the modern forms this
                        # exclusion let part (c) rewrite the end state of made field goals
                        # and PATs -- 4 rows across 2014-2022, none before 2014.
                        pl.col("text").str.contains(
                            r"(?i)kickoff|punt|field goal|extra point|touchback|kick attempt"
                            r"|\bFG\b|\bPAT\b|\(.*\bkick\b.*\)"
                        )
                        == False
                    )
                    # Part (c) once excluded rows carrying a penalty, on the reasoning
                    # that a flag legitimately moves the ball between snaps. That
                    # reasoning guards a DISAGREEMENT between end and next start; it
                    # does not touch the EXACT-COMPLEMENT test, which with possession
                    # unchanged is the mirror signature whether or not a flag was
                    # thrown -- end and next start describe the same ball either way.
                    # Measured with possession continuing into the next real play:
                    # 2024 has 1 such scrimmage-penalty row, 2025 has 229 of 2,045
                    # (11.2%), and the 362-game validation surfaced them as
                    # counterfactual sign disagreements ("Personal Foul 11 yard from
                    # NEV21 to NEV1" stored as 90). Kicks stay excluded; their end
                    # states are parts (d) and (e).
                    .and_(pl.col("end.yardsToEndzone") != pl.col("start.yardsToEndzone").shift(-1))
                    .and_(
                        pl.col("end.yardsToEndzone") == (pl.lit(100) - pl.col("start.yardsToEndzone").shift(-1)),
                    ),
                )
                .then(pl.col("start.yardsToEndzone").shift(-1))
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
            )
            .with_columns(
                # B3 part (d): a kickoff row that carries a penalty but describes no kick
                # outcome at all -- no yardage, no return, no touchback, no recovery --
                # established nobody's field position. ESPN writes the penalty's
                # ENFORCEMENT spot into end.yardsToEndzone on those rows: the spot the
                # kick will be taken from, in the KICKING team's own territory, while
                # pos_team is the receiving team. The EP model reads it as the receiving
                # team's distance to its target endzone. 83 such rows in 2022-24.
                #
                # "Tristan Mattson kickoff ARKANSAS ST Penalty, Unsportsmanlike Conduct
                # to the ArkSt 20" carried end.yardsToEndzone=20 with EP -2.21 -> +3.31,
                # publishing a 15-yard flag AGAINST the receiving team as +5.52 EPA FOR
                # it. Milder but commoner is "to the 50 yard line" holding 50, which
                # looks correct only because 50 is its own complement.
                #
                # The no-kick-described gate is load bearing. Five kickoff penalties end
                # inside the 25 legitimately, and all five carry a "return for N yds"
                # clause where the return really finished there -- a 90-yard return to
                # the Rice 5 earns its +5.91 and keeps it.
                #
                # Flag whether the ball is re-kicked. The lookahead skips one
                # administrative row, since a Timeout routinely sits between the flag and
                # the re-kick (401636889: flag p52, timeout p53, re-kick p54).
                _ko_pen_rekick_follows=(
                    (pl.col("type.text").shift(-1).is_in(kickoff_vec)).or_(
                        pl.col("type.text")
                        .shift(-1)
                        .str.contains(_ADMIN_ROW_RE)
                        .and_(pl.col("type.text").shift(-2).is_in(kickoff_vec)),
                    )
                ).fill_null(False),
                # (d.ii) needs the next REAL play, not the next row. (d.i) already
                # skipped administrative rows and (d.ii) did not, so a Timeout between
                # the flag and the ensuing snap made it read the Timeout's own start
                # yardline as the receiving team's field position.
                _pen_nxt_start=pl.when(pl.col("type.text").shift(-1).str.contains(_ADMIN_ROW_RE))
                .then(
                    pl.when(pl.col("type.text").shift(-2).str.contains(_ADMIN_ROW_RE))
                    .then(pl.col("start.yardsToEndzone").shift(-3))
                    .otherwise(pl.col("start.yardsToEndzone").shift(-2)),
                )
                .otherwise(pl.col("start.yardsToEndzone").shift(-1)),
                _pen_nxt_pos=pl.when(pl.col("type.text").shift(-1).str.contains(_ADMIN_ROW_RE))
                .then(
                    pl.when(pl.col("type.text").shift(-2).str.contains(_ADMIN_ROW_RE))
                    .then(pl.col("start.pos_team.id").shift(-3))
                    .otherwise(pl.col("start.pos_team.id").shift(-2)),
                )
                .otherwise(pl.col("start.pos_team.id").shift(-1)),
            )
            .with_columns(
                # (d.i) A re-kick follows, so this row is a NO-PLAY: the ensuing kickoff
                # row scores that possession in full, and anything credited here is
                # counted twice. EP_start on a kickoff is already the touchback EP, so
                # setting the end state to the same touchback yardline drives EPA to ~0.
                # start.yardsToEndzone would NOT: the real start (own 35, y2ez 65) is
                # better field position than a touchback and leaves a spurious ~+1.2.
                # 42 of the 83 rows, currently averaging +0.78 EPA and reaching +3.92.
                #
                # (d.ii) No re-kick follows, so the receiving team really did take over
                # and the first real play after this row is the only record of where.
                # The next play is trustworthy here precisely because this row describes
                # nothing: across the 22 such rows whose next play keeps the same
                # possession the two disagree 17 times, by a mean of 14 yards, and every
                # disagreement checked resolves in the next play's favour ("kickoff
                # MURRAY ST Penalty, Delay Of Game to the MurrS 30" holds 30 while the
                # next snap starts at 75, Texas Tech on their own 25). This does NOT
                # generalise to kickoff penalties whose text DOES describe a kick --
                # tested and rejected, it moves 256 of 843 rows and overwrites correct
                # values with the next row's inherited error.
                pl.when(
                    (pl.col("type.text").is_in(kickoff_vec).or_(pl.col("text").str.contains(r"(?i)kickoff")))
                    .and_(pl.col("text").str.contains(r"(?i)penalty"))
                    .and_(
                        pl.col("text").str.contains(
                            # Era vocabulary: ESPN wrote "for N yards" through 2013 and
                            # "for N yds" from 2014. The changeover is total -- 98% one
                            # side, 0% the other, no overlap season -- so matching only
                            # "yds" made this gate fire on 108 pre-2014 rows that DO
                            # describe a kick ("kickoff for 50 yards out-of-bounds"),
                            # rewriting an end state that was never in question.
                            # Out-of-bounds is hyphenated in the old texts, spaced in new.
                            # 2025 adds a THIRD kickoff vocabulary. Alongside "for N yards"
                            # (2004-2013) and "for N yds" (2014+), a vendor feed writes
                            # "(15:00) #36 T.Morrison kickoff 65 yards to the SAC00" -- no
                            # "for" at all, and only 57% of 2025 kickoff rows carry the
                            # 2014 form. Without it this gate called those rows "no kick
                            # described": 12 in 2025, 5 in 2023, 1 in 2022.
                            #
                            # The distance is anchored on the word kickoff rather than
                            # matched loosely, because a bare "N yards" also appears as the
                            # PENALTY yardage on exactly the rows part (d) exists to repair
                            # ("kickoff UTEP Penalty, Targeting ... (-15 Yards) to the UTEP
                            # 20"). Anchoring cures all 18 false fires and over-excludes
                            # nothing, in any season from 2005 to 2025.
                            r"(?i)kick(?:off)?\s+(?:for\s+)?-?\d+\s*(?:yds|yards)"
                            r"|for \d+ (?:yds|yards)|touchback|return|out.of.bounds|recovered|downed|fair catch",
                        )
                        == False
                    )
                    .and_(pl.col("_ko_pen_rekick_follows") == True),
                )
                .then(pl.when(pl.col("season") > 2013).then(pl.lit(75)).otherwise(pl.lit(80)))
                .when(
                    (pl.col("type.text").is_in(kickoff_vec).or_(pl.col("text").str.contains(r"(?i)kickoff")))
                    .and_(pl.col("text").str.contains(r"(?i)penalty"))
                    .and_(
                        pl.col("text").str.contains(
                            # Era vocabulary: ESPN wrote "for N yards" through 2013 and
                            # "for N yds" from 2014. The changeover is total -- 98% one
                            # side, 0% the other, no overlap season -- so matching only
                            # "yds" made this gate fire on 108 pre-2014 rows that DO
                            # describe a kick ("kickoff for 50 yards out-of-bounds"),
                            # rewriting an end state that was never in question.
                            # Out-of-bounds is hyphenated in the old texts, spaced in new.
                            # 2025 adds a THIRD kickoff vocabulary. Alongside "for N yards"
                            # (2004-2013) and "for N yds" (2014+), a vendor feed writes
                            # "(15:00) #36 T.Morrison kickoff 65 yards to the SAC00" -- no
                            # "for" at all, and only 57% of 2025 kickoff rows carry the
                            # 2014 form. Without it this gate called those rows "no kick
                            # described": 12 in 2025, 5 in 2023, 1 in 2022.
                            #
                            # The distance is anchored on the word kickoff rather than
                            # matched loosely, because a bare "N yards" also appears as the
                            # PENALTY yardage on exactly the rows part (d) exists to repair
                            # ("kickoff UTEP Penalty, Targeting ... (-15 Yards) to the UTEP
                            # 20"). Anchoring cures all 18 false fires and over-excludes
                            # nothing, in any season from 2005 to 2025.
                            r"(?i)kick(?:off)?\s+(?:for\s+)?-?\d+\s*(?:yds|yards)"
                            r"|for \d+ (?:yds|yards)|touchback|return|out.of.bounds|recovered|downed|fair catch",
                        )
                        == False
                    )
                    .and_(pl.col("_pen_nxt_pos") == pl.col("end.pos_team.id"))
                    .and_(pl.col("_pen_nxt_start").is_null() == False),
                )
                .then(pl.col("_pen_nxt_start"))
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
            )
            .drop(["_ko_pen_rekick_follows", "_pen_nxt_start", "_pen_nxt_pos"])
            .with_columns(
                # B3 part (e): the kick-return end state, when ESPN's own structured
                # fields contradict its own text.
                #
                # Raw payload for 401762869 p122 carries end.yardLine 8 and
                # end.yardsToEndzone 8, while its text ends "... to EMU08" -- EMU, the
                # possessing team, on their own 8, which is 92 to go. 8 is the yardline
                # stored where the distance belongs, and start is correct on the same
                # row, so it is not a wholesale feed failure. Published 2025 carries 40
                # kickoff penalties with end.yardsToEndzone <= 25 against 5-11 in each of
                # 2022-24, 29 above |EPA| 4 against 3-4, and a season mean kickoff-penalty
                # EPA of +0.233 where every other year is negative.
                #
                # Parts (b), (c) and (d) all miss it: the end state is present, 8 is not
                # the complement of the NEXT play's start, and the text plainly describes
                # a kick and a return so part (d) correctly declines it.
                #
                # THE TEXT ALONE IS NOT ENOUGH. Where text and field disagree, the field
                # is usually right -- 313 of 326 adjudicable disagreements in 2005, 88 of
                # 94 in 2018, 29 of 39 in 2024 -- because the side resolution fails on
                # older abbreviations and returns the complement. Only 2025 inverts, 73
                # to 38. A repair that trusted the text would corrupt hundreds of rows.
                #
                # So the gate requires TWO independent sources against the field: the
                # text's final spot, and the next real play starting exactly there with
                # possession unchanged. That fires on 113 rows across 2005-2025, of which
                # 73 are 2025 and no season before it exceeds 7.
                _ret_spot=pl.struct(
                    [
                        "text",
                        "homeTeamAbbrev",
                        "awayTeamAbbrev",
                        "homeTeamName",
                        "awayTeamName",
                        "homeTeamNameAlt",
                        "awayTeamNameAlt",
                        "homeTeamMascot",
                        "awayTeamMascot",
                    ],
                ).map_elements(_parse_return_spot, return_dtype=pl.Utf8),
                # The next REAL play, skipping up to two administrative rows -- a Timeout
                # sits between a kickoff and the ensuing snap often enough that a bare
                # shift(-1) reads the wrong row (401762869: kickoff p122, Timeout p123).
                _nxt_start=pl.when(
                    pl.col("type.text").shift(-1).str.contains(_ADMIN_ROW_RE),
                )
                .then(
                    pl.when(pl.col("type.text").shift(-2).str.contains(_ADMIN_ROW_RE))
                    .then(pl.col("start.yardsToEndzone").shift(-3))
                    .otherwise(pl.col("start.yardsToEndzone").shift(-2)),
                )
                .otherwise(pl.col("start.yardsToEndzone").shift(-1)),
                _nxt_pos=pl.when(
                    pl.col("type.text").shift(-1).str.contains(_ADMIN_ROW_RE),
                )
                .then(
                    pl.when(pl.col("type.text").shift(-2).str.contains(_ADMIN_ROW_RE))
                    .then(pl.col("start.pos_team.id").shift(-3))
                    .otherwise(pl.col("start.pos_team.id").shift(-2)),
                )
                .otherwise(pl.col("start.pos_team.id").shift(-1)),
            )
            .with_columns(
                # Resolved against the END possession team, which is what
                # end.yardsToEndzone expresses -- the distance for whoever has the ball
                # when the play is over. start.pos_team.id agrees on a kickoff (the same
                # team on 99% of them) but is the LOSING side on a punt or an
                # interception, so it returns the complement there. Measured agreement
                # with the field: kickoffs 99.4% end-ref vs 98.7% start-ref in 2024,
                # punts 99.7% vs 3.5%, interceptions 99.3% vs 6.1%.
                _ret_y2ez=pl.when(pl.col("_ret_spot").str.starts_with("mid"))
                .then(pl.lit(50))
                .when(
                    (pl.col("_ret_spot").str.starts_with("home")).and_(
                        pl.col("end.pos_team.id") == pl.col("homeTeamId"),
                    )
                    | (pl.col("_ret_spot").str.starts_with("away")).and_(
                        pl.col("end.pos_team.id") != pl.col("homeTeamId"),
                    ),
                )
                .then(pl.lit(100) - pl.col("_ret_spot").str.extract(r":(\d+)$", 1).cast(pl.Int64, strict=False))
                .when(pl.col("_ret_spot").is_not_null())
                .then(pl.col("_ret_spot").str.extract(r":(\d+)$", 1).cast(pl.Int64, strict=False))
                .otherwise(None),
            )
            .with_columns(
                pl.when(
                    pl.col("_ret_y2ez")
                    .is_not_null()
                    .and_(pl.col("end.yardsToEndzone").is_null() == False)
                    .and_(pl.col("_ret_y2ez") != pl.col("end.yardsToEndzone"))
                    .and_(pl.col("_nxt_pos") == pl.col("end.pos_team.id"))
                    .and_(pl.col("_nxt_start") == pl.col("_ret_y2ez")),
                )
                .then(pl.col("_ret_y2ez"))
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
            )
            .drop(["_ret_spot", "_ret_y2ez", "_nxt_start", "_nxt_pos"])
        )
        pbp_txt["firstHalfKickoffTeamId"] = np.where(
            (pbp_txt["plays"]["game_play_number"] == 1)
            & (pbp_txt["plays"]["type.text"].is_in(kickoff_vec))
            & (pbp_txt["plays"]["start.team.id"] == init["homeTeamId"]),
            init["homeTeamId"],
            init["awayTeamId"],
        )
        pbp_txt["firstHalfKickoffTeamId"] = pbp_txt["firstHalfKickoffTeamId"][0]

        if "scoringType.displayName" in pbp_txt["plays"].columns:
            pbp_txt["plays"] = (
                pbp_txt["plays"]
                .with_columns(
                    pl.when(pl.col("scoringType.displayName") == "Field Goal")
                    .then(pl.lit("Field Goal Good"))
                    .otherwise(pl.col("type.text"))
                    .alias("type.text"),
                )
                .with_columns(
                    pl.when(pl.col("scoringType.displayName") == "Extra Point")
                    .then(pl.lit("Extra Point Good"))
                    .otherwise(pl.col("type.text"))
                    .alias("type.text"),
                )
            )
        pbp_txt["plays"] = (
            pbp_txt["plays"]
            .with_columns(
                pl.when(pl.col("type.text").is_null())
                .then(pl.lit("Unknown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    pl.col("type.text")
                    .str.to_lowercase()
                    .str.contains("(?i)extra point")
                    .and_(pl.col("type.text").str.to_lowercase().str.contains("(?i)no good")),
                )
                .then(pl.lit("Extra Point Missed"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    pl.col("type.text")
                    .str.to_lowercase()
                    .str.contains("(?i)extra point")
                    .and_(pl.col("type.text").str.to_lowercase().str.contains("(?i)blocked")),
                )
                .then(pl.lit("Extra Point Missed"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    pl.col("type.text")
                    .str.to_lowercase()
                    .str.contains("(?i)field goal")
                    .and_(pl.col("type.text").str.to_lowercase().str.contains("(?i)blocked")),
                )
                .then(pl.lit("Extra Point Missed"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    pl.col("type.text")
                    .str.to_lowercase()
                    .str.contains("(?i)field goal")
                    .and_(pl.col("type.text").str.to_lowercase().str.contains("(?i)no good")),
                )
                .then(pl.lit("Extra Point Missed"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
        )

        return pbp_txt

    def __helper_cfb_pbp(self, pbp_txt):
        # ESPN's summary endpoint intermittently returns a payload with no
        # `header.competitions` (transient gap / a game not yet ingested).
        # Short-circuit with a clear, catchable NoDataError *before* pickcenter
        # resolution (which would otherwise make a fallback odds network hop) and
        # before the deep `KeyError: 'competitions'` in __helper_cfb_game_data.
        # Local import so the error class doesn't leak into the package namespace
        # (it is not a public wrapper) and trip the codegen autodoc/parsed gates.
        from sportsdataverse.errors import NoDataError

        if not ((pbp_txt.get("header") or {}).get("competitions") or []):
            raise NoDataError(
                f"ESPN summary for game {self.gameId} has no header.competitions; cannot build play-by-play.",
            )
        init = self.__helper_cfb_pickcenter(pbp_txt)
        return self.__helper_cfb_game_data(pbp_txt, init)

    def __helper__espn_cfb_odds_information__(self):
        """Fetch pre-game spread/total from ESPN's modern core odds endpoint.

        Returns ``(gameSpread, overUnder, homeFavorite, gameSpreadAvailable)``.
        ESPN emptied the legacy ``pickcenter`` array on the summary endpoint
        for 2024+ college games; this helper restores the data path for those
        games via the ``sports.core.api.espn.com`` v2 odds collection. Falls
        back to defaults ``(2.5, 55.5, True, False)`` when the endpoint
        returns no items, errors out, or the JSON cannot be decoded —
        preserving the legacy caller-visible behavior on those failure
        paths.
        """
        cache_buster = int(time.time() * 1000)
        odds_url = (
            f"https://sports.core.api.espn.com/v2/sports/football/leagues/"
            f"college-football/events/{self.gameId}/competitions/{self.gameId}/"
            f"odds?limit=100&{cache_buster}"
        )
        try:
            odds_resp = download(odds_url)
            odds = odds_resp.json()
        except Exception as e:
            logger.warning(
                "%s: odds fetch failed (%r); falling back to defaults",
                self.gameId,
                e,
            )
            return (2.5, 55.5, True, False)

        items = odds.get("items", []) if isinstance(odds, dict) else []
        if not items:
            return (2.5, 55.5, True, False)

        # Prefer ESPN BET (their canonical book) when present; the upstream
        # 0.36-live helper assumed items[0] was ESPN BET, but the items array
        # is sorted by provider.id, so this is provider-dependent. Falling
        # back to items[0] preserves that legacy ordering when no explicit
        # match is found.
        espn_bet = next(
            (
                it
                for it in items
                if isinstance(it, dict) and (it.get("provider") or {}).get("name", "").lower() == "espn bet"
            ),
            items[0],
        )

        spread_raw = espn_bet.get("spread") if isinstance(espn_bet, dict) else None
        ou_raw = espn_bet.get("overUnder") if isinstance(espn_bet, dict) else None
        home_odds = (espn_bet.get("homeTeamOdds") or {}) if isinstance(espn_bet, dict) else {}
        home_fav_raw = home_odds.get("favorite")

        gameSpreadAvailable = spread_raw is not None
        gameSpread = float(spread_raw) if spread_raw is not None else 2.5
        overUnder = float(ou_raw) if ou_raw is not None else 55.5
        homeFavorite = bool(home_fav_raw) if home_fav_raw is not None else True
        return (gameSpread, overUnder, homeFavorite, gameSpreadAvailable)

    def __helper_cfb_pickcenter(self, pbp_txt):
        # Spread definition
        if self.odds_override is not None:
            o = self.odds_override
            self.gameSpread = o["gameSpread"]
            self.overUnder = o["overUnder"]
            self.homeFavorite = o["homeFavorite"]
            self.gameSpreadAvailable = o["gameSpreadAvailable"]
            self.odds_source = "injected"
            return {
                "gameSpread": self.gameSpread,
                "overUnder": self.overUnder,
                "homeFavorite": self.homeFavorite,
                "gameSpreadAvailable": self.gameSpreadAvailable,
            }
        if len(pbp_txt.get("pickcenter", [])) > 1:
            pickcenter = pd.json_normalize(data=pbp_txt, record_path="pickcenter")
            pickcenter = pickcenter.sort_values(by=["provider.id"])
            homeFavorite = (
                pickcenter[pickcenter["homeTeamOdds.favorite"].notnull()][["homeTeamOdds.favorite"]].values[0]
                if "homeTeamOdds.favorite" in pickcenter.columns
                else True
            )
            gameSpread = (
                pickcenter[pickcenter["spread"].notnull()][["spread"]].values[0]
                if "spread" in pickcenter.columns
                else 2.5
            )
            overUnder = (
                pickcenter[pickcenter["overUnder"].notnull()][["overUnder"]].values[0]
                if "overUnder" in pickcenter.columns
                else 55.0
            )
            gameSpreadAvailable = True
            self.odds_source = "summary_pickcenter"
            # self.logger.info(f"Spread: {gameSpread}, home Favorite: {homeFavorite}, ou: {overUnder}")
        else:
            # Cascade: legacy `pickcenter` array is empty (true for all 2024+
            # games on ESPN's summary endpoint). Try the modern core odds
            # endpoint before silently falling through to defaults — which
            # would otherwise corrupt every WPA/EP downstream calculation
            # because every play would inherit `(2.5, 55.0, True)`.
            (
                gameSpread,
                overUnder,
                homeFavorite,
                gameSpreadAvailable,
            ) = self.__helper__espn_cfb_odds_information__()
            self.odds_source = "core_odds_api" if gameSpreadAvailable else "default"
        self.gameSpread = gameSpread
        self.overUnder = overUnder
        self.homeFavorite = homeFavorite
        self.gameSpreadAvailable = gameSpreadAvailable
        return {
            "gameSpread": gameSpread,
            "overUnder": overUnder,
            "homeFavorite": homeFavorite,
            "gameSpreadAvailable": gameSpreadAvailable,
        }

    def __helper_cfb_game_data(self, pbp_txt, init):
        pbp_txt["timeouts"] = {}
        pbp_txt["teamInfo"] = pbp_txt["header"]["competitions"][0]
        pbp_txt["season"] = pbp_txt["header"]["season"]
        pbp_txt["playByPlaySource"] = pbp_txt["header"]["competitions"][0]["playByPlaySource"]
        pbp_txt["boxscoreSource"] = pbp_txt["header"]["competitions"][0]["boxscoreSource"]
        pbp_txt["gameSpreadAvailable"] = init["gameSpreadAvailable"]
        pbp_txt["gameSpread"] = init["gameSpread"]
        pbp_txt["homeFavorite"] = init["homeFavorite"]
        pbp_txt["homeTeamSpread"] = np.where(
            init["homeFavorite"] == True,
            abs(init["gameSpread"]),
            -1 * abs(init["gameSpread"]),
        )
        pbp_txt["overUnder"] = init["overUnder"]
        pbp_txt["odds_source"] = self.odds_source
        # Home and Away identification variables
        if pbp_txt["header"]["competitions"][0]["competitors"][0]["homeAway"] == "home":
            pbp_txt["header"]["competitions"][0]["home"] = pbp_txt["header"]["competitions"][0]["competitors"][0][
                "team"
            ]
            homeTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["id"])
            homeTeamMascot = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["name"])
            homeTeamName = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["location"])
            homeTeamAbbrev = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["abbreviation"])
            homeTeamNameAlt = re.sub("Stat(.+)", "St", homeTeamName)
            pbp_txt["header"]["competitions"][0]["away"] = pbp_txt["header"]["competitions"][0]["competitors"][1][
                "team"
            ]
            awayTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["id"])
            awayTeamMascot = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["name"])
            awayTeamName = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["location"])
            awayTeamAbbrev = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["abbreviation"])
            awayTeamNameAlt = re.sub("Stat(.+)", "St", awayTeamName)
        else:
            pbp_txt["header"]["competitions"][0]["away"] = pbp_txt["header"]["competitions"][0]["competitors"][0][
                "team"
            ]
            awayTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["id"])
            awayTeamMascot = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["name"])
            awayTeamName = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["location"])
            awayTeamAbbrev = str(pbp_txt["header"]["competitions"][0]["competitors"][0]["team"]["abbreviation"])
            awayTeamNameAlt = re.sub("Stat(.+)", "St", awayTeamName)
            pbp_txt["header"]["competitions"][0]["home"] = pbp_txt["header"]["competitions"][0]["competitors"][1][
                "team"
            ]
            homeTeamId = int(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["id"])
            homeTeamMascot = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["name"])
            homeTeamName = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["location"])
            homeTeamAbbrev = str(pbp_txt["header"]["competitions"][0]["competitors"][1]["team"]["abbreviation"])
            homeTeamNameAlt = re.sub("Stat(.+)", "St", homeTeamName)
        init["homeTeamId"] = homeTeamId
        init["homeTeamMascot"] = homeTeamMascot
        init["homeTeamName"] = homeTeamName
        init["homeTeamAbbrev"] = homeTeamAbbrev
        init["homeTeamNameAlt"] = homeTeamNameAlt
        init["awayTeamId"] = awayTeamId
        init["awayTeamMascot"] = awayTeamMascot
        init["awayTeamName"] = awayTeamName
        init["awayTeamAbbrev"] = awayTeamAbbrev
        init["awayTeamNameAlt"] = awayTeamNameAlt
        self.homeTeamId = homeTeamId
        self.homeTeamMascot = homeTeamMascot
        self.homeTeamName = homeTeamName
        self.homeTeamAbbrev = homeTeamAbbrev
        self.homeTeamNameAlt = homeTeamNameAlt
        self.awayTeamId = awayTeamId
        self.awayTeamMascot = awayTeamMascot
        self.awayTeamName = awayTeamName
        self.awayTeamAbbrev = awayTeamAbbrev
        self.awayTeamNameAlt = awayTeamNameAlt
        return pbp_txt, init

    def __add_downs_data(self, play_df):
        """
        Creates the following columns in play_df:
            * id, drive_id, game_id
            * down, ydstogo (distance), game_half, period
        """
        play_df = _sort_plays_ot_aware(play_df)

        play_df = play_df.unique(
            subset=["text", "id", "type.text", "start.down", "sequenceNumber"],
            keep="last",
            maintain_order=True,
        )
        play_df = play_df.filter(
            pl.col("type.text").str.contains("(?i)end of|(?i)coin toss|(?i)end period|(?i)wins toss") == False,
        )
        play_df = (
            play_df.with_columns(
                period=pl.col("period.number"),
                half=pl.when(pl.col("period.number") <= 2).then(1).otherwise(2),
            )
            .with_columns(
                lead_half=pl.col("half").shift(-1),
                lag_scoringPlay=pl.col("scoringPlay").shift(1),
            )
            .with_columns(
                pl.when(pl.col("lead_half").is_null()).then(2).otherwise(pl.col("lead_half")).alias("lead_half"),
                end_of_half=pl.col("half") != pl.col("lead_half"),
                down_1=pl.col("start.down") == 1,
                down_2=pl.col("start.down") == 2,
                down_3=pl.col("start.down") == 3,
                down_4=pl.col("start.down") == 4,
                down_1_end=pl.col("end.down") == 1,
                down_2_end=pl.col("end.down") == 2,
                down_3_end=pl.col("end.down") == 3,
                down_4_end=pl.col("end.down") == 4,
            )
        )

        return play_df

    def __add_play_type_flags(self, play_df):
        """
        Creates the following columns in play_df:
            * Flags for fumbles, scores, kickoffs, punts, field goals
        """
        # --- Touchdown, Fumble, Special Teams flags -----------------
        play_df = (
            play_df.with_columns(
                scoring_play=pl.when(pl.col("type.text").is_in(scores_vec)).then(True).otherwise(False),
                # A touchdown ESPN says was wiped out is not a touchdown. The
                # marker has to be applied HERE, at the definition, because the
                # scoring logic below consumes `td_play` long before
                # `__setup_penalty_data` runs -- hence the shared constant
                # rather than a second copy of the pattern.
                td_play=pl.col("text").str.contains("(?i)touchdown|(?i)for a TD")
                & ~pl.col("text").str.contains(_PENALTY_NEGATED_TEXT),
                touchdown=pl.col("type.text").str.contains("(?i)touchdown")
                & ~pl.col("text").str.contains(_PENALTY_NEGATED_TEXT),
                ## Portion of touchdown check for plays where touchdown is not listed in the play_type--
                td_check=pl.col("text").str.contains("(?i)touchdown")
                & ~pl.col("text").str.contains(_PENALTY_NEGATED_TEXT),
                safety=pl.col("text").str.contains("(?i)safety"),
                fumble_vec=pl.when(pl.col("text").str.contains("(?i)fumble"))
                .then(True)
                .when(
                    (pl.col("text").str.contains("(?i)fumble")).and_(
                        pl.col("type.text") == "Rush",
                        pl.col("start.pos_team.id") != pl.col("end.pos_team.id"),
                    ),
                )
                .then(True)
                .when(
                    (pl.col("text").str.contains("(?i)fumble")).and_(
                        pl.col("type.text") == "Sack",
                        pl.col("start.pos_team.id") != pl.col("end.pos_team.id"),
                    ),
                )
                .then(True)
                .otherwise(False),
                forced_fumble=pl.when(pl.col("text").str.contains("(?i)forced by")).then(True).otherwise(False),
                # --- Kicks----
                kickoff_play=pl.col("type.text").is_in(kickoff_vec),
            )
            .with_columns(
                kickoff_tb=pl.when((pl.col("text").str.contains("(?i)touchback")).and_(pl.col("kickoff_play") == True))
                .then(True)
                # 2018+ NCAA rule: a fair catch of a kickoff between the goal line and
                # the receiver's 25 is a touchback (ball spotted at the 25). Gated on
                # season so it does not mis-place pre-rule fair-caught kickoffs.
                # (0.36-live applied this ungated; the season gate is the correctness
                # refinement. Confirm the 2018 cutoff if porting to other rulebooks.)
                .when(
                    (pl.col("text").str.contains(r"(?i)fair catch|fair caught"))
                    .and_(pl.col("kickoff_play") == True)
                    .and_(pl.col("season") >= 2018),
                )
                .then(True)
                .when((pl.col("text").str.contains("(?i)kickoff$")).and_(pl.col("kickoff_play") == True))
                .then(True)
                .otherwise(False),
                kickoff_onside=pl.when(
                    (pl.col("text").str.contains("(?i)on-side|(?i)onside|(?i)on side")).and_(
                        pl.col("kickoff_play") == True,
                    ),
                )
                .then(True)
                .otherwise(False),
                kickoff_oob=pl.when(
                    (pl.col("text").str.contains("(?i)out-of-bounds|(?i)out of bounds")).and_(
                        pl.col("kickoff_play") == True,
                    ),
                )
                .then(True)
                .otherwise(False),
                kickoff_fair_catch=pl.when(
                    (pl.col("text").str.contains("(?i)fair catch|(?i)fair caught")).and_(
                        pl.col("kickoff_play") == True,
                    ),
                )
                .then(True)
                .otherwise(False),
                kickoff_downed=pl.when((pl.col("text").str.contains("(?i)downed")).and_(pl.col("kickoff_play") == True))
                .then(True)
                .otherwise(False),
                kick_play=pl.col("text").str.contains("(?i)kick|(?i)kickoff"),
                kickoff_safety=pl.when(
                    (pl.col("text").str.contains("(?i)kickoff")).and_(
                        pl.col("safety") == True,
                        pl.col("type.text").is_in(["Blocked Punt", "Penalty"]) == False,
                    ),
                )
                .then(True)
                .otherwise(False),
                # --- Punts----
                punt=pl.col("type.text").is_in(punt_vec),
                punt_play=pl.col("text").str.contains("(?i)punt"),
            )
            .with_columns(
                punt_tb=pl.when((pl.col("text").str.contains("(?i)touchback")).and_(pl.col("punt") == True))
                .then(True)
                .otherwise(False),
                punt_oob=pl.when(
                    (pl.col("text").str.contains("(?i)out-of-bounds|(?i)out of bounds")).and_(pl.col("punt") == True),
                )
                .then(True)
                .otherwise(False),
                punt_fair_catch=pl.when(
                    (pl.col("text").str.contains("(?i)fair catch|(?i)fair caught")).and_(pl.col("punt") == True),
                )
                .then(True)
                .otherwise(False),
                punt_downed=pl.when((pl.col("text").str.contains("(?i)downed")).and_(pl.col("punt") == True))
                .then(True)
                .otherwise(False),
                punt_safety=pl.when((pl.col("text").str.contains("(?i)punt")).and_(pl.col("safety") == True))
                .then(True)
                .otherwise(False),
                punt_blocked=pl.when((pl.col("text").str.contains("(?i)blocked")).and_(pl.col("punt") == True))
                .then(True)
                .otherwise(False),
                penalty_safety=pl.when((pl.col("type.text").is_in(["Penalty"])).and_(pl.col("safety") == True))
                .then(True)
                .otherwise(False),
            )
        )

        return play_df

    def __add_rush_pass_flags(self, play_df):
        """
        Creates the following columns in play_df:
            * Rush, Pass, Sacks
        """

        play_df = (
            play_df.with_columns(
                # --- Pass/Rush----
                pl.when(
                    (pl.col("type.text") == "Rush")
                    .or_(pl.col("type.text") == "Rushing Touchdown")
                    .or_(
                        (
                            pl.col("type.text").is_in(
                                [
                                    "Safety",
                                    "Fumble Recovery (Opponent)",
                                    "Fumble Recovery (Opponent) Touchdown",
                                    "Fumble Recovery (Own)",
                                    "Fumble Recovery (Own) Touchdown",
                                    "Fumble Return Touchdown",
                                ],
                            )
                        ).and_(pl.col("text").str.contains("run for")),
                    ),
                )
                .then(True)
                .otherwise(False)
                .alias("rush"),
                pl.when(
                    (
                        pl.col("type.text").is_in(
                            [
                                "Pass Reception",
                                "Pass Completion",
                                "Passing Touchdown",
                                "Sack",
                                "Pass",
                                "Interception",
                                "Pass Interception Return",
                                "Interception Return Touchdown",
                                "Pass Incompletion",
                                "Sack Touchdown",
                                "Interception Return",
                            ],
                        )
                    )
                    .or_((pl.col("type.text") == "Safety").and_(pl.col("text").str.contains("sacked")))
                    .or_((pl.col("type.text") == "Safety").and_(pl.col("text").str.contains("pass complete")))
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Own)").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete|pass intercepted"),
                        ),
                    )
                    .or_((pl.col("type.text") == "Fumble Recovery (Own)").and_(pl.col("text").str.contains("sacked")))
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Own) Touchdown").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete|pass intercepted"),
                        ),
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Own) Touchdown").and_(
                            pl.col("text").str.contains("sacked"),
                        ),
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Opponent)").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete|pass intercepted"),
                        ),
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Opponent)").and_(
                            pl.col("text").str.contains("sacked"),
                        ),
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Recovery (Opponent) Touchdown").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete"),
                        ),
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Return Touchdown").and_(
                            pl.col("text").str.contains(r"pass complete|pass incomplete"),
                        ),
                    )
                    .or_(
                        (pl.col("type.text") == "Fumble Return Touchdown").and_(pl.col("text").str.contains("sacked")),
                    )
                    # Interception plays are pass attempts. The branches above
                    # only catch them in the 2005 and 2014+ text formats; 2004
                    # and 2006-2013 interception rows shipped with pass=False
                    # (measured: season flag-rate 0.0 there vs 1.0 elsewhere;
                    # 12,890 rows -- the int_implies_pass definitional WARN).
                    .or_(
                        pl.col("type.text")
                        .is_in(
                            [
                                "Interception Return",
                                "Interception Return Touchdown",
                                "Pass Interception",
                                "Pass Interception Return",
                            ],
                        )
                        .and_(pl.col("text").str.contains(r"(?i)intercept")),
                    ),
                )
                .then(True)
                .otherwise(False)
                .alias("pass"),
            )
            .with_columns(
                # --- Sacks----
                sack_vec=pl.when(
                    (pl.col("type.text").is_in(["Sack", "Sack Touchdown"])).or_(
                        (
                            pl.col("type.text").is_in(
                                [
                                    "Fumble Recovery (Own)",
                                    "Fumble Recovery (Own) Touchdown",
                                    "Fumble Recovery (Opponent)",
                                    "Fumble Recovery (Opponent) Touchdown",
                                    "Fumble Return Touchdown",
                                ],
                            )
                        ).and_(pl.col("text").str.contains("(?i)sacked"), pl.col("pass") == True),
                    ),
                )
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                pl.when(pl.col("sack_vec") == True).then(True).otherwise(pl.col("pass")).alias("pass"),
            )
        )

        return play_df

    def __add_team_score_variables(self, play_df):
        """
        Creates the following columns in play_df:
            * Team Score variables
            * Fix change of poss variables
        """
        play_df = (
            play_df.with_columns(
                pos_team=pl.col("start.pos_team.id"),
                def_pos_team=pl.col("start.def_pos_team.id"),
            )
            .with_columns(
                is_home=pl.col("pos_team") == pl.col("homeTeamId"),
                # --- Team Score variables ------
                lag_homeScore=pl.col("homeScore").shift(1),
                lag_awayScore=pl.col("awayScore").shift(1),
            )
            .with_columns(
                lag_HA_score_diff=pl.col("lag_homeScore") - pl.col("lag_awayScore"),
                HA_score_diff=pl.col("homeScore") - pl.col("awayScore"),
            )
            .with_columns(
                net_HA_score_pts=pl.col("HA_score_diff") - pl.col("lag_HA_score_diff"),
                H_score_diff=pl.col("homeScore") - pl.col("lag_homeScore"),
                A_score_diff=pl.col("awayScore") - pl.col("lag_awayScore"),
            )
            .with_columns(
                homeScore=pl.when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("H_score_diff") >= 9),
                )
                .then(pl.col("lag_homeScore"))
                .when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("H_score_diff") < 9)
                    & (pl.col("H_score_diff") > 1),
                )
                .then(pl.col("lag_homeScore"))
                .when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("H_score_diff") >= -9)
                    & (pl.col("H_score_diff") < -1),
                )
                .then(pl.col("homeScore"))
                .otherwise(pl.col("homeScore")),
                awayScore=pl.when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("A_score_diff") >= 9),
                )
                .then(pl.col("lag_awayScore"))
                .when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("A_score_diff") < 9)
                    & (pl.col("A_score_diff") > 1),
                )
                .then(pl.col("lag_awayScore"))
                .when(
                    (pl.col("scoringPlay") == False)
                    & (pl.col("game_play_number") != 1)
                    & (pl.col("A_score_diff") >= -9)
                    & (pl.col("A_score_diff") < -1),
                )
                .then(pl.col("awayScore"))
                .otherwise(pl.col("awayScore")),
            )
            .drop(["lag_homeScore", "lag_awayScore"])
            .with_columns(
                lag_homeScore=pl.col("homeScore").shift(1),
                lag_awayScore=pl.col("awayScore").shift(1),
            )
            .with_columns(
                lag_homeScore=pl.when(pl.col("lag_homeScore").is_null()).then(0).otherwise(pl.col("lag_homeScore")),
                lag_awayScore=pl.when(pl.col("lag_awayScore").is_null()).then(0).otherwise(pl.col("lag_awayScore")),
            )
            .with_columns(
                pl.when(pl.col("game_play_number") == 1)
                .then(0)
                .otherwise(pl.col("lag_homeScore"))
                .alias("start.homeScore"),
                pl.when(pl.col("game_play_number") == 1)
                .then(0)
                .otherwise(pl.col("lag_awayScore"))
                .alias("start.awayScore"),
                pl.col("homeScore").alias("end.homeScore"),
                pl.col("awayScore").alias("end.awayScore"),
                pl.when(pl.col("pos_team") == pl.col("homeTeamId"))
                .then(pl.col("homeScore"))
                .otherwise(pl.col("awayScore"))
                .alias("pos_team_score"),
                pl.when(pl.col("pos_team") == pl.col("homeTeamId"))
                .then(pl.col("awayScore"))
                .otherwise(pl.col("homeScore"))
                .alias("def_pos_team_score"),
            )
            .with_columns(
                pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("start.homeScore"))
                .otherwise(pl.col("start.awayScore"))
                .alias("start.pos_team_score"),
                pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("start.awayScore"))
                .otherwise(pl.col("start.homeScore"))
                .alias("start.def_pos_team_score"),
            )
            .with_columns(
                (pl.col("start.pos_team_score") - pl.col("start.def_pos_team_score")).alias("start.pos_score_diff"),
                pl.when(pl.col("pos_team") == pl.col("homeTeamId"))
                .then(pl.col("end.homeScore"))
                .otherwise(pl.col("end.awayScore"))
                .alias("end.pos_team_score"),
                pl.when(pl.col("pos_team") == pl.col("homeTeamId"))
                .then(pl.col("end.awayScore"))
                .otherwise(pl.col("end.homeScore"))
                .alias("end.def_pos_team_score"),
            )
            .with_columns(
                (pl.col("end.pos_team_score") - pl.col("end.def_pos_team_score")).alias("end.pos_score_diff"),
                pl.col("pos_team").shift(1).alias("lag_pos_team"),
            )
            .with_columns(
                pl.when(pl.col("lag_pos_team").is_null())
                .then(pl.col("pos_team"))
                .otherwise(pl.col("lag_pos_team"))
                .alias("lag_pos_team"),
                pl.col("pos_team").shift(-1).alias("lead_pos_team"),
                pl.col("pos_team").shift(-2).alias("lead_pos_team2"),
                (pl.col("pos_team_score") - pl.col("def_pos_team_score")).alias("pos_score_diff"),
            )
            .with_columns(
                pl.col("pos_score_diff").shift(1).alias("lag_pos_score_diff"),
            )
            .with_columns(
                pl.when(pl.col("lag_pos_score_diff").is_null())
                .then(0)
                .otherwise(pl.col("lag_pos_score_diff"))
                .alias("lag_pos_score_diff"),
            )
            .with_columns(
                pl.when(pl.col("lag_pos_team") == pl.col("pos_team"))
                .then(pl.col("pos_score_diff") - pl.col("lag_pos_score_diff"))
                .otherwise(pl.col("pos_score_diff") + pl.col("lag_pos_score_diff"))
                .alias("pos_score_pts"),
                pl.when((pl.col("kickoff_play") == True).and_(pl.col("lag_pos_team") == pl.col("pos_team")))
                .then(pl.col("lag_pos_score_diff"))
                .when((pl.col("kickoff_play") == True).or_(pl.col("lag_pos_team") != pl.col("pos_team")))
                .then(-1 * pl.col("lag_pos_score_diff"))
                .otherwise(pl.col("lag_pos_score_diff"))
                .alias("pos_score_diff_start"),
            )
            .with_columns(
                pl.when(pl.col("pos_score_diff_start").is_null() == True)
                .then(pl.col("pos_score_diff"))
                .otherwise(pl.col("pos_score_diff_start"))
                .alias("pos_score_diff_start"),
                pl.when(pl.col("start.pos_team.id") == pl.col("firstHalfKickoffTeamId"))
                .then(True)
                .otherwise(False)
                .alias("start.pos_team_receives_2H_kickoff"),
                pl.when(pl.col("end.pos_team.id") == pl.col("firstHalfKickoffTeamId"))
                .then(True)
                .otherwise(False)
                .alias("end.pos_team_receives_2H_kickoff"),
                pl.when(pl.col("start.pos_team.id") == pl.col("end.pos_team.id"))
                .then(False)
                .otherwise(True)
                .alias("change_of_poss"),
            )
            .with_columns(
                pl.when(pl.col("change_of_poss").is_null() == True)
                .then(False)
                .otherwise(pl.col("change_of_poss"))
                .alias("change_of_poss"),
            )
        )

        return play_df

    def __add_new_play_types(self, play_df):
        """
        Creates the following columns in play_df:
            * Fix play types
        """
        # --------------------------------------------------
        # --- Legacy / pre-2014 ESPN label normalization ----
        # These raw labels appear only in older seasons (verified 2004-2013); every
        # rule is gated on the raw label, so it is a no-op on modern data.
        play_df = play_df.with_columns(
            # ESPN's pre-2014 *successful* two-point label is the bare "2pt Conversion"
            # (failed ones are already "Two-Point Conversion Missed"). Resolve good/missed
            # via scoringPlay so the play routes through the two-point EPA/scoring path
            # instead of being treated as a generic scrimmage play.
            pl.when((pl.col("type.text") == "2pt Conversion").and_(pl.col("scoringPlay") == True))
            .then(pl.lit("Two-Point Conversion Good"))
            .when(pl.col("type.text") == "2pt Conversion")
            .then(pl.lit("Two-Point Conversion Missed"))
            # 2004 "Unknown" is a grab-bag of period/game markers and a few misclassified
            # kicks. Relabel the recognizable ones from the text so the non-plays are
            # excluded (End Period) and the real kicks get a proper type + EPA, instead of
            # being scored as generic plays (which produced garbage EPA on non-plays).
            .when(
                (pl.col("type.text") == "Unknown").and_(
                    pl.col("text").str.contains(r"(?i)(start|end) of (the )?.*(quarter|half|game|overtime|regulation)"),
                ),
            )
            .then(pl.lit("End Period"))
            .when(
                (pl.col("type.text") == "Unknown")
                .and_(pl.col("text").str.contains(r"(?i)field goal"))
                .and_(pl.col("text").str.contains(r"(?i)no good|missed|blocked")),
            )
            .then(pl.lit("Field Goal Missed"))
            .when(
                (pl.col("type.text") == "Unknown")
                .and_(pl.col("text").str.contains(r"(?i)field goal"))
                .and_(pl.col("text").str.contains(r"(?i)is good")),
            )
            .then(pl.lit("Field Goal Good"))
            .when(
                (pl.col("type.text") == "Unknown")
                .and_(pl.col("text").str.contains(r"(?i)extra point"))
                .and_(pl.col("text").str.contains(r"(?i)no good|missed|blocked")),
            )
            .then(pl.lit("Extra Point Missed"))
            .when(
                (pl.col("type.text") == "Unknown")
                .and_(pl.col("text").str.contains(r"(?i)extra point"))
                .and_(pl.col("text").str.contains(r"(?i)is good")),
            )
            .then(pl.lit("Extra Point Good"))
            # Pre-2014 onside-kick-recovered rows ("Onside kick recovered by ...") carry the
            # label "Kickoff Return (Defense)"; normalize to the generic kickoff label so
            # they fall in kickoff_vec and get consistent kickoff handling.
            .when(pl.col("type.text") == "Kickoff Return (Defense)")
            .then(pl.lit("Kickoff"))
            .otherwise(pl.col("type.text"))
            .alias("type.text"),
        )
        play_df = (
            play_df.with_columns(
                # --- Fix Strip Sacks to Fumbles ----
                pl.when(
                    (pl.col("fumble_vec") == True)
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("change_of_poss") == 1)
                    .and_(pl.col("td_play") == False)
                    .and_(pl.col("start.down") != 4)
                    .and_(pl.col("type.text").is_in(defense_score_vec) == False)
                    # Do not sweep interception-return-fumbles into "Fumble Recovery
                    # (Opponent)": the interception already set change_of_poss=1, so the
                    # strip-sack predicate matches a pick whose returner later fumbles.
                    # ESPN's original int label is still present here (normalization to
                    # "Interception Return" happens later in this method), so guard on it.
                    .and_(pl.col("type.text").is_in(int_vec) == False),
                )
                .then(pl.lit("Fumble Recovery (Opponent)"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("fumble_vec") == True)
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("change_of_poss") == 1)
                    .and_(pl.col("td_play") == True)
                    # Same interception guard as the non-TD strip-sack rule above: a
                    # pick-six is an "Interception Return Touchdown", not a fumble TD.
                    .and_(pl.col("type.text").is_in(int_vec) == False),
                )
                .then(pl.lit("Fumble Recovery (Opponent) Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # --- Fix rushes with fumbles and a change of possession to fumbles----
                pl.when(
                    (pl.col("fumble_vec") == True)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("change_of_poss") == 1)
                    .and_(pl.col("td_play") == False)
                    .and_(pl.col("start.down") != 4)
                    .and_(pl.col("type.text").is_in(defense_score_vec) == False),
                )
                .then(pl.lit("Fumble Recovery (Opponent)"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("fumble_vec") == True)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("change_of_poss") == 1)
                    .and_(pl.col("td_play") == True),
                )
                .then(pl.lit("Fumble Recovery (Opponent) Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix kickoff fumble return TDs ----
                pl.when(
                    (pl.col("kickoff_play") == True)
                    .and_(pl.col("change_of_poss") == 1)
                    .and_(pl.col("td_play") == True)
                    .and_(pl.col("td_check") == True),
                )
                .then(pl.lit("Kickoff Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix punt return TDs ----
                pl.when((pl.col("punt_play") == True).and_(pl.col("td_play") == True).and_(pl.col("td_check") == True))
                .then(pl.lit("Punt Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix kick return TDs ----
                pl.when(
                    (pl.col("kickoff_play") == True)
                    .and_(pl.col("fumble_vec") == False)
                    .and_(pl.col("td_play") == True)
                    .and_(pl.col("td_check") == True),
                )
                .then(pl.lit("Kickoff Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix rush/pass tds that aren't explicit----
                pl.when(
                    (pl.col("td_play") == True)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("fumble_vec") == False)
                    .and_(pl.col("td_check") == True),
                )
                .then(pl.lit("Rushing Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("td_play") == True)
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("fumble_vec") == False)
                    .and_(pl.col("td_check") == True)
                    .and_(pl.col("type.text").is_in(int_vec) == False),
                )
                .then(pl.lit("Passing Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("pass") == True)
                    .and_(pl.col("type.text").is_in(["Pass Reception", "Pass Completion", "Pass"]))
                    .and_(pl.col("statYardage") == pl.col("start.yardsToEndzone"))
                    .and_(pl.col("fumble_vec") == False)
                    .and_(pl.col("type.text").is_in(int_vec) == False),
                )
                .then(pl.lit("Passing Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text").is_in(["Blocked Field Goal"])).and_(
                        pl.col("text").str.contains("(?i)for a TD"),
                    ),
                )
                .then(pl.lit("Blocked Field Goal Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when((pl.col("type.text").is_in(["Blocked Punt"])).and_(pl.col("text").str.contains("(?i)for a TD")))
                .then(pl.lit("Blocked Punt Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix blocked field goals ESPN mislabels as "Extra Point Missed" ----
                # A blocked FG returned by the defense is sometimes typed "Extra Point
                # Missed" by ESPN, which routes it through PAT-scoring EPA logic. Relabel to
                # the correct blocked-FG type (TD variant when returned for a score). Gate on
                # text showing a blocked FIELD GOAL -- "blocked" plus an FG/field-goal token --
                # so a genuine blocked/missed PAT (no FG token) is left untouched.
                pl.when(
                    (pl.col("type.text") == "Extra Point Missed")
                    .and_(pl.col("text").str.contains("(?i)blocked"))
                    .and_(pl.col("text").str.contains(r"(?i)\bfg\b|field goal"))
                    .and_((pl.col("td_play") == True).or_(pl.col("text").str.contains("(?i)for a TD"))),
                )
                .then(pl.lit("Blocked Field Goal Touchdown"))
                .when(
                    (pl.col("type.text") == "Extra Point Missed")
                    .and_(pl.col("text").str.contains("(?i)blocked"))
                    .and_(pl.col("text").str.contains(r"(?i)\bfg\b|field goal")),
                )
                .then(pl.lit("Blocked Field Goal"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix duplicated TD play_type labels----
                pl.col("type.text").str.replace(r"(?i)Touchdown Touchdown", "Touchdown").alias("type.text"),
            )
            .with_columns(
                # -- Fix Pass Interception Return TD play_type labels----
                pl.when(pl.col("text").str.contains("(?i)pass intercepted for a TD"))
                .then(pl.lit("Interception Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix Sack/Fumbles Touchdown play_type labels----
                pl.when(
                    (pl.col("text").str.contains("(?i)sacked"))
                    .and_(pl.col("text").str.contains("(?i)fumbled"))
                    .and_(pl.col("text").str.contains("(?i)TD")),
                )
                .then(pl.lit("Fumble Recovery (Opponent) Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # -- Fix generic pass plays ----
                ##-- first one looks for complete pass
                pl.when((pl.col("type.text") == "Pass").and_(pl.col("text").str.contains("(?i)pass complete")))
                .then(pl.lit("Pass Completion"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                ##-- second one looks for incomplete pass
                pl.when((pl.col("type.text") == "Pass").and_(pl.col("text").str.contains("(?i)pass incomplete")))
                .then(pl.lit("Pass Incompletion"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                ##-- third one looks for interceptions
                pl.when((pl.col("type.text") == "Pass").and_(pl.col("text").str.contains("(?i)pass intercepted")))
                .then(pl.lit("Pass Interception"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                ##-- fourth one looks for sacked
                pl.when((pl.col("type.text") == "Pass").and_(pl.col("text").str.contains("(?i)sacked")))
                .then(pl.lit("Sack"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                ##-- fifth one play type is Passing Touchdown, but its intercepted
                pl.when(
                    (pl.col("type.text") == "Passing Touchdown").and_(
                        pl.col("text").str.contains("(?i)pass intercepted for a TD"),
                    ),
                )
                .then(pl.lit("Interception Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # --- Moving non-Touchdown pass interceptions to one play_type: "Interception Return" -----
                pl.when(pl.col("type.text").is_in(["Interception", "Pass Interception", "Pass Interception Return"]))
                .then(pl.lit("Interception Return"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # --- Moving Kickoff/Punt Touchdowns without fumbles to Kickoff/Punt Return Touchdown
                pl.when((pl.col("type.text") == "Kickoff Touchdown").and_(pl.col("fumble_vec") == False))
                .then(pl.lit("Kickoff Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text") == "Kickoff")
                    .and_(pl.col("td_play") == True)
                    .and_(pl.col("fumble_vec") == False),
                )
                .then(pl.lit("Kickoff Return Touchdown"))
                .when(
                    (pl.col("type.text") == "Kickoff")
                    .and_(pl.col("text").str.contains("(?i)for a TD"))
                    .and_(pl.col("fumble_vec") == False),
                )
                .then(pl.lit("Kickoff Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text").is_in(["Kickoff", "Kickoff Return (Offense)"]))
                    .and_(pl.col("fumble_vec") == True)
                    .and_(pl.col("change_of_poss") == 1),
                )
                .then(pl.lit("Kickoff Team Fumble Recovery"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text") == "Punt Touchdown")
                    .and_(pl.col("fumble_vec") == False)
                    .and_(pl.col("change_of_poss") == 1),
                )
                .then(pl.lit("Punt Return Touchdown"))
                .when(
                    (pl.col("type.text") == "Punt")
                    .and_(pl.col("text").str.contains("(?i)for a TD"))
                    .and_(pl.col("change_of_poss") == 1),
                )
                .then(pl.lit("Punt Return Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text") == "Punt")
                    .and_(pl.col("fumble_vec") == True)
                    .and_(pl.col("change_of_poss") == 0),
                )
                .then(pl.lit("Punt Team Fumble Recovery"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(pl.col("type.text").is_in(["Punt Touchdown"]))
                .then(pl.lit("Punt Team Fumble Recovery Touchdown"))
                .when(
                    (pl.col("scoringPlay") == True)
                    .and_(pl.col("punt_play") == True)
                    .and_(pl.col("change_of_poss") == 0),
                )
                .then(pl.lit("Punt Team Fumble Recovery Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(pl.col("type.text").is_in(["Kickoff Touchdown"]))
                .then(pl.lit("Kickoff Team Fumble Recovery Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text").is_in(["Fumble Return Touchdown"])).and_(
                        (pl.col("pass") == True).or_(pl.col("rush") == True),
                    ),
                )
                .then(pl.lit("Fumble Recovery (Opponent) Touchdown"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                # --- Safeties (kickoff, punt, penalty) ----
                pl.when(
                    (pl.col("type.text").is_in(["Pass Reception", "Rush", "Rushing Touchdown"]))
                    .and_((pl.col("pass") == True).or_(pl.col("rush") == True))
                    .and_(pl.col("safety") == True),
                )
                .then(pl.lit("Safety"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(pl.col("kickoff_safety") == True)
                .then(pl.lit("Kickoff (Safety)"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(pl.col("punt_safety") == True)
                .then(pl.lit("Punt (Safety)"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(pl.col("penalty_safety") == True)
                .then(pl.lit("Penalty (Safety)"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when((pl.col("type.text") == "Extra Point Good").and_(pl.col("text").str.contains("(?i)Two-Point")))
                .then(pl.lit("Two-Point Conversion Good"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
            .with_columns(
                pl.when(
                    (pl.col("type.text") == "Extra Point Missed").and_(pl.col("text").str.contains("(?i)Two-Point")),
                )
                .then(pl.lit("Two-Point Conversion Missed"))
                .otherwise(pl.col("type.text"))
                .alias("type.text"),
            )
        )

        # --- Normalize separate extra-point rows to the no-down sentinel ----
        # Pre-2005 games sometimes carry a real down/distance on the separate
        # extra-point rows (2005+ already use -1, and two-point rows already use -1
        # in every era). Force the sentinel so these no-down scoring plays stay out
        # of down-based logic consistently. Extra-point rows are scrimmage_play=False,
        # so this never touches scrimmage aggregates; and modern games have no
        # separate extra-point rows, so this is a strictly pre-2005 normalization.
        _pat_set = ["Extra Point Good", "Extra Point Missed"]
        play_df = play_df.with_columns(
            pl.when(pl.col("type.text").is_in(_pat_set).and_(pl.col("start.down") >= 0))
            .then(pl.lit(-1))
            .otherwise(pl.col("start.down"))
            .alias("start.down"),
            pl.when(pl.col("type.text").is_in(_pat_set).and_(pl.col("start.distance") >= 0))
            .then(pl.lit(-1))
            .otherwise(pl.col("start.distance"))
            .alias("start.distance"),
        )

        return play_df

    def __setup_penalty_data(self, play_df):
        """
        Creates the following columns in play_df:
            * Penalty flag
            * Penalty declined
            * Penalty no play
            * Penalty off-set
            * Penalty 1st down conversion
            * Penalty in text
            * Yds Penalty
        """
        ##-- 'Penalty' in play text ----
        play_df = (
            play_df.with_columns(
                # -- T/F flag conditions penalty_flag
                penalty_flag=pl.when((pl.col("type.text") == "Penalty").or_(pl.col("text").str.contains("(?i)penalty")))
                .then(True)
                .otherwise(False),
                # -- T/F flag conditions penalty_declined
                # The second branch is NOT redundant: ESPN labels most penalty
                # plays as "Penalty", but writes plenty of them onto a normally
                # labelled play ("Pass Incompletion", "Rush"). Gating only on
                # the play label missed 576 of 894 "declined" texts in 2025.
                penalty_declined=pl.when(
                    (pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains("(?i)declined")),
                )
                .then(True)
                .when((pl.col("text").str.contains("(?i)penalty")).and_(pl.col("text").str.contains("(?i)declined")))
                .then(True)
                .otherwise(False),
                # -- T/F flag conditions penalty_no_play
                # "nullified by penalty" is ESPN's own verdict that the play did
                # not count, and it is the RELIABLE signal. Deriving negation
                # ourselves from the infraction name is not safe: a play can
                # carry two fouls on the same side where one is declined and the
                # other accepted (so "declined" in the text does NOT mean the
                # play stood), or a live-ball foul plus a dead-ball foul that are
                # BOTH accepted. ESPN's marker already reflects the net
                # enforcement outcome. 179 plays carry it in 2025; 26 of them
                # say nothing about "no play".
                penalty_no_play=pl.when(
                    (pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains(_PENALTY_NEGATED_TEXT)),
                )
                .then(True)
                .when(
                    (pl.col("text").str.contains("(?i)penalty")).and_(
                        pl.col("text").str.contains(_PENALTY_NEGATED_TEXT)
                    )
                )
                .then(True)
                .otherwise(False),
                # -- T/F flag conditions penalty_offset
                # ESPN spells this BOTH ways and overwhelmingly without the
                # hyphen: 44 "offsetting" vs 1 "off-setting" in 2025, of which
                # the hyphen-only pattern matched exactly one.
                penalty_offset=pl.when(
                    (pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains("(?i)off-?setting")),
                )
                .then(True)
                .when(
                    (pl.col("text").str.contains("(?i)penalty")).and_(pl.col("text").str.contains("(?i)off-?setting"))
                )
                .then(True)
                .otherwise(False),
                # -- T/F flag conditions penalty_1st_conv
                penalty_1st_conv=pl.when(
                    (pl.col("type.text") == "Penalty").and_(pl.col("text").str.contains("(?i)1st down")),
                )
                .then(True)
                .when((pl.col("text").str.contains("(?i)penalty")).and_(pl.col("text").str.contains("(?i)1st down")))
                .then(True)
                .otherwise(False),
                # -- T/F flag for penalty text but not penalty play type --
                penalty_in_text=pl.when(
                    (pl.col("text").str.contains("(?i)penalty")).and_(
                        pl.col("type.text") != "Penalty",
                        pl.col("text").str.contains("(?i)declined") == False,
                        pl.col("text").str.contains("(?i)off-?setting") == False,
                        pl.col("text").str.contains(_PENALTY_NEGATED_TEXT) == False,
                    ),
                )
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                # ---- per-penalty state -------------------------------------
                # A play can carry more than one penalty (398 plays across
                # 2015/2021/2025), and a single boolean cannot represent that.
                # 54 of those have SOME but not all penalties declined -- their
                # down-replay rate is 0.231 against 0.813 when none are
                # declined, so the two groups genuinely differ and collapsing
                # them loses information.
                penalty_count=pl.col("text").str.count_matches("(?i)penalty"),
                penalty_declined_count=pl.col("text").str.count_matches("(?i)declined"),
            )
            .with_columns(
                # Only when EVERY penalty was declined does the play stand.
                # `penalty_declined` alone (>=1 declined) is not sufficient.
                penalty_all_declined=(pl.col("penalty_count") > 0)
                & (pl.col("penalty_count") == pl.col("penalty_declined_count")),
            )
            .with_columns(
                # ---- enforcement class, resolved in priority order ---------
                # `unknown` is a real answer, not a gap to be filled: the
                # automatic-first-down fouls cannot be classified from the
                # signals available (see _PENALTY_AUTO_FIRST_DOWN_FOUL).
                penalty_enforcement=pl.when(pl.col("penalty_flag") == False)
                .then(pl.lit(None, dtype=pl.Utf8))
                .when(pl.col("penalty_no_play") == True)
                .then(pl.lit("no_play"))
                .when(pl.col("penalty_all_declined") == True)
                .then(pl.lit("declined"))
                .when(pl.col("penalty_offset") == True)
                .then(pl.lit("offsetting"))
                .when(pl.col("text").str.contains(_PENALTY_AUTO_FIRST_DOWN_FOUL))
                .then(pl.lit("unknown"))
                .when(pl.col("text").str.contains(_PENALTY_NEGATING_FOUL))
                .then(pl.lit("negating_foul"))
                .when(pl.col("text").str.contains(_PENALTY_PLAY_STANDS_FOUL))
                .then(pl.lit("play_stands"))
                .otherwise(pl.lit("unknown")),
            )
            .with_columns(
                # The consumer-facing answer for the classes we can settle.
                # NULL (not False) for `unknown`, so a caller cannot silently
                # treat "we do not know" as "the play counted".
                penalty_negated_play=pl.when(pl.col("penalty_enforcement").is_null())
                .then(False)
                .when(pl.col("penalty_enforcement").is_in(["no_play", "offsetting", "negating_foul"]))
                .then(True)
                .when(pl.col("penalty_enforcement").is_in(["declined", "play_stands"]))
                .then(False)
                .otherwise(pl.lit(None, dtype=pl.Boolean)),
            )
            .with_columns(
                # Foul-name branches FIRST: a declined/offset penalty keeps its
                # foul name (the disposition already lives in penalty_declined /
                # penalty_offset), instead of the label swallowing it -- the
                # 2025-season taxonomy measured 318 "Declined" + 844 "Missing"
                # rows whose foul names were recoverable from text.
                penalty_detail=pl.when(pl.col("text").str.contains("(?i)roughing (?:the )?passer"))
                .then(pl.lit("Roughing the Passer"))
                .when(pl.col("text").str.contains("(?i)offensive holding"))
                .then(pl.lit("Offensive Holding"))
                # inter?ference: ESPN ships a literal "Inteference" typo
                .when(pl.col("text").str.contains("(?i)pass inter?ference"))
                .then(pl.lit("Pass Interference"))
                .when(pl.col("text").str.contains("(?i)encroachment"))
                .then(pl.lit("Encroachment"))
                .when(pl.col("text").str.contains("(?i)defensive pass inter?ference"))
                .then(pl.lit("Defensive Pass Interference"))
                .when(pl.col("text").str.contains("(?i)offensive pass inter?ference"))
                .then(pl.lit("Offensive Pass Interference"))
                .when(pl.col("text").str.contains("(?i)illegal procedure"))
                .then(pl.lit("Illegal Procedure"))
                .when(pl.col("text").str.contains("(?i)defensive holding"))
                .then(pl.lit("Defensive Holding"))
                .when(pl.col("text").str.contains("(?i)holding"))
                .then(pl.lit("Holding"))
                .when(pl.col("text").str.contains("(?i)offensive off-?side|(?i)off-?side offense"))
                .then(pl.lit("Offensive Offside"))
                .when(pl.col("text").str.contains("(?i)defensive off-?side|(?i)off-?side defense"))
                .then(pl.lit("Defensive Offside"))
                # off-?side: hyphenated vendor spelling ("off-side") observed 44x in 2025
                .when(pl.col("text").str.contains("(?i)off-?side"))
                .then(pl.lit("Offside"))
                .when(pl.col("text").str.contains("(?i)(?:illegal|invalid) fair catch signal"))
                .then(pl.lit("Illegal Fair Catch Signal"))
                .when(pl.col("text").str.contains(r"(?i)illegal bat(?:ting)?\b"))
                .then(pl.lit("Illegal Batting"))
                .when(pl.col("text").str.contains("(?i)neutral zone infraction"))
                .then(pl.lit("Neutral Zone Infraction"))
                # inelgible: another literal vendor typo
                .when(pl.col("text").str.contains("(?i)inel[ei]gible downfield|(?i)inelgible downfield"))
                .then(pl.lit("Ineligible Downfield"))
                .when(pl.col("text").str.contains("(?i)illegal use of hands"))
                .then(pl.lit("Illegal Use of Hands"))
                .when(pl.col("text").str.contains("(?i)kickoff out of bounds|(?i)kickoff out-of-bounds"))
                .then(pl.lit("Kickoff Out of Bounds"))
                .when(pl.col("text").str.contains("(?i)12 men on the field"))
                .then(pl.lit("12 Men on the Field"))
                .when(pl.col("text").str.contains("(?i)block(?:ing)? below (?:the )?waist"))
                .then(pl.lit("Block Below the Waist"))
                .when(pl.col("text").str.contains("(?i)chop block"))
                .then(pl.lit("Chop Block"))
                .when(pl.col("text").str.contains("(?i)illegal block|(?i)low block"))
                .then(pl.lit("Illegal Block"))
                .when(pl.col("text").str.contains("(?i)personal foul"))
                .then(pl.lit("Personal Foul"))
                .when(pl.col("text").str.contains("(?i)false start"))
                .then(pl.lit("False Start"))
                .when(pl.col("text").str.contains("(?i)substitution infraction|(?i)illegal substitution"))
                .then(pl.lit("Substitution Infraction"))
                .when(pl.col("text").str.contains("(?i)illegal formation"))
                .then(pl.lit("Illegal Formation"))
                # prefix covers "Illegal Touching" / "Illegal Touch Pass" / "Illegal Touch-Pass"
                .when(pl.col("text").str.contains("(?i)illegal touch"))
                .then(pl.lit("Illegal Touching"))
                .when(pl.col("text").str.contains("(?i)sideline inter?ference"))
                .then(pl.lit("Sideline Interference"))
                .when(pl.col("text").str.contains("(?i)clipping"))
                .then(pl.lit("Clipping"))
                .when(pl.col("text").str.contains("(?i)sideline infraction"))
                .then(pl.lit("Sideline Infraction"))
                .when(pl.col("text").str.contains("(?i)crackback"))
                .then(pl.lit("Crackback"))
                .when(pl.col("text").str.contains("(?i)illegal snap"))
                .then(pl.lit("Illegal Snap"))
                .when(pl.col("text").str.contains("(?i)illegal helmet contact"))
                .then(pl.lit("Illegal Helmet Contact"))
                .when(pl.col("text").str.contains("(?i)roughing holder"))
                .then(pl.lit("Roughing the Holder"))
                .when(pl.col("text").str.contains("(?i)horse collar tackle"))
                .then(pl.lit("Horse Collar Tackle"))
                .when(pl.col("text").str.contains("(?i)illegal participation"))
                .then(pl.lit("Illegal Participation"))
                .when(pl.col("text").str.contains("(?i)tripping"))
                .then(pl.lit("Tripping"))
                .when(pl.col("text").str.contains("(?i)illegal shift"))
                .then(pl.lit("Illegal Shift"))
                .when(pl.col("text").str.contains("(?i)illegal motion"))
                .then(pl.lit("Illegal Motion"))
                .when(pl.col("text").str.contains("(?i)roughing (?:the )?kicker"))
                .then(pl.lit("Roughing the Kicker"))
                .when(pl.col("text").str.contains("(?i)delay of game"))
                .then(pl.lit("Delay of Game"))
                .when(pl.col("text").str.contains("(?i)targeting"))
                .then(pl.lit("Targeting"))
                .when(pl.col("text").str.contains("(?i)face mask"))
                .then(pl.lit("Face Mask"))
                .when(pl.col("text").str.contains("(?i)illegal forward pass"))
                .then(pl.lit("Illegal Forward Pass"))
                .when(pl.col("text").str.contains("(?i)intentional grounding"))
                .then(pl.lit("Intentional Grounding"))
                .when(pl.col("text").str.contains("(?i)illegal kicking"))
                .then(pl.lit("Illegal Kicking"))
                .when(pl.col("text").str.contains("(?i)illegal conduct"))
                .then(pl.lit("Illegal Conduct"))
                .when(pl.col("text").str.contains("(?i)kick catching interference"))
                .then(pl.lit("Kick Catch Interference"))
                .when(pl.col("text").str.contains("(?i)kick catch interference"))
                .then(pl.lit("Kick Catch Interference"))
                .when(pl.col("text").str.contains("(?i)unnecessary roughness"))
                .then(pl.lit("Unnecessary Roughness"))
                .when(pl.col("text").str.contains("(?i)Penalty, UR"))
                .then(pl.lit("Unnecessary Roughness"))
                .when(pl.col("text").str.contains("(?i)roughing the snapper"))
                .then(pl.lit("Roughing the Snapper"))
                .when(pl.col("text").str.contains("(?i)illegal blindside block"))
                .then(pl.lit("Illegal Blindside Block"))
                .when(pl.col("text").str.contains("(?i)unsportsmanlike conduct"))
                .then(pl.lit("Unsportsmanlike Conduct"))
                .when(pl.col("text").str.contains("(?i)running into (?:the )?kicker"))
                .then(pl.lit("Running Into Kicker"))
                .when(pl.col("text").str.contains("(?i)failure to wear required equipment"))
                .then(pl.lit("Failure to Wear Required Equipment"))
                .when(pl.col("text").str.contains("(?i)player disqualification"))
                .then(pl.lit("Player Disqualification"))
                .when(pl.col("text").str.contains("(?i)disconcerting"))
                .then(pl.lit("Disconcerting Signals"))
                .when(pl.col("text").str.contains(r"(?i)\bleaping\b"))
                .then(pl.lit("Leaping"))
                # disposition-only labels LAST: they fire only when no foul name
                # was recognizable in the text
                .when(pl.col("penalty_offset") == 1)
                .then(pl.lit("Offsetting"))
                .when(pl.col("penalty_declined") == 1)
                .then(pl.lit("Declined"))
                .when(pl.col("penalty_flag") == True)
                .then(pl.lit("Missing")),
            )
            .with_columns(
                penalty_text=pl.when(pl.col("penalty_flag") == True)
                .then(pl.col("text").str.extract(r"(?i)Penalty(.+)", 1))
                .otherwise(None),
            )
            .with_columns(
                # Direct signed-integer capture. The prior fragment capture
                # (".{0,3} yards") produced garbage on 40% of rows measured
                # against 2004-2025 ('U (', 'k 8'); a numeric grab cannot.
                yds_penalty=pl.when(pl.col("penalty_flag") == True)
                .then(pl.col("penalty_text").str.extract(r"\(?(-?\d{1,3})\)?\s*(?i:yards?|yds?)\b", 1))
                .otherwise(None),
            )
            .with_columns(
                yds_penalty=pl.when(
                    (pl.col("penalty_flag") == True).and_(
                        pl.col("yds_penalty").is_null(),
                        pl.col("text").str.contains(r"(?i)ards\)"),
                    ),
                )
                .then(
                    # The parenthesised form, "(15 Yards)". The prior expression here
                    # was the same ".{0,4} before yards" fragment capture the comment
                    # above records as producing garbage, and its alternation was
                    # mis-grouped besides -- group 1 exists only in the first branch, so
                    # the other three always returned null. Grab the number directly.
                    pl.col("text").str.extract(r"\((-?\d{1,3})\s*(?i:yards?|yds?)\)", 1)
                )
                .otherwise(pl.col("yds_penalty")),
            )
        )

        return play_df

    def __add_play_category_flags(self, play_df):
        play_df = (
            play_df.with_columns(
                # --- Sacks -----
                sack=pl.when(pl.col("type.text").is_in(["Sack"]))
                .then(True)
                .when(
                    (
                        pl.col("type.text").is_in(
                            [
                                "Fumble Recovery (Opponent)",
                                "Fumble Recovery (Opponent) Touchdown",
                                "Fumble Recovery (Own)",
                                "Fumble Recovery (Own) Touchdown",
                            ],
                        )
                    )
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("text").str.contains("(?i)sacked")),
                )
                .then(True)
                .when((pl.col("type.text").is_in(["Safety"])).and_(pl.col("text").str.contains("(?i)sacked")))
                .then(True)
                .otherwise(False),
                # --- Interceptions ------
                int=pl.col("type.text").is_in(["Interception Return", "Interception Return Touchdown"]),
                int_td=pl.col("type.text").is_in(["Interception Return Touchdown"]),
                # --- Pass Completions, Attempts and Targets -------
                completion=pl.when(
                    pl.col("type.text").is_in(["Pass Reception", "Pass Completion", "Passing Touchdown"]),
                )
                .then(True)
                # A completion on a penalty play COUNTS when the play stood
                # (penalty_no_play False). ESPN's box includes these; measured
                # against the team box this is a +0.1 to +0.4pp monotone gain
                # in every season, never negative. Nullified plays stay
                # excluded -- counting those is catastrophic in every season.
                .when(
                    (pl.col("type.text") == "Penalty")
                    .and_(pl.col("text").str.contains(r"(?i)pass complete"))
                    .and_(pl.col("penalty_no_play") == False),
                )
                .then(True)
                .when(
                    (
                        pl.col("type.text").is_in(
                            [
                                "Fumble Recovery (Opponent)",
                                "Fumble Recovery (Opponent) Touchdown",
                                "Fumble Recovery (Own)",
                                "Fumble Recovery (Own) Touchdown",
                            ],
                        )
                    )
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("text").str.contains("(?i)sacked") == False),
                )
                .then(True)
                .otherwise(False),
                pass_attempt=pl.when(
                    pl.col("type.text").is_in(
                        ["Pass Reception", "Pass Completion", "Passing Touchdown", "Pass Incompletion"],
                    ),
                )
                .then(True)
                .when(
                    (
                        pl.col("type.text").is_in(
                            [
                                "Fumble Recovery (Opponent)",
                                "Fumble Recovery (Opponent) Touchdown",
                                "Fumble Recovery (Own)",
                                "Fumble Recovery (Own) Touchdown",
                            ],
                        )
                    )
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("text").str.contains("(?i)sacked") == False),
                )
                .then(True)
                .when((pl.col("pass") == True).and_(pl.col("text").str.contains("(?i)sacked") == False))
                .then(True)
                .otherwise(False),
                target=pl.when(
                    pl.col("type.text").is_in(
                        ["Pass Reception", "Pass Completion", "Passing Touchdown", "Pass Incompletion"],
                    ),
                )
                .then(True)
                .when(
                    (
                        pl.col("type.text").is_in(
                            [
                                "Fumble Recovery (Opponent)",
                                "Fumble Recovery (Opponent) Touchdown",
                                "Fumble Recovery (Own)",
                                "Fumble Recovery (Own) Touchdown",
                            ],
                        )
                    )
                    .and_(pl.col("pass") == True)
                    .and_(pl.col("text").str.contains("(?i)sacked") == False),
                )
                .then(True)
                .when((pl.col("pass") == True).and_(pl.col("text").str.contains("(?i)sacked") == False))
                .then(True)
                .otherwise(False),
                pass_breakup=pl.when(pl.col("text").str.contains("(?i)broken up by")).then(True).otherwise(False),
                # --- Pass/Rush TDs ------
                # ESPN keeps the "Passing Touchdown" / "Rushing Touchdown" label
                # on a play it also says was wiped out, so gating `td_play`
                # alone is not enough -- the label branch below would still fire.
                # Measured on 2025: 15 `pass_td` and 15 `rush_td` on negated
                # plays, every one of them reaching a player leaderboard, where
                # `summarize_passer` sums `pass_td` straight into `passing_td`.
                pass_td=pl.when(pl.col("text").str.contains(_PENALTY_NEGATED_TEXT))
                .then(False)
                .when(pl.col("type.text").is_in(["Passing Touchdown"]))
                .then(True)
                .when((pl.col("pass") == True).and_(pl.col("td_play") == True))
                .then(True)
                .otherwise(False),
                rush_td=pl.when(pl.col("text").str.contains(_PENALTY_NEGATED_TEXT))
                .then(False)
                .when(pl.col("type.text").is_in(["Rushing Touchdown"]))
                .then(True)
                .when((pl.col("rush") == True).and_(pl.col("td_play") == True))
                .then(True)
                .otherwise(False),
                # --- Pass depth/direction + rush direction (Game on Paper matrix fields) ---
                # Extracted from ESPN play description text; null when the pattern is absent
                # (sacks, screens, and pre-2025 plays that omit depth/direction).
                # Depth: "short" (0-12 air yards) | "deep" (12+ air yards)
                # Direction: "left" | "middle" | "right"
                pass_depth=pl.when(pl.col("pass") == True)
                .then(pl.col("text").str.extract(r"\s(short|deep)\s", 1))
                .otherwise(None),
                pass_direction=pl.when(pl.col("pass") == True)
                .then(pl.col("text").str.extract(r"\s(left|middle|right)\s", 1))
                .otherwise(None),
                rush_direction=pl.when(pl.col("rush") == True)
                .then(pl.col("text").str.extract(r"\s(left|middle|right)\s", 1))
                .otherwise(None),
                # QB pressured into a hurried throw -- ESPN appends "QB hurried by
                # #NN X.Name" to the play text. A whitespace-bounded flag (null text
                # -> False) so it stays a clean boolean for downstream filters/models.
                qb_hurry=pl.col("text").str.contains(r"(?i)\shurried by\s").fill_null(False),
                # --- Change of possession via turnover
                turnover_vec=pl.col("type.text").is_in(turnover_vec),
                offense_score_play=pl.col("type.text").is_in(offense_score_vec),
                defense_score_play=pl.col("type.text").is_in(defense_score_vec),
                downs_turnover=pl.when(
                    (pl.col("type.text").is_in(normalplay))
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4)
                    .and_(pl.col("penalty_1st_conv") == False),
                )
                .then(True)
                .otherwise(False),
                # --- Touchdowns ----
                scoring_play=pl.col("type.text").is_in(scores_vec),
                yds_punted=pl.col("text").str.extract(r"(?i)(punt for \d+)").str.extract(r"(\d+)").cast(pl.Int32),
                yds_punt_gained=pl.when(pl.col("punt") == True).then(pl.col("statYardage")).otherwise(None),
                fg_attempt=pl.when(
                    (pl.col("type.text").str.contains(r"(?i)Field Goal")).or_(
                        pl.col("text").str.contains(r"(?i)Field Goal"),
                    ),
                )
                .then(True)
                .otherwise(False),
                fg_made=pl.col("type.text") == "Field Goal Good",
                yds_fg=pl.col("text")
                .str.extract(
                    r"(?i)(\d+)\s?Yd Field|(?i)(\d+)\s?YD FG|(?i)(\d+)\s?Yard FG|(?i)(\d+)\s?Field|(?i)(\d+)\s?Yard Field",
                    0,
                )
                .str.extract(r"(\d+)")
                .cast(pl.Int32),
            )
            .with_columns(
                pl.when(pl.col("fg_attempt") == True)
                .then(pl.col("yds_fg") - 17)
                .otherwise(pl.col("start.yardsToEndzone"))
                .alias("start.yardsToEndzone"),
            )
            .with_columns(
                pl.when(
                    (pl.col("start.yardsToEndzone").is_null())
                    .and_(pl.col("type.text").is_in(kickoff_vec) == False)
                    .and_(pl.col("start.pos_team.id") == pl.col("homeTeamId")),
                )
                .then(100 - pl.col("start.yardLine").cast(pl.Int32))
                .when(
                    (pl.col("start.yardsToEndzone").is_null())
                    .and_(pl.col("type.text").is_in(kickoff_vec) == False)
                    .and_(pl.col("start.pos_team.id") == pl.col("awayTeamId")),
                )
                .then(pl.col("start.yardLine").cast(pl.Int32))
                .otherwise(pl.col("start.yardsToEndzone"))
                .alias("start.yardsToEndzone"),
            )
            .with_columns(
                pos_unit=pl.when(pl.col("punt") == True)
                .then(pl.lit("Punt Offense"))
                .when(pl.col("kickoff_play") == True)
                .then(pl.lit("Kickoff Return"))
                .when(pl.col("fg_attempt") == True)
                .then(pl.lit("Field Goal Offense"))
                .when(pl.col("type.text") == "Defensive 2pt Conversion")
                .then(pl.lit("Offense"))
                .otherwise(pl.lit("Offense")),
                def_pos_unit=pl.when(pl.col("punt") == True)
                .then(pl.lit("Punt Return"))
                .when(pl.col("kickoff_play") == True)
                .then(pl.lit("Kickoff Defense"))
                .when(pl.col("fg_attempt") == True)
                .then(pl.lit("Field Goal Defense"))
                .when(pl.col("type.text") == "Defensive 2pt Conversion")
                .then(pl.lit("Defense"))
                .otherwise(pl.lit("Defense")),
                # --- Lags/Leads play type ----
                lead_play_type=pl.col("type.text").shift(-1),
                sp=pl.when(
                    (pl.col("fg_attempt") == True).or_(pl.col("punt") == True).or_(pl.col("kickoff_play") == True),
                )
                .then(True)
                .otherwise(False),
                play=pl.when(pl.col("type.text").is_in(["Timeout", "End Period", "End of Half", "Penalty"]) == False)
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                # Presentational-token-stripped play text (leading game clock, the
                # No-Huddle/Shotgun formation tags, and the depth/direction modifiers
                # short|deep|left|middle|right) so verb-anchored parsers match modern
                # ESPN phrasing -- e.g. "rush middle for 5 yards" -> "rush for 5 yards".
                # Without this, main's `rush for`/`complete to` extractors miss every
                # play that carries a direction word. Ported from cfbfastR-cfb-data
                # 0.36-live. pass_direction/rush_direction still read raw `text`.
                cleaned_text=(
                    pl.col("text")
                    .str.replace(r"^\(\d{1,2}:\d{2}\)\s*", "")
                    .str.replace_all(r"(?i)\s*No Huddle-Shotgun\s+", " ")
                    .str.replace_all(r"(?i)No Huddle-", "")
                    .str.replace_all(r"(?i)\s*Shotgun\s+", " ")
                    # Two passes: depth ("short"/"deep") then direction. ESPN stacks
                    # them ("complete short right to"); a single combined pass would
                    # consume the shared space and strip only the first modifier.
                    .str.replace_all(r"(?i) (short|deep) ", " ")
                    .str.replace_all(r"(?i) (left|middle|right) ", " ")
                    .str.replace_all(r"\s+", " ")
                    .str.strip_chars()
                ),
            )
            .with_columns(
                # Kneel-down detection (ported 0.36-live). Guards first (special
                # teams, non-scrimmage type, pass) so the text matches only apply to
                # genuine offensive kneels; then explicit "kneel"/"takes a knee", and
                # finally an end-of-half/-game TEAM-rush-for-(-1/-2) heuristic for
                # plays ESPN anonymizes without the word "kneel".
                kneel_down=pl.when(pl.col("sp") == True)
                .then(False)
                .when(
                    pl.col("type.text").is_in(
                        [
                            "Timeout",
                            "Extra Point Good",
                            "Extra Point Missed",
                            "Two-Point Pass",
                            "Two-Point Rush",
                            "Penalty",
                        ],
                    ),
                )
                .then(False)
                .when(pl.col("pass") == True)
                .then(False)
                .when(pl.col("cleaned_text").str.contains(r"(?i)kneel"))
                .then(True)
                .when(pl.col("cleaned_text").str.contains(r"(?i)takes a knee"))
                .then(True)
                .when(
                    (
                        ((pl.col("start.adj_TimeSecsRem") <= 1860).and_(pl.col("start.adj_TimeSecsRem") >= 1800)).or_(
                            (pl.col("start.adj_TimeSecsRem") <= 60).and_(pl.col("start.adj_TimeSecsRem") >= 0),
                        )
                    ).and_(
                        pl.col("cleaned_text").str.contains(r"(?i)^team run for a loss of (?:1 yard|2 yards)"),
                    ),
                )
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                scrimmage_play=pl.when(
                    (pl.col("sp") == False)
                    .and_(pl.col("kneel_down") == False)
                    .and_(
                        pl.col("type.text").is_in(
                            [
                                "Timeout",
                                "Extra Point Good",
                                "Extra Point Missed",
                                "Two-Point Pass",
                                "Two-Point Rush",
                                "Penalty",
                            ],
                        )
                        == False,
                    ),
                )
                .then(True)
                .otherwise(False),
                # --- Change of pos_team by lead('pos_team', 1)----
                change_of_pos_team=pl.when(
                    (pl.col("pos_team") == pl.col("lead_pos_team")).and_(
                        ((pl.col("lead_play_type").is_in(["End Period", "End of Half"])) == False).or_(
                            pl.col("lead_play_type").is_null(),
                        ),
                    ),
                )
                .then(False)
                .when(
                    (pl.col("pos_team") == pl.col("lead_pos_team2")).and_(
                        (pl.col("lead_play_type").is_in(["End Period", "End of Half"])).or_(
                            pl.col("lead_play_type").is_null(),
                        ),
                    ),
                )
                .then(False)
                .otherwise(True),
            )
            .with_columns(
                change_of_pos_team=pl.when(pl.col("change_of_poss").is_null())
                .then(False)
                .otherwise(pl.col("change_of_pos_team")),
                pos_score_diff_end=pl.when(
                    (
                        (pl.col("type.text").is_in(end_change_vec)).and_(
                            pl.col("start.pos_team.id") != pl.col("end.pos_team.id"),
                        )
                    ).or_(pl.col("downs_turnover") == True),
                )
                .then(-1 * pl.col("pos_score_diff"))
                .otherwise(pl.col("pos_score_diff")),
            )
            .with_columns(
                pos_score_diff_end=pl.when(
                    (pl.col("pos_score_pts").abs() >= 8)
                    .and_(pl.col("scoring_play") == False)
                    .and_(pl.col("change_of_pos_team") == False),
                )
                .then(pl.col("pos_score_diff_start"))
                .when(
                    (pl.col("pos_score_pts").abs() >= 8)
                    .and_(pl.col("scoring_play") == False)
                    .and_(pl.col("change_of_pos_team") == True),
                )
                .then(-1 * pl.col("pos_score_diff_start"))
                .otherwise(pl.col("pos_score_diff_end")),
                fumble_lost=pl.when((pl.col("fumble_vec") == True).and_(pl.col("change_of_pos_team") == True))
                .then(True)
                .otherwise(False),
                fumble_recovered=pl.when((pl.col("fumble_vec") == True).and_(pl.col("change_of_pos_team") == False))
                .then(True)
                .otherwise(False),
            )
        )

        # --- nflfastR-compatible scoring event result columns ---
        # field_goal_result is always derivable; extra_point_result and
        # two_point_conv_result require pointAfterAttempt.* from the ESPN API
        # (present on TD plays in modern data; absent for very old seasons).
        scoring_exprs: list = [
            pl.when(pl.col("fg_attempt") == True)
            .then(
                pl.when(pl.col("fg_made") == True)
                .then(pl.lit("made"))
                .when(pl.col("type.text").str.contains(r"(?i)blocked"))
                .then(pl.lit("blocked"))
                .otherwise(pl.lit("missed"))
            )
            .otherwise(None)
            .alias("field_goal_result"),
        ]
        if "pointAfterAttempt.abbreviation" in play_df.columns and "pointAfterAttempt.value" in play_df.columns:
            scoring_exprs += [
                # extra_point_result: "good" | "blocked" | "failed" | null (non-TD plays)
                pl.when(pl.col("pointAfterAttempt.abbreviation").str.contains(r"(?i)extra point"))
                .then(
                    pl.when(pl.col("pointAfterAttempt.value") == 1.0)
                    .then(pl.lit("good"))
                    .when(pl.col("pointAfterAttempt.abbreviation").str.contains(r"(?i)block"))
                    .then(pl.lit("blocked"))
                    .otherwise(pl.lit("failed"))
                )
                .otherwise(None)
                .alias("extra_point_result"),
                # two_point_conv_result: "success" | "failure" | null (non-TD plays)
                pl.when(pl.col("pointAfterAttempt.abbreviation").str.contains(r"(?i)two.?point"))
                .then(
                    pl.when(pl.col("pointAfterAttempt.value") == 2.0)
                    .then(pl.lit("success"))
                    .otherwise(pl.lit("failure"))
                )
                .otherwise(None)
                .alias("two_point_conv_result"),
            ]
        play_df = play_df.with_columns(scoring_exprs)

        return play_df

    def __add_yardage_cols(self, play_df):
        # A5 (0.36-live): some ESPN feeds (the 2024 Week 1 batch) report
        # statYardage==0 on completions that actually gained yards (the text carries
        # no "for N yards"). Rebuild the yardage from the yardline delta -- on RAW
        # yardlines, since this runs before __process_epa clamps end-of-half to 99 --
        # using ``start - (100 - end)`` when the play flipped possession (cross-team)
        # and ``start - end`` otherwise. Gated on the ``completion`` flag (NOT a
        # "complete to" text match -- that substring also matches "incomplete to" and
        # would wrongly rebuild incompletions), and on non-penalty plays
        # (penalty_detail null) so the penalty-residual chain below -- which only fires
        # when penalty_detail is set -- is untouched. Completions are never
        # interceptions, so A3's interception handling is also unaffected.
        play_df = play_df.with_columns(
            statYardage=pl.when(
                (pl.col("completion") == True)
                .and_(pl.col("statYardage") == 0)
                .and_(pl.col("penalty_detail").is_null())
                .and_(pl.col("start.team.id") != pl.col("end.team.id")),
            )
            .then((pl.col("start.yardsToEndzone") - (100 - pl.col("end.yardsToEndzone"))).cast(pl.Int64))
            .when(
                (pl.col("completion") == True)
                .and_(pl.col("statYardage") == 0)
                .and_(pl.col("penalty_detail").is_null()),
            )
            .then((pl.col("start.yardsToEndzone") - pl.col("end.yardsToEndzone")).cast(pl.Int64))
            .otherwise(pl.col("statYardage")),
        )
        play_df = play_df.with_columns(
            # Rush yardage reads cleaned_text (direction word stripped) so
            # "rush middle for 5 yards" -> "rush for 5 yards" matches; raw `text`
            # silently missed every direction-carrying rush on modern ESPN feeds.
            yds_rushed=pl.when(
                (pl.col("rush") == True).and_(pl.col("cleaned_text").str.contains("(?i)run for no gain"))
            )
            .then(0)
            .when((pl.col("rush") == True).and_(pl.col("cleaned_text").str.contains("(?i)for no gain")))
            .then(0)
            .when((pl.col("rush") == True).and_(pl.col("cleaned_text").str.contains("(?i)run for a loss of")))
            .then(-1 * pl.col("cleaned_text").str.extract(r"(?i)run for a loss of (\d+)").cast(pl.Int32))
            .when((pl.col("rush") == True).and_(pl.col("cleaned_text").str.contains("(?i)rush for a loss of")))
            .then(-1 * pl.col("cleaned_text").str.extract(r"(?i)rush for a loss of (\d+)").cast(pl.Int32))
            .when((pl.col("rush") == True).and_(pl.col("cleaned_text").str.contains("(?i)run for")))
            .then(pl.col("cleaned_text").str.extract(r"(?i)run for (\d+)").cast(pl.Int32))
            .when((pl.col("rush") == True).and_(pl.col("cleaned_text").str.contains("(?i)rush for")))
            .then(pl.col("cleaned_text").str.extract(r"(?i)rush for (\d+)").cast(pl.Int32))
            .when((pl.col("rush") == True).and_(pl.col("cleaned_text").str.contains("(?i)Yd Run")))
            .then(pl.col("cleaned_text").str.extract(r"(?i)(\d+) Yd Run").cast(pl.Int32))
            .when((pl.col("rush") == True).and_(pl.col("cleaned_text").str.contains("(?i)Yd Rush")))
            .then(pl.col("cleaned_text").str.extract(r"(?i)(\d+) Yd Rush").cast(pl.Int32))
            .when((pl.col("rush") == True).and_(pl.col("cleaned_text").str.contains("(?i)Yard Rush")))
            .then(pl.col("cleaned_text").str.extract(r"(?i)(\d+) Yard Rush").cast(pl.Int32))
            # ESPN "N yds loss" / "N yds gain" phrasings (0.36-live port)
            .when((pl.col("rush") == True).and_(pl.col("cleaned_text").str.contains(r"(?i)\d+ y\w*ds loss")))
            .then(-1 * pl.col("cleaned_text").str.extract(r"(?i)(\d+) y\w*ds loss").cast(pl.Int32))
            .when((pl.col("rush") == True).and_(pl.col("cleaned_text").str.contains(r"(?i)\d+ y\w*ds gain")))
            .then(pl.col("cleaned_text").str.extract(r"(?i)(\d+) y\w*ds gain").cast(pl.Int32))
            .when(
                (pl.col("rush") == True)
                .and_(pl.col("cleaned_text").str.contains("(?i)rushed"))
                .and_(pl.col("cleaned_text").str.contains("(?i)touchdown") == False),
            )
            .then(pl.col("cleaned_text").str.extract(r"(?i)for (\d+) yards").cast(pl.Int32))
            .when(
                (pl.col("rush") == True)
                .and_(pl.col("cleaned_text").str.contains("(?i)rushed"))
                .and_(pl.col("cleaned_text").str.contains("(?i)touchdown") == True),
            )
            .then(pl.col("cleaned_text").str.extract(r"(?i)for a (\d+) yard").cast(pl.Int32))
            .otherwise(None),
            # Receiving yardage reads cleaned_text so the "complete to" guard fires
            # on modern "complete short middle to ..." phrasing (raw `text` left it
            # null on every completion, zeroing the receiving box-score yards).
            yds_receiving=pl.when(
                (pl.col("pass") == True)
                .and_(pl.col("cleaned_text").str.contains(r"(?i)complete to"))
                .and_(pl.col("cleaned_text").str.contains(r"(?i)for no gain")),
            )
            .then(0)
            .when(
                (pl.col("pass") == True)
                .and_(pl.col("cleaned_text").str.contains(r"(?i)complete to"))
                .and_(pl.col("cleaned_text").str.contains(r"(?i)for a loss of")),
            )
            .then(-1 * pl.col("cleaned_text").str.extract(r"(?i)for a loss of (\d+)").cast(pl.Int32))
            .when((pl.col("pass") == True).and_(pl.col("cleaned_text").str.contains(r"(?i)complete to")))
            .then(pl.col("cleaned_text").str.extract(r"(?i)for (\d+)").cast(pl.Int32))
            .when(
                (pl.col("pass") == True).and_(
                    pl.col("cleaned_text").str.contains(
                        r"(?i)incomplete|(?i) sacked|(?i)intercepted|(?i)pass defensed",
                    ),
                ),
            )
            .then(0)
            .when((pl.col("pass") == True).and_(pl.col("cleaned_text").str.contains(r"(?i)incompletion")))
            .then(0)
            .when((pl.col("pass") == True).and_(pl.col("cleaned_text").str.contains(r"(?i)Yd pass")))
            .then(pl.col("cleaned_text").str.extract(r"(?i)(\d+) Yd pass").cast(pl.Int32))
            .otherwise(None),
            yds_int_return=pl.when(
                (pl.col("pass") == True)
                .and_(pl.col("int_td") == True)
                .and_(pl.col("text").str.contains(r"(?i)Yd Interception Return")),
            )
            .then(pl.col("text").str.extract(r"(?i)(.+)Yd Interception Return").str.extract(r"(\d+)").cast(pl.Int32))
            .when(
                (pl.col("pass") == True)
                .and_(pl.col("int") == True)
                .and_(pl.col("text").str.contains(r"(?i)for no gain")),
            )
            .then(0)
            .when(
                (pl.col("pass") == True)
                .and_(pl.col("int") == True)
                .and_(pl.col("text").str.contains(r"(?i)for a loss of")),
            )
            .then(-1 * pl.col("text").str.extract(r"(?i)for a loss of (\d+)").cast(pl.Int32))
            .when(
                (pl.col("pass") == True).and_(pl.col("int") == True).and_(pl.col("text").str.contains(r"(?i)for a TD")),
            )
            .then(pl.col("text").str.extract(r"(?i)return for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .when((pl.col("pass") == True).and_(pl.col("int") == True))
            .then(
                pl.col("text")
                .str.replace("for a 1st", "")
                .str.extract(r"(?i)for (.+)")
                .str.extract(r"(\d+)")
                .cast(pl.Int32),
            )
            .otherwise(None),
            yds_kickoff=pl.when(pl.col("kickoff_play") == True)
            .then(pl.col("text").str.extract(r"(?i)kickoff for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .otherwise(None),
            yds_kickoff_return=pl.when(
                (pl.col("kickoff_play") == True).and_(pl.col("kickoff_tb") == True).and_(pl.col("season") > 2013),
            )
            .then(25)
            .when((pl.col("kickoff_play") == True).and_(pl.col("kickoff_tb") == True).and_(pl.col("season") <= 2013))
            .then(20)
            .when(
                (pl.col("kickoff_play") == True)
                .and_(pl.col("fumble_vec") == False)
                .and_(pl.col("text").str.contains(r"(?i)for no gain|fair catch|fair caught")),
            )
            .then(0)
            .when(
                (pl.col("kickoff_play") == True)
                .and_(pl.col("fumble_vec") == False)
                .and_(pl.col("text").str.contains(r"(?i)out-of-bounds|out of bounds")),
            )
            .then(40)
            .when((pl.col("kickoff_downed") == True).or_(pl.col("kickoff_fair_catch") == True))
            .then(0)
            .when((pl.col("kickoff_play") == True).and_(pl.col("text").str.contains(r"(?i)returned by")))
            .then(pl.col("text").str.extract(r"(?i)returned by (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .when((pl.col("kickoff_play") == True).and_(pl.col("text").str.contains(r"(?i)return for")))
            .then(pl.col("text").str.extract(r"(?i)return for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .otherwise(None),
            yds_punted=pl.when((pl.col("punt") == True).and_(pl.col("punt_blocked") == True))
            .then(0)
            .when(pl.col("punt") == True)
            .then(pl.col("text").str.extract(r"(?i)punt for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .otherwise(None),
            yds_punt_return=pl.when((pl.col("punt") == True).and_(pl.col("punt_tb") == True))
            .then(20)
            .when((pl.col("punt") == True).and_(pl.col("text").str.contains(r"(?i)fair catch|fair caught")))
            .then(0)
            .when(
                (pl.col("punt") == True).and_(
                    (pl.col("punt_downed") == True)
                    .or_(pl.col("punt_oob") == True)
                    .or_(pl.col("punt_fair_catch") == True),
                ),
            )
            .then(0)
            .when((pl.col("punt") == True).and_(pl.col("text").str.contains(r"(?i)no return|no gain")))
            .then(0)
            # Losses first: the generic branches grab the first unsigned int,
            # so "returns for a loss of 9 yards" stored +9 (2,482 rows measured).
            .when(
                (pl.col("punt") == True).and_(
                    pl.col("text").str.contains(r"(?i)return(?:s|ed)?[^,]{0,20}? for a loss of \d+")
                )
            )
            .then(-1 * pl.col("text").str.extract(r"(?i)for a loss of (\d+)", 1).cast(pl.Int32))
            # Dominant 2005-2013 shape, previously never matched: 18,746 real
            # returns carried NULL yardage ("...punt for 39 yards, returned by
            # Haruki Nakamura for 1 yard to the Cincy 16.").
            .when((pl.col("punt") == True).and_(pl.col("text").str.contains(r"(?i)returned by .{2,40}? for \d+ yard")))
            .then(pl.col("text").str.extract(r"(?i)returned by .{2,40}? for (\d+) yard", 1).cast(pl.Int32))
            .when((pl.col("punt") == True).and_(pl.col("text").str.contains(r"(?i)returned \d+ yards")))
            .then(pl.col("text").str.extract(r"(?i)returned (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .when((pl.col("punt") == True).and_(pl.col("punt_blocked") == False))
            .then(pl.col("text").str.extract(r"(?i)returns for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .when((pl.col("punt") == True).and_(pl.col("punt_blocked") == True))
            .then(pl.col("text").str.extract(r"(?i)return for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .otherwise(None),
            yds_fumble_return=pl.when((pl.col("fumble_vec") == True).and_(pl.col("kickoff_play") == False))
            .then(pl.col("text").str.extract(r"(?i)return for (.+)").str.extract(r"(\d+)").cast(pl.Int32))
            .otherwise(None),
            # The first number after "sacked" is the YARDLINE in 2004-2007 text --
            # "sacked by Pierre Bell at the ECaro 48 for a loss of 8 yards" -- so the
            # positional grab stored -48 for an 8-yard sack. 2,227 of 2,283 sacks in
            # 2005 disagreed with their own "loss of N" clause, 1,393 of them by more
            # than 25 yards. The stated loss is authoritative wherever it appears
            # (98%+ of sack rows in every era); the positional grab is the fallback.
            # Two stated forms carry the loss: "for a loss of N yards" (2004-2013) and
            # "sacked by X for N yds" (2014+; 2025's vendor feed writes "for -6 yds"). A sack row with neither -- "sacked by
            # Wendell Chavis, fumbled at the SMU 30, recovered by ..." -- states no
            # loss at all, and the old positional grab returned the yardline (-30) for
            # it. Null is the honest value there; a number that is really a yardline
            # is the defect class this branch exists to remove.
            yds_sacked=pl.when(pl.col("sack") == True)
            .then(
                pl.coalesce(
                    -1 * pl.col("text").str.extract(r"(?i)loss of (\d+)", 1).cast(pl.Int32, strict=False),
                    -1
                    * pl.col("text").str.extract(r"(?i)sacked\b[^,]*?\bfor -?(\d+) y", 1).cast(pl.Int32, strict=False),
                )
            )
            .otherwise(None),
        ).with_columns(
            yds_penalty=pl.when(pl.col("penalty_detail").is_in(["Penalty Declined", "Penalty Offset"]))
            .then(0)
            .when(pl.col("yds_penalty").is_not_null())
            .then(pl.col("yds_penalty"))
            .when(
                (pl.col("penalty_detail").is_not_null())
                .and_(pl.col("yds_penalty").is_null())
                .and_(pl.col("rush") == True),
            )
            .then(pl.col("statYardage") - pl.col("yds_rushed"))
            .when(
                (pl.col("penalty_detail").is_not_null())
                .and_(pl.col("yds_penalty").is_null())
                .and_(pl.col("int") == True),
            )
            .then(pl.col("statYardage") - pl.col("yds_int_return"))
            .when(
                (pl.col("penalty_detail").is_not_null())
                .and_(pl.col("yds_penalty").is_null())
                .and_(pl.col("pass") == True)
                .and_(pl.col("sack") == False)
                .and_(pl.col("type.text") != "Pass Incompletion"),
            )
            .then(pl.col("statYardage") - pl.col("yds_receiving"))
            .when(
                (pl.col("penalty_detail").is_not_null())
                .and_(pl.col("yds_penalty").is_null())
                .and_(pl.col("pass") == True)
                .and_(pl.col("sack") == False)
                .and_(pl.col("type.text") == "Pass Incompletion"),
            )
            .then(pl.col("statYardage"))
            .when(
                (pl.col("penalty_detail").is_not_null())
                .and_(pl.col("yds_penalty").is_null())
                .and_(pl.col("pass") == True)
                .and_(pl.col("sack") == True),
            )
            .then(pl.col("statYardage") - pl.col("yds_sacked"))
            .when(pl.col("type.text") == "Penalty")
            .then(pl.col("statYardage"))
            .otherwise(None),
        )
        return play_df

    def __add_air_yards_cols(self, play_df):
        """Derive air yards / yards-after-catch from the ESPN play text.

        ESPN annotates pass plays with the on-field catch/target point as
        ``"caught at OU35"`` (completions) or ``"thrown to TEX42"`` (targets).
        The stated yardline is relative to whichever team owns that side of the
        field, not the offense, so the abbreviation in the text is resolved to
        the possessing-vs-defending team -- using the same prefix-tolerant
        matcher (:func:`_abbr_compat`) that resolves recovery/penalty teams, so
        ESPN's BUF/BUFF two-abbreviation-form inconsistency still resolves --
        then converted to yards-to-endzone:

        * catch abbrev on the possessing team's side -> ``100 - yardline``
        * catch abbrev on the defending team's side  -> ``yardline``
        * no air-yards text / unresolved abbreviation -> null

        ``air_yards = start.yardsToEndzone - air_yardsToEndzone`` and
        ``yards_after_catch = statYardage - air_yards`` (completed passes only;
        ``statYardage`` is ESPN's reliable net-yardage field == ``air_yards +
        YAC``). Ported from the cfbfastR-cfb-data air-yards derivation
        (R/pandas ``0.36-live``); the original character-count cosine-similarity
        disambiguation is replaced by the codebase's abbreviation matcher, and
        the R's ``yds_receiving`` (same quantity, but its sdv-py text extractor
        is empty on modern ESPN text) by ``statYardage``.

        Args:
            play_df: the in-flight plays frame; must already carry ``text``,
                ``start.yardsToEndzone``, ``pos_team`` / ``def_pos_team`` (team
                ids), ``homeTeamId`` / ``awayTeamId``, ``homeTeamAbbrev`` /
                ``awayTeamAbbrev``, ``statYardage`` and ``completion``.

        Returns:
            polars.DataFrame: the frame with ``air_yardsToEndzone``,
            ``air_yards`` and ``yards_after_catch`` (all nullable ``Int64``)
            appended.
        """
        home_u = pl.col("homeTeamAbbrev").str.to_uppercase()
        away_u = pl.col("awayTeamAbbrev").str.to_uppercase()
        # pos_team / def_pos_team are team ids; map each back to its abbreviation
        # for THIS play so the catch-point abbrev can be sided against them.
        pos_abbr = pl.when(pl.col("pos_team") == pl.col("homeTeamId")).then(home_u).otherwise(away_u)
        def_abbr = pl.when(pl.col("def_pos_team") == pl.col("homeTeamId")).then(home_u).otherwise(away_u)

        play_df = play_df.with_columns(
            _catch_abbr=pl.col("text")
            .str.extract(r"(?i)(?:caught at|thrown to) ([A-Za-z]+)\d{1,2}", 1)
            .str.to_uppercase(),
            _catch_yardline=pl.col("text")
            .str.extract(r"(?i)(?:caught at|thrown to) [A-Za-z]+(\d{1,2})", 1)
            .cast(pl.Int64),
        )
        play_df = play_df.with_columns(
            air_yardsToEndzone=pl.when(
                pl.col("_catch_abbr").is_null().or_(pl.col("_catch_yardline").is_null()),
            )
            .then(pl.lit(None, dtype=pl.Int64))
            # possessing-team side of the field: yards still to go from the catch point
            .when(_abbr_compat(pl.col("_catch_abbr"), pos_abbr))
            .then(100 - pl.col("_catch_yardline"))
            # defending-team side: the stated yardline IS the distance to the endzone
            .when(_abbr_compat(pl.col("_catch_abbr"), def_abbr))
            .then(pl.col("_catch_yardline"))
            .otherwise(pl.lit(None, dtype=pl.Int64)),
        )
        play_df = play_df.with_columns(
            air_yards=(pl.col("start.yardsToEndzone").cast(pl.Int64) - pl.col("air_yardsToEndzone")),
        ).with_columns(
            # YAC = total play yardage - air yards. statYardage is ESPN's reliable
            # net-yards field (== air_yards + YAC for a completion); the R source's
            # yds_receiving is the same quantity but its sdv-py text extractor is
            # empty on modern ESPN text ("complete short middle to ...") so it can't
            # be used here.
            yards_after_catch=pl.when(pl.col("completion") == True)
            .then(pl.col("statYardage").cast(pl.Int64) - pl.col("air_yards"))
            .otherwise(pl.lit(None, dtype=pl.Int64)),
        )
        return play_df.drop("_catch_abbr", "_catch_yardline")

    def __add_player_cols(self, play_df):
        play_df = (
            play_df.with_columns(
                # --- RB Names -----
                rush_player=pl.when(pl.col("rush") == True)
                .then(
                    _extract_player_name(
                        pl.col("text"),
                        r"(?i)(.{0,25} )run |(?i)(.{0,25} )\d{0,2} Yd Run|(?i)(.{0,25} )rush |(?i)(.{0,25} )rushed ",
                    )
                    .str.replace(r"(?i) run |(?i) \d+ Yd Run|(?i) rush ", "")
                    .str.replace(r" \((.+)\)", ""),
                )
                .otherwise(None),
                # --- QB Names -----
                pass_player=pl.when(
                    (pl.col("pass") == True)
                    .and_(pl.col("sack_vec") == False)
                    .and_(pl.col("type.text") != "Passing Touchdown"),
                )
                .then(
                    _extract_player_name(
                        pl.col("text"),
                        r"(?i)(.{0,30} )pass |(?i)(.{0,30} )sacked by|(?i)(.{0,30} )sacked for|(?i)(.{0,30} )incomplete|(?i)pass from (.{0,30} ) \( ",
                    ).str.replace(r"(?i)pass |(?i) sacked by|(?i) sacked for|(?i) incomplete", ""),
                )
                .when(
                    (pl.col("pass") == True)
                    .and_(pl.col("sack_vec") == True)
                    .and_(pl.col("type.text") != "Passing Touchdown"),
                )
                .then(
                    _extract_player_name(
                        pl.col("text"), r"(?i)(.{0,30} )sacked by|(?i)(.{0,30} )sacked for"
                    ).str.replace(r"(?i)pass |(?i) sacked by|(?i) sacked for|(?i) incomplete", ""),
                )
                .when((pl.col("pass") == True).and_(pl.col("type.text") == "Passing Touchdown"))
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)pass from(.+)")
                    .str.replace(r"pass from", "")
                    .str.replace(r" \((.+)\)", "")
                    .str.replace(r" \,", ""),
                )
                .otherwise(None),
            )
            .with_columns(
                pass_player=pl.when((pl.col("type.text") == "Passing Touchdown").and_(pl.col("pass_player").is_null()))
                .then(
                    pl.col("text")
                    .str.extract(r"(.+)pass(.+)? complete to")
                    .str.replace(r" pass complete to(.+)", "")
                    .str.replace(r" pass complete to", ""),
                )
                .otherwise(pl.col("pass_player")),
            )
            .with_columns(
                pass_player=pl.when((pl.col("type.text") == "Passing Touchdown").and_(pl.col("pass_player").is_null()))
                .then(
                    pl.col("text")
                    .str.extract(r"(.+)pass,to")
                    .str.replace(r" pass,to(.+)", "")
                    .str.replace(r" pass,to", "")
                    .str.replace(r" \((.+)\)", ""),
                )
                .otherwise(pl.col("pass_player")),
            )
            .with_columns(
                pass_player=pl.when(
                    (pl.col("pass") == True).and_(
                        (
                            (pl.col("pass_player").str.strip_chars().str.len_chars() == 0).or_(
                                pl.col("pass_player").is_null(),
                            )
                        ),
                    ),
                )
                .then(pl.lit("TEAM"))
                .otherwise(pl.col("pass_player")),
                # --- WR Names -----
                receiver_player=pl.when(
                    (pl.col("pass") == True).and_(pl.col("text").str.contains(r"(?i)sacked") == False),
                )
                .then(pl.col("text").str.extract(r"(?i)to (.+)"))
                .when(pl.col("text").str.contains(r"(?i)Yd pass"))
                .then(pl.col("text").str.extract(r"(?i)(.{0,25} )\d{0,2} Yd pass"))
                .when(pl.col("text").str.contains(r"(?i)Yd TD pass"))
                .then(pl.col("text").str.extract(r"(?i)(.{0,25} )\d{0,2} Yd TD pass"))
                .otherwise(None),
            )
            .with_columns(
                receiver_player=pl.when(
                    (pl.col("type.text") == "Sack")
                    .or_(pl.col("type.text") == "Interception Return")
                    .or_(pl.col("type.text") == "Interception Return Touchdown")
                    .or_(
                        (
                            pl.col("type.text").is_in(
                                ["Fumble Recovery (Opponent) Touchdown", "Fumble Recovery (Opponent)"],
                            )
                        ).and_(pl.col("text").str.contains(r"(?i)sacked")),
                    ),
                )
                .then(None)
                .otherwise(
                    pl.col("receiver_player")
                    .str.replace(r"to ", "")
                    .str.replace(r"(?i)\\,.+", "")
                    .str.replace(r"(?i)for (.+)", "")
                    .str.replace(r"(?i) (\d{1,2})", "")
                    .str.replace(r"(?i) Yd pass", "")
                    .str.replace(r"(?i) Yd TD pass", "")
                    .str.replace(r"(?i)pass complete to", "")
                    .str.replace(r"(?i)penalty", "")
                    .str.replace(r'(?i) "', ""),
                ),
            )
            .with_columns(
                receiver_player=pl.when(pl.col("receiver_player").str.contains(r"(?i)III") == True)
                .then(pl.col("receiver_player").str.replace(r"(?i)[A-Z]{3,}", ""))
                .otherwise(pl.col("receiver_player")),
            )
            .with_columns(
                receiver_player=pl.col("receiver_player")
                .str.replace(r"(?i) &", "")
                .str.replace(r"(?i)A&M", "")
                # Strip a stray trailing team/state token only as the FINAL standalone
                # word -- anchored so it can't corrupt real names (" ST" used to eat the
                # " St" inside "Stewart" -> "ewart").
                .str.replace(r"(?i)\s(ST|GA|UL|FL|OH|NC)$", "")
                .str.replace(r'(?i) "', "")
                .str.replace(r"(?i) \\u00c9", "")
                .str.replace(r"(?i) fumbled,", "")
                .str.replace(r"(?i)the (.+)", "")
                .str.replace(r"(?i)pass incomplete to", "")
                .str.replace(r"(?i)(.+)pass incomplete", "")
                .str.replace(r"(?i)pass incomplete", "")
                .str.replace(r"(?i) \((.+)\)", "")
                # 2025's vendor template continues past the receiver -- "to #3 A.Evans
                # III caught at ARK40", "to J.Hayes thrown TCU45", "thrown to UM20
                # broken up by #1 G.Smith" -- and none of the trims above knew those
                # clauses, so the capture ran on into them. The participant join hides
                # it on the site; the regex fallback is what produced the phantom
                # box-score rows in the first place, so it has to be right on its own.
                .str.replace(r"(?i)\s*(?:caught at|thrown|broken up|intended for|defended by)\b.*$", "")
                .str.strip_chars(),
                # --- Sack Names -----
                sack_players=pl.when(
                    (pl.col("sack") == True).or_((pl.col("fumble_vec") == True).and_(pl.col("pass") == True)),
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)sacked by(.+)")
                    .str.replace(r"for (.+)", "")
                    .str.replace(r"(.+) by ", "")
                    .str.replace(r" at the (.+)", ""),
                )
                .otherwise(None),
            )
            .with_columns(
                # Not receivers: a bare yardline ("thrown to UM20" names no target and
                # the capture returned "UM20"), and a lone initial ("to K. thrown UNC25"
                # is ESPN's own truncation). Null beats a phantom box-score row.
                # The yardline may be mixed case ("Tulsa25") and the team may stand
                # alone with no number ("FAU"); in this template a real name always
                # carries an initial and a period, so a bare token is never one.
                # Trailing punctuation left by a trim ("T.J. Simpson," after cutting
                # "broken up by ..."). That debris predates 2025 by fifteen years: the
                # chain's `\\,.+` trim carries a doubled backslash, so it matches a
                # literal "\," and has never fired -- "Anthony Miller, broken up by
                # Omar Bolden." survived intact from 2010 on. Left in place, since a
                # real first-comma cut would break "Garcia Rodriguez,Guillermo".
                # ...and an UNCLOSED parenthetical -- "Corey (WR Brown, clock:55" (2010)
                # -- which the " \((.+)\)" trim above needs a closing paren to catch.
                # No receiver name contains "(".
                receiver_player=pl.col("receiver_player")
                .str.replace(r"\s*\(.*$", "")
                .str.replace(r"\b(Jr|Sr)\.$", "${1}\x00")  # protect a suffix period ...
                .str.replace(r"[,.\s]+$", "")  # ... strip "Troy Evans." / "T.J. Simpson," ...
                .str.replace("\x00", "."),  # ... and restore it. Rust regex has no lookbehind.
            )
            .with_columns(
                receiver_player=pl.when(
                    pl.col("receiver_player").str.contains(r"^[A-Za-z]{2,8}\d{1,2},?$")
                    | pl.col("receiver_player").str.contains(r"^[A-Z]{2,6}$")
                    | pl.col("receiver_player").str.contains(r"^[A-Z]\.?$")
                    # a bare jersey ("pass complete to #87 for 8 yards", 2005) is not a name
                    | pl.col("receiver_player").str.contains(r"^#\d{1,3}$"),
                )
                .then(None)
                .otherwise(pl.col("receiver_player")),
            )
            .with_columns(
                sack_player1=pl.col("sack_players").str.replace(r"and (.+)", ""),
                sack_player2=pl.when(pl.col("sack_players").str.contains(r"and (.+)"))
                .then(pl.col("sack_players").str.replace(r"(.+) and", ""))
                .otherwise(None),
                # --- Interception Names -----
                interception_player=pl.when(pl.col("text").str.contains(r"(?i)yd interception return"))
                .then(
                    _extract_player_name(
                        pl.col("text"),
                        r"(?i)(.{0,25} )\d{0,2} Yd Interception Return|(?i)(.{0,25} )\d{0,2} yd interception return",
                    )
                    .str.replace(r"return (.+)", "")
                    .str.replace(r"(.+) intercepted", "")
                    .str.replace(r"intercepted", "")
                    .str.replace(r"Yd Interception Return", "")
                    .str.replace(r"for a 1st down", "")
                    .str.replace(r"(\d{1,2})", "")
                    .str.replace(r"for a TD", "")
                    .str.replace(r"at the (.+)", "")
                    .str.replace(r" by ", ""),
                )
                # Narrative texts name the interceptor in two era-dependent
                # shapes: "pass intercepted by <Name> (TEAM)..." (older
                # seasons) and "pass intercepted <Name> return for ..."
                # (newer seasons, no "by"). Capture capitalized name tokens
                # with the case-toggle idiom so the lowercase tail (return/
                # for/at) terminates the name — the old `intercepted (.+)`
                # capture took the whole tail and missed the no-"by" era.
                # First token allows a bare initial ("S Castille", "M Tauiliili");
                # an optional "#NN " consumes NCAA-style jersey prefixes
                # ("intercepted by #3 J.Dugger").
                .when(pl.col("text").str.contains(r"(?i)intercepted"))
                .then(
                    pl.coalesce(
                        pl.col("text").str.extract(
                            r"(?i)intercepted by (?:#\d+\s+)?(?-i:([A-Z][\w'.\-]*(?:\s+[A-Z][\w'.\-]*){0,2}))",
                            1,
                        ),
                        pl.col("text").str.extract(
                            r"(?i)pass intercepted (?:#\d+\s+)?(?-i:([A-Z][\w'.\-]*(?:\s+[A-Z][\w'.\-]*){0,2}))",
                            1,
                        ),
                    ),
                )
                .otherwise(None),
                # --- Pass Breakup Players ----
                pass_breakup_player=pl.when(pl.col("pass") == True)
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)broken up by (.+)")
                    .str.replace(r"(.+) broken up by", "")
                    .str.replace(r"broken up by", "")
                    .str.replace(r"Penalty(.+)", "")
                    .str.replace(r"SOUTH FLORIDA", "")
                    .str.replace(r"WEST VIRGINIA", "")
                    .str.replace(r"MISSISSIPPI ST", "")
                    .str.replace(r"CAMPBELL", "")
                    .str.replace(r"COASTL CAROLINA", ""),
                )
                .otherwise(None),
                # --- Punter Names ----
                punter_player=pl.when(pl.col("type.text").str.contains("Punt"))
                .then(
                    _extract_player_name(pl.col("text"), r"(?i)(.{0,30}) punt|(?i)Punt by (.{0,30})")
                    .str.replace(r"(?i) punt", "")
                    .str.replace(r"(?i) for(.+)", "")
                    .str.replace(r"(?i)Punt by ", "")
                    .str.replace(r"(?i)\((.+)\)", "")
                    .str.replace(r"(?i) returned \d+", "")
                    .str.replace(r"(?i) returned", "")
                    .str.replace(r"(?i) no return", ""),
                )
                .otherwise(None),
                # --- Punt Returner Names ----
                punt_return_player=pl.when(pl.col("type.text").str.contains("Punt"))
                .then(
                    _extract_player_name(
                        pl.col("text"),
                        r"(?i), (.{0,25}) returns|(?i)fair catch by (.{0,25})|(?i), returned by (.{0,25})|(?i)yards by (.{0,30})|(?i) return by (.{0,25})",
                    )
                    .str.replace(r"(?i), ", "")
                    .str.replace(r"(?i) returns", "")
                    .str.replace(r"(?i) returned", "")
                    .str.replace(r"(?i) return", "")
                    .str.replace(r"(?i)fair catch by", "")
                    .str.replace(r"(?i) at (.+)", "")
                    .str.replace(r"(?i) for (.+)", "")
                    .str.replace(r"(?i)(.+) by ", "")
                    .str.replace(r"(?i) to (.+)", "")
                    .str.replace(r"(?i)\((.+)\)", ""),
                )
                .otherwise(None),
                # --- Punt Blocker Names ----
                punt_block_player=pl.when(pl.col("type.text").str.contains("Punt"))
                .then(
                    _extract_player_name(pl.col("text"), r"(?i)punt blocked by (.{0,25})|(?i)blocked by(.+)")
                    .str.replace(r"punt blocked by |for a(.+)", "")
                    .str.replace(r"blocked by(.+)", "")
                    .str.replace(r"blocked(.+)", "")
                    .str.replace(r" for(.+)", "")
                    .str.replace(r",(.+)", "")
                    .str.replace(r"punt blocked by |for a(.+)", ""),
                )
                .otherwise(None),
            )
            .with_columns(
                punt_block_player=pl.when((pl.col("type.text").str.contains(r"(?i)yd return of blocked punt")))
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)(.+) yd return of blocked")
                    .str.replace(r"(?i)blocked|(?i)Blocked", "")
                    .str.replace(r"(?i)\d+", "")
                    .str.replace(r"(?i)yd return of", ""),
                )
                .otherwise(pl.col("punt_block_player")),
                # --- Punt Block Returner Names ----
                punt_block_return_player=pl.when(
                    (pl.col("type.text").str.contains(r"Punt"))
                    .and_(pl.col("text").str.contains(r"(?i)blocked"))
                    .and_(pl.col("text").str.contains(r"(?i)return")),
                )
                .then(pl.col("text").str.extract(r"(?i)(.+) return"))
                .otherwise(None),
            )
            .with_columns(
                punt_block_return_player=pl.struct("punt_block_player", "punt_block_return_player").map_elements(
                    lambda cols: (
                        cols["punt_block_return_player"]
                        .replace(r"(?i)(.+)blocked by", "")
                        .replace(str(pl.format(r"(?i)blocked by {}", cols["punt_block_player"])), "")
                        if cols["punt_block_return_player"] is not None
                        else None
                    ),
                    return_dtype=pl.Utf8,
                ),
            )
            .with_columns(
                punt_block_return_player=pl.col("punt_block_return_player")
                .str.replace(r"(?i)return(.+)", "")
                .str.replace(r"(?i)return", "")
                .str.replace(r"for a TD(.+)|for a SAFETY(.+)", "")
                .str.replace(r"(?i)blocked by ", "")
                .str.replace(r", ", ""),
                # --- Kickoff Names ----
                kickoff_player=pl.when(pl.col("type.text").str.contains(r"(?i)kickoff"))
                .then(
                    _extract_player_name(pl.col("text"), r"(?i)(.{0,25}) kickoff|(.{0,25}) on-side").str.replace(
                        r"(?i) on-side| kickoff", ""
                    ),
                )
                .otherwise(None),
                # --- Kickoff Returner Names ----
                kickoff_return_player=pl.when(pl.col("type.text").str.contains(r"(?i)ickoff"))
                .then(
                    _extract_player_name(
                        pl.col("text"),
                        r"(?i), (.{0,25}) return|(?i), (.{0,25}) fumble|(?i)returned by (.{0,25})|(?i)touchback by (.{0,25})",
                    )
                    .str.replace(r", ", "")
                    .str.replace(r"(?i) for .*", "")
                    .str.replace(r"(?i) return|(?i) fumble|(?i) returned by|(?i)touchback by ", "")
                    .str.replace(r"(?i) at the.*", "")
                    .str.replace(r"(?i) to the.*", "")
                    .str.replace(r"\((.+)\)(.+)", ""),
                )
                .otherwise(None),
                # --- Field Goal Kicker Names ----
                fg_kicker_player=pl.when(pl.col("type.text").str.contains(r"(?i)Field Goal"))
                .then(
                    _extract_player_name(
                        pl.col("text"),
                        r"(?i)(.{0,25} )\d{0,2} yd field goal|(?i)(.{0,25} )\d{0,2} yd fg|(?i)(.{0,25} )\d{0,2} yard field goal",
                    )
                    .str.replace(r"(?i) Yd Field Goal|(?i)Yd FG |(?i)yd FG|(?i) yd FG", "")
                    .str.replace(r"(\d{1,2})", ""),
                )
                .otherwise(None),
                # --- Field Goal Blocker Names ----
                # Name-shaped capture first, legacy fixed-window chain as the fallback --
                # the same two-tier shape the recovered-by extraction already uses below.
                #
                # The window is (.{0,25}) and its cleanup trims at the first comma, which
                # cannot help when ESPN appends the NEXT play's text directly after the
                # name: "blocked by LaMareon James (00:07) Boomer,Colton field goal
                # attempt from 27 yards NO GOOD" yields "LaMareon James (00:07) Bo" --
                # exactly 25 characters, the window's ceiling, with the clock arriving
                # before any comma. 9 such rows in the published 2024 season and 1 in
                # 2023; the identical defect in fumble_recovered_player_name was already
                # cured by giving it a name-shaped pattern, so this follows suit rather
                # than adding another cleanup step to a chain of twenty.
                fg_block_player=pl.when(pl.col("type.text").str.contains(r"(?i)Field Goal"))
                .then(
                    pl.coalesce(
                        pl.col("text").str.extract(
                            # Single spaces throughout, deliberately. ESPN writes
                            # "blocked by TEAM  Teu Kautai return for 12" when the block
                            # is credited to the team rather than a player: TEAM is the
                            # correct answer and the double space marks the start of the
                            # RETURN clause. With \s+ the optional team token swallowed
                            # "TEAM" and the capture ran on into the returner's name.
                            r"(?i)blocked by (?:(?-i:[A-Z]{2,6}) )?(?:#\d+ )?"
                            r"((?-i:[A-Z][\w'.\-]*(?:\s[A-Z][\w'.\-]*){1,2}))",
                            1,
                        ),
                        pl.col("text")
                        .str.extract(r"(?i)blocked by (.{0,25})")
                        .str.replace(r",(.+)", "")
                        .str.replace(r"blocked by ", "")
                        .str.replace(r"  (.)+", ""),
                    )
                )
                .otherwise(None),
                # --- Field Goal Returner Names ----
                fg_return_player=pl.when(
                    (pl.col("type.text").str.contains(r"(?i)Field Goal"))
                    .and_(pl.col("text").str.contains(r"(?i)blocked by|missed"))
                    .and_(pl.col("text").str.contains(r"(?i)return")),
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)  (.+)")
                    .str.replace(r"(?i),(.+)", "")
                    .str.replace(r"(?i)return ", "")
                    .str.replace(r"(?i)returned ", "")
                    .str.replace(r"(?i) for (.+)", "")
                    .str.replace(r"(?i) for (.+)", ""),
                )
                .otherwise(None),
            )
            .with_columns(
                fg_return_player=pl.when(
                    (pl.col("type.text").is_in(["Missed Field Goal Return", "Missed Field Goal Return Touchdown"])),
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)(.+)return")
                    .str.replace(r"(?i) return", "")
                    .str.replace(r"(?i)(.+),", ""),
                )
                .otherwise(pl.col("fg_return_player")),
                # --- Fumble Recovery Names ----
                fumble_player=pl.when(pl.col("text").str.contains(r"(?i)fumble"))
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)(.{0,25} )fumble|(?i)(.{0,25} )fumble")
                    .str.replace(r"(?i) fumble(.+)", "")
                    .str.replace(r"(?i)fumble", "")
                    .str.replace(r"(?i) yds", "")
                    .str.replace(r"(?i) yd", "")
                    .str.replace(r"(?i)yardline", "")
                    .str.replace(r"(?i) yards|(?i) yard|(?i)for a TD|(?i)or a safety", "")
                    .str.replace(r"(?i) for ", "")
                    .str.replace(r"(?i) a safety", "")
                    .str.replace(r"(?i)r no gain", "")
                    .str.replace(r"(?i)(\d{1,2})", "")
                    .str.replace(r", ", ""),
                )
                .otherwise(None),
            )
            .with_columns(
                # Name-shaped capture first (optional #NN jersey prefix, 1-3
                # capitalized tokens, case-toggle termination); the legacy
                # fragment capture is the fallback. Measured: 100% of sampled
                # name-set/id-null fumble rows were dirty fragment captures
                # ("o the UTAH Nate Johnson") that then failed the roster
                # attach; simulated fix lifts fumble credit joins 56% -> 79%.
                fumble_player=pl.when(pl.col("type.text") == "Penalty")
                .then(None)
                .otherwise(
                    pl.coalesce(
                        pl.col("text").str.extract(
                            r"(?i)(?:#\d+\s+)?((?-i:[A-Z][\w'.\-]*(?:\s[A-Z][\w'.\-]*){0,2}))\s+fumbled?", 1
                        ),
                        pl.col("text").str.extract(
                            r"(?i)fumbled? by\s+(?:#\d+\s+)?((?-i:[A-Z][\w'.\-]*(?:\s[A-Z][\w'.\-]*){0,2}))", 1
                        ),
                        pl.col("fumble_player"),
                    )
                ),
                # --- Forced Fumble Names ----
                fumble_forced_player=pl.when(
                    (pl.col("text").str.contains(r"(?i)fumble")).and_(pl.col("text").str.contains(r"(?i)forced by")),
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)forced by(.{0,25})")
                    .str.replace(r"(?i)(.+)forced by", "")
                    .str.replace(r"(?i)forced by", "")
                    .str.replace(r"(?i), recove(.+)", "")
                    .str.replace(r"(?i), re(.+)", "")
                    .str.replace(r"(?i), fo(.+)", "")
                    .str.replace(r"(?i), r", "")
                    .str.replace(r"(?i), ", "")
                    .str.replace(r"(?i) at th.*", ""),
                )
                .otherwise(None),
            )
            .with_columns(
                fumble_forced_player=pl.when(pl.col("type.text") == "Penalty")
                .then(None)
                .otherwise(pl.col("fumble_forced_player")),
                # --- Fumble Recovered Names ----
                fumble_recovered_player=pl.when(
                    (pl.col("text").str.contains(r"(?i)fumble")).and_(pl.col("text").str.contains(r"(?i)recovered by")),
                )
                .then(
                    pl.col("text")
                    .str.extract(r"(?i)recovered by(.{0,30})")
                    .str.replace(r"(?i)for a 1ST down", "")
                    .str.replace(r"(?i)for a 1st down", "")
                    .str.replace(r"(?i)(.+)recovered", "")
                    .str.replace(r"(?i)(.+) by", "")
                    .str.replace(r"(?i), recove(.+)", "")
                    .str.replace(r"(?i), re(.+)", "")
                    .str.replace(r"(?i)a 1st down", "")
                    .str.replace(r"(?i) a 1st down", "")
                    .str.replace(r"(?i), for(.+)", "")
                    .str.replace(r"(?i) for a", "")
                    .str.replace(r"(?i) fo", "")
                    .str.replace(r"(?i) , r", "")
                    .str.replace(r"(?i), r", "")
                    .str.replace(r"(?i)  (.+)", "")
                    .str.replace(r"(?i) ,", "")
                    .str.replace(r"(?i)penalty(.+)", "")
                    .str.replace(r"(?i)for a 1ST down", "")
                    .str.replace(r"(?i) at the.*", ""),
                )
                .otherwise(None),
            )
            .with_columns(
                # Name-shaped recovered-by capture with an optional leading
                # ALL-CAPS team token ("recovered by SAC Noah St-Juste") -- the
                # abbr previously stayed glued to the name and the roster
                # attach failed. Legacy chain remains the fallback.
                fumble_recovered_player=pl.when(pl.col("type.text") == "Penalty")
                .then(None)
                .otherwise(
                    pl.coalesce(
                        pl.col("text").str.extract(
                            r"(?i)recovered by\s+(?:(?-i:[A-Z]{2,6})\s+)?(?:#\d+\s+)?((?-i:[A-Z][\w'.\-]*(?:\s[A-Z][\w'.\-]*){1,2}))",
                            1,
                        ),
                        pl.col("fumble_recovered_player"),
                    )
                ),
            )
            .with_columns(
                ## Extract player names. _strip_presentational_tokens runs HERE, at the one
                ## point every name column is finalized, so the direct
                ## .str.extract fallbacks (receiver 'to (.+)', the Passing
                ## Touchdown 'pass from(.+)') are covered too -- not just the
                ## ones routed through _extract_player_name.
                passer_player_name=_strip_presentational_tokens(pl.col("pass_player")),
                rusher_player_name=_strip_presentational_tokens(pl.col("rush_player")),
                receiver_player_name=_strip_presentational_tokens(pl.col("receiver_player")),
                sack_player_name=_strip_presentational_tokens(pl.col("sack_player1")),
                sack_player_name2=_strip_presentational_tokens(pl.col("sack_player2")),
                pass_breakup_player_name=_strip_presentational_tokens(pl.col("pass_breakup_player")),
                interception_player_name=_strip_presentational_tokens(pl.col("interception_player")),
                fg_kicker_player_name=_strip_presentational_tokens(pl.col("fg_kicker_player")),
                fg_block_player_name=_strip_presentational_tokens(pl.col("fg_block_player")),
                fg_return_player_name=_strip_presentational_tokens(pl.col("fg_return_player")),
                kickoff_player_name=_strip_presentational_tokens(pl.col("kickoff_player")),
                kickoff_return_player_name=_strip_presentational_tokens(pl.col("kickoff_return_player")),
                punter_player_name=_strip_presentational_tokens(pl.col("punter_player")),
                punt_block_player_name=_strip_presentational_tokens(pl.col("punt_block_player")),
                punt_return_player_name=_strip_presentational_tokens(pl.col("punt_return_player")),
                punt_block_return_player_name=_strip_presentational_tokens(pl.col("punt_block_return_player")),
                fumble_player_name=_strip_presentational_tokens(pl.col("fumble_player")),
                fumble_forced_player_name=_strip_presentational_tokens(pl.col("fumble_forced_player")),
                fumble_recovered_player_name=_strip_presentational_tokens(pl.col("fumble_recovered_player")),
            )
            .drop(
                [
                    "rush_player",
                    "receiver_player",
                    "pass_player",
                    "sack_player1",
                    "sack_player2",
                    "pass_breakup_player",
                    "interception_player",
                    "punter_player",
                    "fg_kicker_player",
                    "fg_block_player",
                    "fg_return_player",
                    "kickoff_player",
                    "kickoff_return_player",
                    "punt_return_player",
                    "punt_block_player",
                    "punt_block_return_player",
                    "fumble_player",
                    "fumble_forced_player",
                    "fumble_recovered_player",
                ],
            )
        )
        return play_df

    def __add_series_data(self, play_df):
        """Port of cfbfastR's series / first-down decomposition.

        Provenance (verbatim port, bug-for-bug):
            * ``cfbfastR/R/pbp_prep_epa_df_after.R`` L194-298 --
              ``first_by_penalty`` / ``first_by_yards`` row flags, their lag
              ladder, ``new_series``, and the four ``firstD_by_*`` outputs.
            * ``cfbfastR/R/pbp_clean_drive_dat.R`` L18-66 + L300-312 -- the
              half-scoped ``lag(1..3)`` event-flag lags (0-filled at half
              starts) and ``drive_event_number`` (within-drive cumsum of
              non-End rows).
            * ``cfbfastR/R/pbp_clean_pbp_dat.R`` L388-390 -- ``lag_play_type``
              is UNGROUPED on the per-game frame ("End of Half" is not in the
              Timeout/End-Period skip set, so crossing the half boundary is
              inert).

        Emits TWO first-down representations, both Boolean:

        * **Earned family** (``first_down_yards`` / ``first_down_penalty`` /
          ``first_down_earned``) -- flagged on the play that EARNED the first
          down, so the row's own ``pos_team`` is the crediting team. This is
          the COUNTING surface: box/team aggregation sums these grouped by
          ``pos_team``. Mutually exclusive buckets; yards takes precedence.
        * **Series-start family** (``firstD_by_kickoff`` / ``firstD_by_poss``
          / ``firstD_by_penalty`` / ``firstD_by_yards`` + ``new_series``) --
          the verbatim cfbfastR port: flags the first play of the NEW series
          with the cause it began (1-3 rows after an earning conversion, via
          the Timeout/End-Period lag ladder). ``_poss`` / ``_kickoff`` mark
          UNEARNED (happenstance) series starts -- possession change, drive
          start, kickoff -- and must never be counted as earned first downs.
          Not a counting surface: the ladder drops drive-ending conversions
          (TDs, end-of-half) and can double-fire across a Timeout row (the
          Timeout row takes the lag1 branch, the next play the lag2 branch) --
          both faithful to R.

        Known divergence from R (documented, benign): R leaves ``firstD_by_yards``
        ``NA`` on rows 1-2 of a half (unfilled lag2/lag3); we emit ``False``.
        """
        to_ep = ["Timeout", "End Period"]
        end_rows = ["End Period", "End of Half", "End of Game"]
        # the Fox cleaning pipeline reaches this stage without the attribution
        # columns; a null penalized_team simply disables the auto-first-down
        # foul branch (penalty_1st_conv still counts)
        if "penalized_team" not in play_df.columns:
            play_df = play_df.with_columns(penalized_team=pl.lit(None, dtype=pl.Int32))
        play_df = (
            play_df.with_columns(
                first_by_penalty=pl.when(
                    (pl.col("type.text").is_in(penalty)).and_(pl.col("penalty_1st_conv") == True),
                )
                .then(True)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("statYardage") > pl.col("start.distance")),
                )
                .then(True)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("statYardage") >= pl.col("start.distance")),
                )
                .then(True)
                .otherwise(False),
                first_by_yards=(
                    (pl.col("type.text").is_in(normalplay)).or_(pl.col("type.text") == "Fumble Recovery (Own)")
                ).and_(pl.col("statYardage") >= pl.col("start.distance")),
                drive_event=pl.col("type.text").is_in(end_rows) == False,
            )
            .with_columns(
                # --- Earned-first-down family (conversion-row, snap-attributed) ---
                # Unlike the series-start firstD_by_* family below (which flags
                # the first play of the NEW series, 1-3 rows after the fact),
                # these flag the play that EARNED the first down, so the row's
                # own pos_team is the crediting team -- no lag attribution
                # ambiguity, and drive-ending conversions (TDs, end-of-half)
                # are captured. Kickoffs / punts / PATs / non-play rows are
                # excluded by the play-type lists. Buckets are mutually
                # exclusive; yards takes precedence (official scoring: if the
                # play reached the line to gain, penalty yardage is moot).
                first_down_yards=pl.when(
                    # 1) vendor text marker -- ESPN's play text writes an
                    # explicit "1ST down"/"1ST DOWN" on converting plays; where
                    # present it is per-play ground truth and overrides the
                    # derived rule (it counts spot/distance-data mismatches and
                    # the occasional goal-to-go TD the stat crew credited).
                    # Penalty-involved rows are excluded here so accepted-penalty
                    # conversions keep routing to the penalty bucket.
                    (pl.col("text").str.contains(r"(?i)\b1st down\b").fill_null(False))
                    .and_(pl.col("text").str.contains("(?i)short of") == False)
                    .and_(pl.col("text").str.contains("(?i)penalty") == False)
                    .and_(
                        (
                            pl.col("type.text").is_in(
                                [
                                    *normalplay,
                                    "Passing Touchdown",
                                    "Rushing Touchdown",
                                    "Pass Reception Touchdown",
                                    "Fumble Recovery (Own) Touchdown",
                                ],
                            )
                        ).or_(
                            # fumble rows keep the marker only when possession
                            # was retained (own recovery)
                            (pl.col("type.text").is_in(["Fumble", "Fumble Recovery (Own)"])).and_(
                                pl.col("change_of_pos_team") == False,
                            ),
                        ),
                    ),
                )
                .then(True)
                .when(
                    # 2) derived rule
                    (
                        (pl.col("type.text").is_in(normalplay)).or_(
                            # offensive scrimmage TDs reach the line to gain but
                            # sit outside normalplay (own play-type strings) --
                            # the structural miss of both first_down_created
                            # (end.down != 1 after a TD) and the series ladder
                            # (next row is a kickoff / new possession).
                            pl.col("type.text").is_in(
                                [
                                    "Passing Touchdown",
                                    "Rushing Touchdown",
                                    "Pass Reception Touchdown",
                                    "Fumble Recovery (Own) Touchdown",
                                ],
                            ),
                        )
                    )
                    .and_(pl.col("statYardage") >= pl.col("start.distance"))
                    # goal-to-go has NO line to gain, so nothing can earn a
                    # first down there (a goal-to-go TD scores without one) --
                    # verified vs the ESPN box: e.g. 401032062/BYU is exact
                    # only when the 4 goal-to-go TDs are excluded.
                    .and_(pl.col("start.distance") < pl.col("start.yardsToEndzone"))
                    # a penalty-wiped play earns nothing by yards (the accepted
                    # penalty path below handles any awarded first down).
                    # penalty_negated_play is TRI-STATE: null means the
                    # enforcement class is unknown, not that the play was
                    # wiped -- only a CONFIRMED wipe blocks the yards credit
                    # (null == False is null in polars and would silently
                    # drop the whole branch).
                    .and_(pl.col("penalty_negated_play").fill_null(False) == False)
                    .and_(pl.col("penalty_no_play").fill_null(False) == False),
                )
                .then(True)
                .when(
                    # 3) declined / offset penalty where the play result stood and
                    # converted (cfbfastR first_by_penalty branches 2-3; officially
                    # these are first downs by yards, so they bucket here)
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(
                        ((pl.col("penalty_offset") == True).and_(pl.col("statYardage") > pl.col("start.distance"))).or_(
                            (pl.col("penalty_offset") == False).and_(
                                pl.col("statYardage") >= pl.col("start.distance"),
                            ),
                        ),
                    ),
                )
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                first_down_penalty=(
                    (pl.col("penalty_1st_conv") == True).or_(
                        # accepted automatic-first-down foul by the DEFENSE whose
                        # text lacks the "1st down" phrase (era/vendor dependent).
                        # NCAA automatic-first-down fouls ONLY: DPI, roughing the
                        # passer/kicker/snapper, targeting. Personal foul /
                        # facemask / horse collar are 15 yards WITHOUT an
                        # automatic first down in NCAA (unlike the NFL) and were
                        # measured to over-fire. Gated on the text-resolved
                        # penalized_team (binary home/away match) -- this branch
                        # was rejected while attribution ran ~50% reliable and
                        # re-added once the token matcher fixed it.
                        (
                            pl.col("text")
                            .str.contains(
                                r"(?i)pass interference|roughing the (?:passer|kicker|snapper)|targeting",
                            )
                            .fill_null(False)
                        )
                        .and_(pl.col("text").str.contains("(?i)penalty").fill_null(False))
                        .and_(pl.col("penalty_declined") == False)
                        .and_(pl.col("penalty_offset") == False)
                        .and_(pl.col("penalized_team").is_not_null())
                        .and_(pl.col("penalized_team") != pl.col("pos_team"))
                        .and_(pl.col("change_of_pos_team") == False)
                        .and_(pl.col("type.text").is_in([*end_rows, "Timeout"]) == False),
                    )
                )
                .and_(pl.col("first_down_yards") == False)
                # kickoff rows only: pos_team flips on kickoffs so a penalty
                # first down there would mis-credit. Punt rows are NOT
                # excluded -- roughing the kicker/snapper happens on punts and
                # awards the KICKING team (= pos_team at snap) the first down.
                .and_(pl.col("kickoff_play") == False),
            )
            .with_columns(
                first_down_earned=(pl.col("first_down_yards") == True).or_(pl.col("first_down_penalty") == True),
            )
            .with_columns(
                drive_event_number=pl.col("drive_event").cast(pl.Int32).cum_sum().over("drive.id"),
                # lag_play_type is ungrouped in R (pbp_clean_pbp_dat.R L388-390);
                # fill "" so is_in() is False like R's NA %in% -> FALSE.
                lag_play_type=pl.col("type.text").shift(1).fill_null(""),
                lag_play_type2=pl.col("type.text").shift(2).fill_null(""),
                # Half-scoped event-flag lags, 0-filled at half starts. R fills
                # via half_event_number %in% 1..k; within-half shift(k) is null
                # exactly on those rows (End rows carry all-False flags, so the
                # half_play_number nuance on scoring_play is inert).
                lag_first_by_penalty=pl.col("first_by_penalty").shift(1).over("half").fill_null(False),
                lag_first_by_penalty2=pl.col("first_by_penalty").shift(2).over("half").fill_null(False),
                lag_first_by_penalty3=pl.col("first_by_penalty").shift(3).over("half").fill_null(False),
                lag_first_by_yards=pl.col("first_by_yards").shift(1).over("half").fill_null(False),
                lag_first_by_yards2=pl.col("first_by_yards").shift(2).over("half").fill_null(False),
                lag_first_by_yards3=pl.col("first_by_yards").shift(3).over("half").fill_null(False),
                lag_change_of_pos_team_srs=pl.col("change_of_pos_team").shift(1).over("half").fill_null(False),
                lag_change_of_pos_team2_srs=pl.col("change_of_pos_team").shift(2).over("half").fill_null(False),
                lag_change_of_pos_team3_srs=pl.col("change_of_pos_team").shift(3).over("half").fill_null(False),
                lag_kickoff_play=pl.col("kickoff_play").shift(1).over("half").fill_null(False),
                lag_kickoff_play2=pl.col("kickoff_play").shift(2).over("half").fill_null(False),
                lag_punt=pl.col("punt").shift(1).over("half").fill_null(False),
                lag_punt2=pl.col("punt").shift(2).over("half").fill_null(False),
                lag_downs_turnover=pl.col("downs_turnover").shift(1).over("half").fill_null(False),
                lag_downs_turnover2=pl.col("downs_turnover").shift(2).over("half").fill_null(False),
                lag_turnover_vec=pl.col("turnover_vec").shift(1).over("half").fill_null(False),
                lag_turnover_vec2=pl.col("turnover_vec").shift(2).over("half").fill_null(False),
                lag_scoring_play=pl.col("scoring_play").shift(1).over("half").fill_null(False),
                lag_scoring_play2=pl.col("scoring_play").shift(2).over("half").fill_null(False),
                half_row=pl.int_range(pl.len()).over("half"),
                lag_drive_id=pl.col("drive.id").shift(1).over("half"),
            )
            .with_columns(
                new_series=pl.when(pl.col("half_row") == 0)
                .then(True)
                .when(pl.col("drive.id") != pl.col("lag_drive_id"))
                .then(True)
                .when(pl.col("lag_first_by_yards") == True)
                .then(True)
                .when(pl.col("lag_first_by_penalty") == True)
                .then(True)
                .otherwise(False),
                # R: kickoff_play == 1 & down == 1. ESPN's raw start.down on
                # kickoff rows is era-dependent junk (0 pre-normalization) and
                # __process_epa's kick_mask later substitutes down=1 on every
                # kickoff row -- the normalized-down semantics the R engine
                # sees natively on CFBD input -- so the down guard is vacuous.
                firstD_by_kickoff=pl.col("kickoff_play") == True,
                firstD_by_poss=pl.when(
                    # 1-L.I) start by play after kickoff
                    (pl.col("lag_play_type").is_in(to_ep) == False)
                    .and_(pl.col("drive_event_number") == 2)
                    .and_(pl.col("lag_kickoff_play") == True)
                    .and_(pl.col("kickoff_play") == False),
                )
                .then(True)
                .when(
                    # 1-L.II) same, one non-play event in between
                    (pl.col("lag_play_type").is_in(to_ep))
                    .and_(pl.col("lag_play_type2").is_in(to_ep) == False)
                    .and_(pl.col("drive_event_number") == 3)
                    .and_(pl.col("lag_kickoff_play2") == True)
                    .and_(pl.col("kickoff_play") == False),
                )
                .then(True)
                .when(
                    # 2-L.I) start by change of pos_team
                    (pl.col("lag_play_type").is_in(to_ep) == False)
                    .and_(pl.col("lag_change_of_pos_team_srs") == True)
                    .and_(
                        (pl.col("lag_punt") == True)
                        .or_(pl.col("lag_downs_turnover") == True)
                        .or_(pl.col("lag_turnover_vec") == True),
                    ),
                )
                .then(True)
                .when(
                    # 2-L.II) same, one non-play event in between
                    (pl.col("lag_play_type").is_in(to_ep))
                    .and_(pl.col("lag_change_of_pos_team2_srs") == True)
                    .and_(pl.col("lag_play_type2").is_in(to_ep) == False)
                    .and_(
                        (pl.col("lag_punt2") == True)
                        .or_(pl.col("lag_downs_turnover2") == True)
                        .or_(pl.col("lag_turnover_vec2") == True),
                    ),
                )
                .then(True)
                .when(
                    # 3-L.I) start by opponent scoring play
                    (pl.col("lag_play_type").is_in(to_ep) == False)
                    .and_(pl.col("lag_scoring_play") == True)
                    .and_(pl.col("kickoff_play") == False),
                )
                .then(True)
                .when(
                    # 3-L.II) same, one non-play event in between
                    (pl.col("lag_play_type").is_in(to_ep))
                    .and_(pl.col("lag_play_type2").is_in(to_ep) == False)
                    .and_(pl.col("lag_scoring_play2") == True)
                    .and_(pl.col("kickoff_play") == False),
                )
                .then(True)
                .when(
                    # 4) start-of-half plays start drives
                    (pl.col("drive_event_number") == 1).and_(pl.col("kickoff_play") == False),
                )
                .then(True)
                .otherwise(False),
                firstD_by_penalty=pl.when(
                    (pl.col("lag_first_by_penalty") == True)
                    .and_(pl.col("lag_change_of_pos_team_srs") == False)
                    .and_(pl.col("lag_play_type").is_in(to_ep) == False),
                )
                .then(True)
                .when(
                    (pl.col("lag_first_by_penalty2") == True)
                    .and_(pl.col("lag_change_of_pos_team2_srs") == False)
                    .and_(pl.col("lag_play_type").is_in(to_ep)),
                )
                .then(True)
                .when(
                    (pl.col("lag_first_by_penalty3") == True)
                    .and_(pl.col("lag_change_of_pos_team3_srs") == False)
                    .and_(pl.col("lag_play_type").is_in(to_ep))
                    .and_(pl.col("lag_play_type2").is_in(to_ep)),
                )
                .then(True)
                .otherwise(False),
                firstD_by_yards=pl.when(
                    (pl.col("lag_first_by_yards") == True)
                    .and_(pl.col("lag_change_of_pos_team_srs") == False)
                    .and_(pl.col("lag_play_type").is_in(to_ep) == False),
                )
                .then(True)
                .when(
                    (pl.col("lag_first_by_yards2") == True)
                    .and_(pl.col("lag_change_of_pos_team2_srs") == False)
                    .and_(pl.col("lag_play_type").is_in(to_ep)),
                )
                .then(True)
                .when(
                    (pl.col("lag_first_by_yards3") == True)
                    .and_(pl.col("lag_change_of_pos_team3_srs") == False)
                    .and_(pl.col("lag_play_type").is_in(to_ep))
                    .and_(pl.col("lag_play_type2").is_in(to_ep)),
                )
                .then(True)
                .otherwise(False),
            )
            .drop(
                "first_by_penalty",
                "first_by_yards",
                "drive_event",
                "drive_event_number",
                "lag_play_type",
                "lag_play_type2",
                "lag_first_by_penalty",
                "lag_first_by_penalty2",
                "lag_first_by_penalty3",
                "lag_first_by_yards",
                "lag_first_by_yards2",
                "lag_first_by_yards3",
                "lag_change_of_pos_team_srs",
                "lag_change_of_pos_team2_srs",
                "lag_change_of_pos_team3_srs",
                "lag_kickoff_play",
                "lag_kickoff_play2",
                "lag_punt",
                "lag_punt2",
                "lag_downs_turnover",
                "lag_downs_turnover2",
                "lag_turnover_vec",
                "lag_turnover_vec2",
                "lag_scoring_play",
                "lag_scoring_play2",
                "half_row",
                "lag_drive_id",
            )
        )
        return play_df

    def __add_xp_suffix_cols(self, play_df):
        """Extra points 2014+ -- parsed from the TD play's parenthetical suffix.

        Through 2013 an extra point is its own row (``Extra Point Good/Missed``,
        ~6/game); from 2014 there is NO XP row -- the try lives as a suffix on
        the touchdown play text: ``(Nick Rice KICK)`` / ``(... PAT MISSED)`` /
        ``(... PAT BLOCKED)``, plus a title-case ``(Name Kick)`` form in
        summary-only games and a 2025 stats.ncaa inline form
        ``#49 G.Meadors kick attempt good|failed``. Validated against the ESPN
        kicking box per (game, kicker): xpm 94.6-97.3% / xpa 93.0-95.1% exact
        for every season 2014-2025, from a 9% baseline. Pre-2014 rows are
        untouched (the paren patterns fire zero times there; the NCAA inline
        pattern is season-gated because 2010-2013 old-NCAA texts contain it for
        XPs that already have their own rows).
        """
        if "text" not in play_df.columns or "season" not in play_df.columns:
            return play_df

        xp_made_re = r"(?i)\(\s*([^()]*?)\s+kick\s*\)"
        xp_miss_kw_re = r"(?i)\(\s*([^()]*?)\s*\b(?:pat|kick)\s+(?:missed|blocked|failed)\s*\)"
        xp_miss_bare_re = r"(?i)\(\s*([^()]+?)\s+(?:missed|blocked)\s*\)"
        xp_ncaa_re = r"(?i)(?:#\d+\s+)?([A-Z][\w'\.\-]*(?:\s[A-Z][\w'\.\-]+)*)\s+kick attempt\s+(good|failed)"

        is_td_row = (pl.col("td_play") == True) | pl.col("type.text").str.contains("(?i)touchdown")  # noqa: E712
        gated = (pl.col("season") >= 2014) & is_td_row & pl.col("text").is_not_null()

        made_name = pl.col("text").str.extract(xp_made_re, 1)
        miss_kw_name = pl.col("text").str.extract(xp_miss_kw_re, 1)
        miss_bare_name = pl.col("text").str.extract(xp_miss_bare_re, 1)
        ncaa_name = pl.col("text").str.extract(xp_ncaa_re, 1)
        ncaa_result = pl.col("text").str.extract(xp_ncaa_re, 2)

        def _guard(e):
            # a captured "name" that is really a 2pt phrase is nulled
            return pl.when(e.str.contains("(?i)two|conversion")).then(pl.lit(None, dtype=pl.Utf8)).otherwise(e)

        # fill_null(False) is load-bearing: Kleene logic makes `False | null` null,
        # and (ncaa_result == "good") is null wherever the pattern did not match.
        made = (made_name.is_not_null() | (ncaa_result == pl.lit("good"))).fill_null(False)
        missed = (
            miss_kw_name.is_not_null() | _guard(miss_bare_name).is_not_null() | (ncaa_result == pl.lit("failed"))
        ).fill_null(False)
        kicker = pl.coalesce(made_name, miss_kw_name, _guard(miss_bare_name), ncaa_name).str.strip_chars()

        return play_df.with_columns(
            pl.when(gated).then(made | missed).otherwise(False).alias("xp_attempt"),
            pl.when(gated).then(made).otherwise(False).alias("xp_made"),
            pl.when(gated & (made | missed))
            .then(pl.when(kicker.str.len_chars() > 0).then(kicker))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("xp_kicker_player_name"),
        )

    def __add_attribution_cols(self, play_df):
        """Resolve the credited team per play (spec section 5).

        Pure/deterministic. Reads pos_team/def_pos_team + play-type flags +
        text, writes kicking_team/return_team, fumble_or_muff, fumbling_team,
        recovery_team, turnover_team, is_turnover, is_st_turnover,
        penalized_team, penalty_yards_signed, and event-team columns.
        """
        # The enforcement-spot resolver reads two columns the full pipeline always
        # has but the synthetic frames in tests/cfb/test_cfb_attribution.py do not --
        # those carry only what the function under test needs. Supply them as nulls
        # so the resolver degrades to "unresolved" instead of raising, matching how
        # penalty_assessed_on_kickoff is guarded in _apply_wp_derivation.
        for _col, _dtype in (
            ("penalty_text", pl.Utf8),
            ("start.pos_team.id", pl.Int64),
            ("season", pl.Int64),
            ("penalty_1st_conv", pl.Boolean),
            ("penalty_declined", pl.Boolean),
            ("penalty_offset", pl.Boolean),
            ("penalty_no_play", pl.Boolean),
        ):
            if _col not in play_df.columns:
                play_df = play_df.with_columns(pl.lit(None, dtype=_dtype).alias(_col))
        play_df = play_df.with_columns(
            # --- Special-teams team flip (verified): kickoff pos_team=receiving;
            #     punt/FG pos_team=kicking. ---
            kicking_team=pl.when(pl.col("kickoff_play") == True)
            .then(pl.col("def_pos_team"))
            .when((pl.col("punt") == True) | (pl.col("fg_attempt") == True))
            .then(pl.col("pos_team"))
            .otherwise(None),
            return_team=pl.when(pl.col("kickoff_play") == True)
            .then(pl.col("pos_team"))
            .when((pl.col("punt") == True) | (pl.col("fg_attempt") == True))
            .then(pl.col("def_pos_team"))
            .otherwise(None),
            # --- Widen fumble detection to include muffs (finding #14) ---
            fumble_or_muff=pl.when(
                (pl.col("fumble_vec") == True) | (pl.col("text").str.contains(r"(?i)muff")),
            )
            .then(True)
            .otherwise(False),
        )

        # --- Cleaned text + ordered recovery chain (strip overturned first) ---
        play_df = (
            play_df.with_columns(
                _clean_text=pl.col("text").map_elements(_strip_overturned_text, return_dtype=pl.Utf8),
            )
            .with_columns(
                _rec_abbrevs=pl.col("_clean_text").map_elements(
                    _parse_recovery_abbrevs,
                    return_dtype=pl.List(pl.Utf8),
                ),
            )
            .with_columns(
                _recovery_abbrev=pl.col("_rec_abbrevs").list.get(0, null_on_oob=True),
                _recovery_abbrev_2=pl.col("_rec_abbrevs").list.get(1, null_on_oob=True),
            )
        )

        # abbrev -> team id using the per-play home/away abbreviations (1st and 2nd recovery).
        # ESPN ships two abbreviation forms for some teams -- the play text uses one (e.g.
        # "recovered by BUF") while homeTeamAbbrev/awayTeamAbbrev carry another ("BUFF"). Match
        # prefix-tolerantly (either is a prefix of the other) so these variants still resolve.
        # In a two-team game cross-opponent prefix collisions are effectively nonexistent.
        _home_u = pl.col("homeTeamAbbrev").str.to_uppercase()
        _away_u = pl.col("awayTeamAbbrev").str.to_uppercase()

        def _abbrev_to_team_id(abbr_col):
            return (
                pl.when(abbr_col.is_null())
                .then(pl.lit(None, dtype=pl.Int64))
                .when(_abbr_compat(abbr_col, _home_u))
                .then(pl.col("homeTeamId"))
                .when(_abbr_compat(abbr_col, _away_u))
                .then(pl.col("awayTeamId"))
                .otherwise(pl.lit(None, dtype=pl.Int64))
            )

        # The penalty-side matcher consumes the team NAME columns too; synthetic
        # test frames only carry the abbrevs, so null-fill any that are absent
        # (matching then degrades gracefully to abbrev-only).
        for _c in (
            "homeTeamName",
            "awayTeamName",
            "homeTeamNameAlt",
            "awayTeamNameAlt",
            "homeTeamMascot",
            "awayTeamMascot",
        ):
            if _c not in play_df.columns:
                play_df = play_df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(_c))

        play_df = (
            play_df.with_columns(
                recovery_team=_abbrev_to_team_id(pl.col("_recovery_abbrev")),
                recovery_team_2=_abbrev_to_team_id(pl.col("_recovery_abbrev_2")),
                # Penalized team parsed from the penalty-team text token (both vendor
                # forms: "PENALTY {TEAM} ..." and "{TEAM} Penalty, ..."). Matched
                # binary home-vs-away against abbrev/name/alt/mascot candidates, so
                # it correctly distinguishes offensive vs defensive fouls (incl.
                # OPI vs DPI) even when the token is a vowel-dropped vendor
                # spelling. Replaces the earlier expression-based era-form matcher:
                # measured on the 13-game box oracle, this resolver scores 61.5%
                # count / 73.1% yards exact vs 46.2% / 50.0% for the era-form one,
                # and resolves 98.6% of the 2025 season's 11,844 penalty rows.
                _penalty_side=pl.struct(
                    [
                        "text",
                        "homeTeamAbbrev",
                        "awayTeamAbbrev",
                        "homeTeamName",
                        "awayTeamName",
                        "homeTeamNameAlt",
                        "awayTeamNameAlt",
                        "homeTeamMascot",
                        "awayTeamMascot",
                    ],
                ).map_elements(_parse_penalty_team_side, return_dtype=pl.Utf8),
                # The enforcement SPOT, resolved the same way and for the same reason:
                # "to the FlaSt 23" names a yardline in one team's half, and which half
                # decides whether the offense has 23 yards to go or 77. Reading the spot
                # as a distance is what published a 15-yard flag against the receiving
                # team as a +5.52 EPA gain FOR it. Kept as "<side>:<yardline>" and turned
                # into a distance below, once the possessing side is known.
                _penalty_spot=pl.struct(
                    [
                        "penalty_text",
                        "homeTeamAbbrev",
                        "awayTeamAbbrev",
                        "homeTeamName",
                        "awayTeamName",
                        "homeTeamNameAlt",
                        "awayTeamNameAlt",
                        "homeTeamMascot",
                        "awayTeamMascot",
                    ],
                ).map_elements(_parse_penalty_spot, return_dtype=pl.Utf8),
            )
            .with_columns(
                _penalty_team=pl.when(pl.col("_penalty_side") == "home")
                .then(pl.col("homeTeamId"))
                .when(pl.col("_penalty_side") == "away")
                .then(pl.col("awayTeamId"))
                .otherwise(pl.lit(None, dtype=pl.Int64)),
            )
            .with_columns(
                penalty_spot_yardline=pl.col("_penalty_spot").str.extract(r":(\d+)$", 1).cast(pl.Int32, strict=False),
                penalty_spot_side=pl.col("_penalty_spot").str.extract(r"^(home|away|mid)", 1),
            )
            .with_columns(
                # The spot as a distance to the possessing team's target endzone -- the
                # form the EP model consumes. Midfield is its own complement, so it needs
                # no side. Null when the side could not be resolved: an unresolved spot
                # must stay absent rather than be guessed, because guessing wrong mirrors
                # the field position and the resulting EP error is ~6 points.
                penalty_spot_yardsToEndzone=pl.when(pl.col("penalty_spot_side") == "mid")
                .then(pl.lit(50, dtype=pl.Int32))
                .when(
                    (pl.col("penalty_spot_side") == "home").and_(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                    | (pl.col("penalty_spot_side") == "away").and_(pl.col("start.pos_team.id") != pl.col("homeTeamId")),
                )
                .then((pl.lit(100) - pl.col("penalty_spot_yardline")).cast(pl.Int32))
                .when(pl.col("penalty_spot_side").is_in(["home", "away"]))
                .then(pl.col("penalty_spot_yardline").cast(pl.Int32))
                .otherwise(None),
            )
        )

        # Special-teams RETURN detection (flag OR text). ESPN sometimes reclassifies a
        # punt/kickoff return fumble to a "Fumble Recovery (...)" type and DROPS the
        # punt/kickoff_play flags (so sp becomes False), so a text fallback is required to
        # recover the special-teams nature and attribute the fumble to the returning team.
        play_df = play_df.with_columns(
            _is_kick_return=pl.when(
                (pl.col("kickoff_play") == True)
                | (pl.col("text").str.contains(r"(?i)kickoff") & pl.col("text").str.contains(r"(?i)return|muff")),
            )
            .then(True)
            .otherwise(False),
            _is_punt_return=pl.when(
                (pl.col("punt") == True)
                | (
                    pl.col("text").str.contains(r"(?i)punt")
                    & pl.col("text").str.contains(r"(?i)return|muff|fair catch")
                ),
            )
            .then(True)
            .otherwise(False),
        )

        # fumbling team (the team that HAD the ball when the fumble/muff occurred):
        #  - interception return: the intercepting team == def_pos_team (an INT that is
        #    returned and then fumbled is a SECOND, opposite-direction turnover)
        #  - kickoff return: receiving team == pos_team (kickoff pos_team=receiving)
        #  - punt return:    receiving team == def_pos_team (punt pos_team=kicking team)
        #  - other sp (e.g. blocked-FG return): return_team
        #  - scrimmage:      the offense == pos_team
        # The kick/punt cases use the text-or-flag detection above, so a reclassified
        # return fumble (punt/kickoff flags dropped, pos_team flipped to the recovering
        # team) still resolves the fumbling team to the side that was returning the kick.
        play_df = play_df.with_columns(
            fumbling_team=pl.when(pl.col("fumble_or_muff") == False)
            .then(pl.lit(None, dtype=pl.Int64))
            .when(pl.col("int") == True)
            .then(pl.col("def_pos_team"))
            .when(pl.col("_is_kick_return") == True)
            .then(pl.col("pos_team"))
            .when(pl.col("_is_punt_return") == True)
            .then(pl.col("def_pos_team"))
            .when(pl.col("sp") == True)
            .then(pl.col("return_team"))
            .otherwise(pl.col("pos_team")),
        )

        # Possession-chain turnover model (per side). A single play can change hands more
        # than once -- offense fumbles (defense recovers), defense fumbles on the return
        # (offense recovers), OR an interception that is returned and fumbled back. Walk the
        # recovery chain: the first holder is `fumbling_team`; recovery_team / recovery_team_2
        # are the next holders. A fumble-lost is charged each time the holder changes. Flags
        # are framed PER SIDE (offense=pos_team, defense=def_pos_team) so BOTH teams can
        # register a turnover on one play, matching the official box's per-event accounting.
        play_df = (
            play_df.with_columns(
                # loser of the 1st fumble: the first holder, when the next holder differs
                _loser_1=pl.when(
                    (pl.col("fumble_or_muff") == True)
                    & (pl.col("recovery_team").is_not_null())
                    & (pl.col("fumbling_team").is_not_null())
                    & (pl.col("recovery_team") != pl.col("fumbling_team")),
                )
                .then(pl.col("fumbling_team"))
                .when(
                    # last-resort possession-change fallback: scrimmage offense fumbles only
                    (pl.col("fumble_or_muff") == True)
                    & (pl.col("recovery_team").is_null())
                    & (pl.col("scrimmage_play") == True)
                    & (pl.col("int") == False)
                    & (pl.col("_is_punt_return") == False)
                    & (pl.col("_is_kick_return") == False)
                    & (pl.col("fumbling_team") == pl.col("pos_team"))
                    & (pl.col("end.pos_team.id") != pl.col("pos_team")),
                )
                .then(pl.col("fumbling_team"))
                .otherwise(pl.lit(None, dtype=pl.Int64)),
                # loser of the 2nd fumble: the 1st recoverer, when the 2nd recoverer differs
                _loser_2=pl.when(
                    (pl.col("fumble_or_muff") == True)
                    & (pl.col("recovery_team_2").is_not_null())
                    & (pl.col("recovery_team").is_not_null())
                    & (pl.col("recovery_team_2") != pl.col("recovery_team")),
                )
                .then(pl.col("recovery_team"))
                .otherwise(pl.lit(None, dtype=pl.Int64)),
                int_turnover=pl.col("int") == True,
            )
            .with_columns(
                pos_fumble_lost=((pl.col("_loser_1") == pl.col("pos_team")).fill_null(False))
                | ((pl.col("_loser_2") == pl.col("pos_team")).fill_null(False)),
                def_fumble_lost=((pl.col("_loser_1") == pl.col("def_pos_team")).fill_null(False))
                | ((pl.col("_loser_2") == pl.col("def_pos_team")).fill_null(False)),
            )
            .with_columns(
                # per-side turnover flags (a play may set BOTH)
                is_pos_team_turnover=(pl.col("int_turnover") == True) | (pl.col("pos_fumble_lost") == True),
                is_def_pos_team_turnover=pl.col("def_fumble_lost") == True,
            )
            .with_columns(
                # back-compat single-flag view + primary losing team (pos side preferred)
                is_turnover=(pl.col("is_pos_team_turnover") == True) | (pl.col("is_def_pos_team_turnover") == True),
                turnover_team=pl.when(pl.col("is_pos_team_turnover") == True)
                .then(pl.col("pos_team"))
                .when(pl.col("is_def_pos_team_turnover") == True)
                .then(pl.col("def_pos_team"))
                .otherwise(pl.lit(None, dtype=pl.Int64)),
                # special-teams turnover = a fumble lost on a kick/punt (INTs are never ST)
                is_st_turnover=pl.when(
                    ((pl.col("pos_fumble_lost") == True) | (pl.col("def_fumble_lost") == True))
                    & (
                        (pl.col("sp") == True)
                        | (pl.col("_is_punt_return") == True)
                        | (pl.col("_is_kick_return") == True)
                    ),
                )
                .then(True)
                .otherwise(False),
                # Blocked-punt possession loss. ESPN's OFFICIAL box counts only giveaways
                # (INT + fumbles lost), so blocked punts are deliberately kept OUT of
                # is_turnover / is_st_turnover to preserve the *_pbp == espn_team
                # reconciliation. This standalone flag surfaces the one possession-losing
                # class that ESPN's per-play `isTurnover` flag catches and the giveaway-based
                # derivation does not: a blocked-punt TD is always a turnover; a non-TD
                # blocked punt is one only when possession actually changed (the defense --
                # not the kicking team -- recovered). (Blocked FGs already yield possession
                # via the normal missed-FG path, so they are out of scope here.)
                is_blocked_punt_turnover=pl.when(pl.col("type.text") == "Blocked Punt Touchdown")
                .then(True)
                .when((pl.col("type.text") == "Blocked Punt").and_(pl.col("change_of_poss") == True))
                .then(True)
                .otherwise(False),
                # Blocked-FG possession loss -- same rationale as is_blocked_punt_turnover:
                # the official box counts only giveaways, so this stays OUT of is_turnover /
                # is_st_turnover. True on a Blocked Field Goal Touchdown (defense scored) or a
                # non-TD Blocked Field Goal the defense recovered (change_of_poss). Keys on the
                # already-corrected type.text from __add_new_play_types.
                is_blocked_fg_turnover=pl.when(pl.col("type.text") == "Blocked Field Goal Touchdown")
                .then(True)
                .when((pl.col("type.text") == "Blocked Field Goal").and_(pl.col("change_of_poss") == True))
                .then(True)
                .otherwise(False),
            )
        )

        # event -> credited team (spec 5.2)
        play_df = (
            play_df.with_columns(
                sack_team=pl.col("def_pos_team"),
                interception_team=pl.col("def_pos_team"),
                pass_breakup_team=pl.col("def_pos_team"),
                # #93: a forced fumble is credited to the side OPPOSITE the fumbling
                # player -- on scrimmage plays that is the defense, but on punt/kick/INT
                # returns the fumbler is the returner and the forcer is on the covering
                # (possession) team. Fall back to def_pos_team when the fumbling side
                # could not be parsed.
                forced_fumble_team=pl.when(pl.col("fumbling_team").is_null())
                .then(pl.col("def_pos_team"))
                .when(pl.col("fumbling_team") == pl.col("pos_team"))
                .then(pl.col("def_pos_team"))
                .otherwise(pl.col("pos_team")),
                # Team that recovered the fumble/muff. Prefer the parsed recovering-team
                # abbreviation; when it does not parse, fall back to the gaining team (the
                # side opposite the fumbling team) for turnovers, or the fumbling team for
                # own recoveries -- so a recovery is never dropped just because the team
                # abbreviation in the text could not be matched.
                fumble_recovery_team=pl.when(pl.col("recovery_team").is_not_null())
                .then(pl.col("recovery_team"))
                .when(pl.col("fumble_or_muff") == False)
                .then(pl.lit(None, dtype=pl.Int64))
                .when(pl.col("is_turnover") == True)
                .then(
                    pl.when(pl.col("fumbling_team") == pl.col("pos_team"))
                    .then(pl.col("def_pos_team"))
                    .otherwise(pl.col("pos_team")),
                )
                .otherwise(pl.col("fumbling_team")),
                punt_return_team=pl.col("return_team"),
                kick_return_team=pl.col("return_team"),
                fg_team=pl.col("kicking_team"),
                punt_team=pl.col("kicking_team"),
                # Team resolution, best evidence first (each layer only fills what
                # the prior left):
                #   1. the binary home/away text resolver (_penalty_team via
                #      _parse_penalty_team_side) -- resolves 98.6% of 2025 rows
                #   2. foul-direction heuristic (_DEFENSIVE_PENALTIES)
                # Measured at 13-game team-box parity: 61.5% penalties / 73.1%
                # yards exact vs the era-form matcher's 46.2% / 50.0%.
                penalized_team=pl.when(pl.col("penalty_detail").is_null())
                .then(pl.lit(None, dtype=pl.Int32))
                .when(pl.col("_penalty_team").is_not_null())
                .then(pl.col("_penalty_team"))
                .when(pl.col("penalty_detail").is_in(list(_DEFENSIVE_PENALTIES)))
                .then(pl.col("def_pos_team"))
                .otherwise(pl.col("pos_team")),
                # yds_penalty arrives as a string from several extraction branches and
                # carries their punctuation ("(16", " 19", "(-5"): 8,745 of 29,892 penalty
                # rows in 2022-24. The numeric derivation below already recovers the value
                # from 95.8% of those, so this is hygiene on the published string rather
                # than a correctness fix -- but a column typed String should hold a number.
                yds_penalty=pl.col("yds_penalty").cast(pl.Utf8).str.extract(r"(-?\d+)"),
            )
            .with_columns(
                # Reject magnitudes no penalty enforcement can produce. The bound is
                # era-aware because what ESPN puts in this field changed with the text
                # template, and one threshold cannot serve both.
                #
                # 2014+ texts state the nominal penalty. NCAA's longest is 15 yards and
                # half-the-distance only reduces it; ESPN reports net spot change on pass
                # interference, which reaches ~25 (a 16-yard DPI and a half-the-distance
                # targeting both appear there). So 25 is the ceiling, and a bound at the
                # rulebook's 15 would discard real data.
                #
                # Pre-2014 texts state a NET figure that routinely exceeds that -- 29, 32,
                # 34 for a kickoff out of bounds ("kickoff for 62 yards out-of-bounds,
                # Ucla penalty 32 yard illegal procedure accepted"). Applying 25 there
                # discarded that era's convention as corrupt: 164 rows in 2007 alone,
                # ~278 across 2005-2013. Half the field is the invariant that still holds
                # -- an enforcement cannot move the ball further -- so 50.
                #
                # What the bound catches is the parser having seized a nearby number that
                # is not the penalty, or ESPN stating an impossible one:
                #
                #   56  "rush ... for a gain of 56 yards"            the rush's yardage
                #  -53  "field goal attempt from 53 yards"           the kick distance
                #   65  "kickoff 65 yards ... Offside offsetting"    offsetting is 0 yards
                #  -65  "unsportsmanlike conduct (-65 Yards)"        ESPN's own text
                #   35  "1035 yards to the ALBANY-1020"              ESPN's own text
                #
                # Publishing 0 says "no reliable penalty yardage", which is true;
                # publishing 56 asserts a 56-yard penalty, which is impossible.
                #
                # Residual: the pre-2014 26-50 band is not separable this way -- a genuine
                # net figure and a seized number look alike there. Splitting them needs the
                # value's provenance (text-stated vs derived from statYardage), which is
                # not tracked today. Logged, not guessed at.
                yds_penalty=pl.when(
                    pl.col("yds_penalty").cast(pl.Int32, strict=False).abs()
                    > pl.when(pl.col("season") >= 2014).then(pl.lit(25)).otherwise(pl.lit(50)),
                )
                .then(None)
                .otherwise(pl.col("yds_penalty")),
            )
            .with_columns(
                penalty_yards_signed=pl.col("yds_penalty")
                .cast(pl.Utf8)
                .str.extract(r"(-?\d+)")
                .cast(pl.Int32, strict=False)
                .fill_null(0),
            )
            .with_columns(
                # B10: which side the flag was on, resolved only when two INDEPENDENT
                # signals agree. penalty_yards_signed has a reliable magnitude
                # (96.8-97.2% against the observed no-play movement) but not a reliable
                # sign -- its raw sign agrees with the enforcement direction on 45% of
                # rows, and any single substitute is not much better: penalized_team
                # alone runs 91.2%, and a 9% sign error MIRRORS the yardage, the exact
                # defect class parts (c) through (e) repair.
                #
                # Three signals, each independent of the others' failure modes:
                #   L1  penalty_1st_conv       an automatic first down means the DEFENSE
                #                              was flagged -- 99.48% on 6,714 rows
                #   L2  one-directional fouls  rulebook identity, >=95% measured
                #   L3  penalized_team         the binary text resolver
                #
                # Any two that fire and agree decide it; otherwise null. Measured on
                # 19,608 labelled rows: 59.5% coverage at 99.62% accuracy. Null beats a
                # guess for the same reason as the enforcement spot: the cost of a wrong
                # side is a mirrored value, not a small error.
                # L1 only holds when the penalty itself produced the first down. On a
                # DECLINED flag the conversion came from the play standing -- "Offensive
                # Holding ... declined for a 1ST down" carries penalty_1st_conv=True
                # with the flag on the offense -- so declined and offsetting rows are
                # excluded from it.
                # ...and, more generally, only when the flag is the SOLE source of
                # the first down -- which is a no-play row. On a play that stands,
                # "pass complete for 36 yards ... for a 1ST down, AIR FORCE penalty
                # 10 yard Illegal Block" carries the conversion from the catch, and
                # L1 read the offensive foul as defensive. The 362-game validation
                # caught it as a sign disagreement with the counterfactual.
                _pen_L1=pl.when(
                    (pl.col("penalty_1st_conv") == True)
                    .and_(pl.col("penalty_no_play") == True)
                    .and_(pl.col("penalty_declined") == False)
                    .and_(pl.col("penalty_offset") == False),
                )
                .then(pl.lit("def"))
                .otherwise(None),
                _pen_L2=pl.when(pl.col("penalty_detail").is_in(list(_PENALTY_SIGN_DEF)))
                .then(pl.lit("def"))
                .when(pl.col("penalty_detail").is_in(list(_PENALTY_SIGN_OFF)))
                .then(pl.lit("off"))
                .otherwise(None),
                _pen_L3=pl.when(pl.col("penalized_team") == pl.col("start.pos_team.id"))
                .then(pl.lit("off"))
                .when(pl.col("penalized_team").is_not_null())
                .then(pl.lit("def"))
                .otherwise(None),
            )
            .with_columns(
                penalty_side=pl.when(
                    (pl.col("_pen_L1").is_not_null()).and_(pl.col("_pen_L1") == pl.col("_pen_L2"))
                    | (pl.col("_pen_L1").is_not_null()).and_(pl.col("_pen_L1") == pl.col("_pen_L3"))
                    | (pl.col("_pen_L2").is_not_null()).and_(pl.col("_pen_L2") == pl.col("_pen_L3")),
                )
                .then(pl.coalesce(pl.col("_pen_L1"), pl.col("_pen_L2"), pl.col("_pen_L3")))
                .otherwise(None),
            )
            .with_columns(
                # The signed yardage in the OFFENSE's perspective: positive when the
                # flag was on the defense (the offense gains ground), negative when on
                # the offense. This is the input the accepted-penalty counterfactual
                # needs; penalty_yards_signed keeps its historical semantics untouched.
                penalty_yards_net=pl.when(pl.col("penalty_side") == "def")
                .then(pl.col("penalty_yards_signed").abs())
                .when(pl.col("penalty_side") == "off")
                .then(-pl.col("penalty_yards_signed").abs())
                .otherwise(None),
            )
            .drop(["_pen_L1", "_pen_L2", "_pen_L3"])
        )

        # penalty_team_id: same value as penalized_team, exported under the
        # *_team_id naming every other join key uses. Kept Int32 to match the
        # frame's team keys (homeTeamId/awayTeamId/pos_team are Int32) -- one
        # canonical dtype per id within a dataset; consumers casting to Int64
        # at a boundary must cast BOTH sides and assert, as the validation
        # harness does.
        play_df = play_df.with_columns(penalty_team_id=pl.col("penalized_team"))

        # drop temp columns that must not leak into the output frame
        play_df = play_df.drop(
            [
                "_clean_text",
                "_rec_abbrevs",
                "_recovery_abbrev",
                "_recovery_abbrev_2",
                "_penalty_team",
                "_penalty_side",
                "_penalty_spot",
                "_is_kick_return",
                "_is_punt_return",
                "_loser_1",
                "_loser_2",
            ],
        )

        # cast all team-id-derived columns to Int32 for join compatibility
        _team_id_cols = [
            "kicking_team",
            "return_team",
            "recovery_team",
            "recovery_team_2",
            "fumbling_team",
            "turnover_team",
            "penalized_team",
            "sack_team",
            "interception_team",
            "pass_breakup_team",
            "forced_fumble_team",
            "fumble_recovery_team",
            "punt_return_team",
            "kick_return_team",
            "fg_team",
            "punt_team",
        ]
        play_df = play_df.with_columns([pl.col(c).cast(pl.Int32) for c in _team_id_cols])

        return play_df

    def __refine_play_types_post_attribution(self, play_df):
        """Correct two play-type labels that need the post-attribution turnover signal.

        ``__add_new_play_types`` runs *before* ``__add_attribution_cols``, so it can only
        key on ``change_of_poss`` -- which is ``True`` on **every** possession flip, not
        just turnovers. Two residual mislabels survive that the now-available
        ``is_turnover`` / ``recovery_team`` flags resolve:

        1. **Sack-strip the offense recovers itself.** ``change_of_poss`` is spuriously
           ``1`` (a returner/fumble id artifact), so the strip-sack rule relabeled it
           ``"Fumble Recovery (Opponent)"``. ``is_turnover`` is ``False`` (the ball never
           left the offense) -> restore ``"Fumble Recovery (Own)"``.
        2. **Punt-return fumble the punting team recovers.** The receiving team fumbled the
           return and the punting team (``pos_team`` on a punt) recovered -- a real
           special-teams turnover -- but the play stayed ``"Punt Return"`` because the
           punt-fumble rule keyed on ``type.text == "Punt"`` -> relabel
           ``"Punt Team Fumble Recovery"``.

        Only this class's own first-pass relabels are undone (guarded on
        ``orig_play_type``). The two frozen ``type.text``-derived columns read downstream
        are then recomputed so EPA/WPA stay consistent: ``downs_turnover`` (``normalplay``
        membership -- ``"Fumble Recovery (Own)"`` newly joins it) and ``pos_score_diff_end``
        (``end_change_vec`` membership). The EPA/WPA turnover sign-flips read
        ``type.text in end_change_vec`` *live*, so they self-heal; the box-score turnover
        totals are ESPN-sourced and unaffected. The recompute mirrors
        ``__add_play_category_flags`` (``downs_turnover`` and ``pos_score_diff_end``) and is
        idempotent for unrelabeled rows.
        """
        opp = ["Fumble Recovery (Opponent)", "Fumble Recovery (Opponent) Touchdown"]
        play_df = play_df.with_columns(
            pl.when(
                pl.col("type.text")
                .is_in(opp)
                .and_(pl.col("orig_play_type").is_in(opp) == False)
                .and_(pl.col("is_turnover") == False)
                .and_(pl.col("fumble_vec") == True),
            )
            .then(
                pl.when(pl.col("td_play") == True)
                .then(pl.lit("Fumble Recovery (Own) Touchdown"))
                .otherwise(pl.lit("Fumble Recovery (Own)")),
            )
            .when(
                (pl.col("punt") == True)
                .and_(pl.col("is_turnover") == True)
                .and_(pl.col("recovery_team") == pl.col("pos_team"))
                .and_(pl.col("type.text").is_in(["Punt", "Punt Return"]))
                .and_(pl.col("td_play") == False),
            )
            .then(pl.lit("Punt Team Fumble Recovery"))
            .otherwise(pl.col("type.text"))
            .alias("type.text"),
        )
        # Recompute the two frozen type.text-derived columns that EPA/WPA read.
        # Source of truth: __add_play_category_flags (downs_turnover; pos_score_diff_end).
        play_df = play_df.with_columns(
            downs_turnover=pl.when(
                (pl.col("type.text").is_in(normalplay))
                .and_(pl.col("statYardage") < pl.col("start.distance"))
                .and_(pl.col("start.down") == 4)
                .and_(pl.col("penalty_1st_conv") == False),
            )
            .then(True)
            .otherwise(False),
        )
        play_df = play_df.with_columns(
            pos_score_diff_end=pl.when(
                (
                    (pl.col("type.text").is_in(end_change_vec)).and_(
                        pl.col("start.pos_team.id") != pl.col("end.pos_team.id"),
                    )
                ).or_(pl.col("downs_turnover") == True),
            )
            .then(-1 * pl.col("pos_score_diff"))
            .otherwise(pl.col("pos_score_diff")),
        ).with_columns(
            pos_score_diff_end=pl.when(
                (pl.col("pos_score_pts").abs() >= 8)
                .and_(pl.col("scoring_play") == False)
                .and_(pl.col("change_of_pos_team") == False),
            )
            .then(pl.col("pos_score_diff_start"))
            .when(
                (pl.col("pos_score_pts").abs() >= 8)
                .and_(pl.col("scoring_play") == False)
                .and_(pl.col("change_of_pos_team") == True),
            )
            .then(-1 * pl.col("pos_score_diff_start"))
            .otherwise(pl.col("pos_score_diff_end")),
        )
        return play_df

    def __after_cols(self, play_df):
        play_df = (
            play_df.with_columns(
                new_down=pl.when(pl.col("type.text") == "Timeout")
                .then(pl.col("start.down"))
                .when((pl.col("type.text").is_in(penalty)).and_(pl.col("penalty_1st_conv") == True))
                .then(1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == False),
                )
                .then(pl.col("start.down"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") <= 3),
                )
                .then(pl.col("start.down") + 1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4),
                )
                .then(1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") >= pl.col("start.distance")),
                )
                .then(1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") <= 3),
                )
                .then(pl.col("start.down") + 1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4),
                )
                .then(1)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") >= pl.col("start.distance")),
                )
                .then(1)
                .otherwise(pl.col("start.down")),
                new_distance=pl.when(pl.col("type.text") == "Timeout")
                .then(pl.col("start.distance"))
                .when((pl.col("type.text").is_in(penalty)).and_(pl.col("penalty_1st_conv") == True))
                .then(10)
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == False),
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") <= 3),
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4),
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == True)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") >= pl.col("start.distance")),
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") <= 3),
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance"))
                    .and_(pl.col("start.down") == 4),
                )
                .then(pl.col("start.distance"))
                .when(
                    (pl.col("type.text").is_in(penalty))
                    .and_(pl.col("penalty_1st_conv") == False)
                    .and_(pl.col("penalty_offset") == False)
                    .and_(pl.col("penalty_declined") == True)
                    .and_(pl.col("statYardage") >= pl.col("start.distance")),
                )
                .then(pl.col("start.distance"))
                .otherwise(pl.col("start.distance")),
                middle_8=pl.when(
                    (pl.col("start.adj_TimeSecsRem") >= 1560).and_(pl.col("start.adj_TimeSecsRem") <= 2040),
                )
                .then(True)
                .otherwise(False),
                rz_play=pl.when(pl.col("start.yardLine") <= 20).then(True).otherwise(False),
                under_2=pl.when(pl.col("start.TimeSecsRem") <= 120).then(True).otherwise(False),
                goal_to_go=pl.when(pl.col("start.yardLine") <= 10).then(True).otherwise(False),
                scoring_opp=pl.when(pl.col("start.yardLine") <= 40).then(True).otherwise(False),
                stuffed_run=pl.when((pl.col("type.text") == "Rush").and_(pl.col("yds_rushed") <= 0))
                .then(True)
                .otherwise(False),
                stopped_run=pl.when((pl.col("type.text") == "Rush").and_(pl.col("yds_rushed") <= 2))
                .then(True)
                .otherwise(False),
                # An "opportunity" is a carry where the blocking did its job, i.e. the run
                # REACHED 4 yards -- cfbfastR's espn_cfb_15 oracle is
                # `opportunity_run = ((rush == 1) & (yds_rushed >= 4))`, and the sibling
                # cfb-data producer agrees. This was inverted to `<= 4`, which also made
                # opp_highlight_yards identically 0 (its gate demanded <= 4 rushing yards
                # while highlight yards only accrue from 4 up, so the two could never
                # co-occur).
                # Gate on `rush`, not `type.text == "Rush"`. The literal play-type
                # string excludes "Rushing Touchdown" and "Fumble Recovery (Own)"
                # rushes, so a 4-yard rushing TD was not counted as an opportunity;
                # cfbfastR's oracle gates on `rush == 1`. It also left
                # adj_rush_yardage null on those plays while line_yards below gates
                # on `rush`, so the decomposition disagreed with itself.
                opportunity_run=pl.when((pl.col("rush") == True).and_(pl.col("yds_rushed") >= 4))
                .then(True)
                .otherwise(False),
                highlight_run=pl.when((pl.col("rush") == True).and_(pl.col("yds_rushed") >= 8))
                .then(True)
                .otherwise(False),
                adj_rush_yardage=pl.when((pl.col("rush") == True).and_(pl.col("yds_rushed") > 8))
                .then(8)
                .when((pl.col("rush") == True).and_(pl.col("yds_rushed") <= 8))
                .then(pl.col("yds_rushed"))
                .otherwise(None),
            )
            .with_columns(
                line_yards=pl.when((pl.col("rush") == True).and_(pl.col("yds_rushed") < 0))
                .then(1.2 * pl.col("adj_rush_yardage"))
                .when((pl.col("rush") == True).and_(pl.col("yds_rushed") >= 0).and_(pl.col("yds_rushed") <= 3))
                .then(pl.col("adj_rush_yardage"))
                .when((pl.col("rush") == True).and_(pl.col("yds_rushed") >= 4).and_(pl.col("yds_rushed") <= 8))
                .then(3 + 0.5 * (pl.col("adj_rush_yardage") - 3))
                .when((pl.col("rush") == True).and_(pl.col("yds_rushed") >= 8))
                .then(5.5)
                .otherwise(None),
                second_level_yards=pl.when((pl.col("rush") == True).and_(pl.col("yds_rushed") >= 4))
                .then(0.5 * (pl.col("adj_rush_yardage") - 4))
                .when(pl.col("rush") == True)
                .then(0)
                .otherwise(None),
                open_field_yards=pl.when((pl.col("rush") == True).and_(pl.col("yds_rushed") > 8))
                .then(pl.col("yds_rushed") - pl.col("adj_rush_yardage"))
                .when(pl.col("rush") == True)
                .then(0)
                .otherwise(None),
            )
            .with_columns(
                highlight_yards=pl.col("second_level_yards") + pl.col("open_field_yards"),
            )
            .with_columns(
                opp_highlight_yards=pl.when(pl.col("opportunity_run") == True)
                .then(pl.col("highlight_yards"))
                .when((pl.col("opportunity_run") == False).and_(pl.col("rush") == True))
                .then(0)
                .otherwise(None),
                short_rush_success=pl.when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("statYardage") >= pl.col("start.distance")),
                )
                .then(True)
                .when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("statYardage") < pl.col("start.distance")),
                )
                .then(False)
                .otherwise(None),
                short_rush_attempt=pl.when((pl.col("start.distance") < 2).and_(pl.col("rush") == True))
                .then(True)
                .when((pl.col("start.distance") >= 2).and_(pl.col("rush") == True))
                .then(False)
                .otherwise(None),
                power_rush_success=pl.when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("start.down").is_in([3, 4]))
                    .and_(pl.col("statYardage") >= pl.col("start.distance")),
                )
                .then(True)
                .when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("start.down").is_in([3, 4]))
                    .and_(pl.col("statYardage") < pl.col("start.distance")),
                )
                .then(False)
                .otherwise(None),
                power_rush_attempt=pl.when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("start.down").is_in([3, 4])),
                )
                .then(True)
                .when(
                    (pl.col("start.distance") < 2)
                    .and_(pl.col("rush") == True)
                    .and_(pl.col("start.down").is_in([3, 4])),
                )
                .then(False)
                .otherwise(None),
                early_down=pl.when(
                    ((pl.col("down_1") == True).or_(pl.col("down_2") == True)).and_(pl.col("scrimmage_play") == True),
                )
                .then(True)
                .otherwise(False),
                late_down=pl.when(
                    ((pl.col("down_3") == True).or_(pl.col("down_4"))).and_(pl.col("scrimmage_play") == True),
                )
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                early_down_pass=pl.when((pl.col("pass") == True).and_(pl.col("early_down") == True))
                .then(True)
                .otherwise(False),
                early_down_rush=pl.when((pl.col("rush") == True).and_(pl.col("early_down") == True))
                .then(True)
                .otherwise(False),
                late_down_pass=pl.when((pl.col("pass") == True).and_(pl.col("late_down") == True))
                .then(True)
                .otherwise(False),
                late_down_rush=pl.when((pl.col("rush") == True).and_(pl.col("late_down") == True))
                .then(True)
                .otherwise(False),
                standard_down=pl.when((pl.col("scrimmage_play") == True).and_(pl.col("down_1") == True))
                .then(True)
                .when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_2") == True)
                    .and_(pl.col("start.distance") < 8),
                )
                .then(True)
                .when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_3") == True)
                    .and_(pl.col("start.distance") < 5),
                )
                .then(True)
                .when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_4") == True)
                    .and_(pl.col("start.distance") < 5),
                )
                .then(True)
                .otherwise(False),
                passing_down=pl.when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_2") == True)
                    .and_(pl.col("start.distance") >= 8),
                )
                .then(True)
                .when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_3") == True)
                    .and_(pl.col("start.distance") >= 5),
                )
                .then(True)
                .when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("down_4") == True)
                    .and_(pl.col("start.distance") >= 5),
                )
                .then(True)
                .otherwise(False),
                TFL=pl.when(
                    (pl.col("type.text") != "Penalty").and_(pl.col("sp") == False).and_(pl.col("statYardage") < 0),
                )
                .then(True)
                .when(pl.col("sack_vec") == True)
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                TFL_pass=pl.when((pl.col("TFL") == True).and_(pl.col("pass") == True)).then(True).otherwise(False),
                TFL_rush=pl.when((pl.col("TFL") == True).and_(pl.col("rush") == True)).then(True).otherwise(False),
                havoc=pl.when(pl.col("pass_breakup") == True)
                .then(True)
                .when(pl.col("TFL") == True)
                .then(True)
                .when(pl.col("int") == True)
                .then(True)
                .when(pl.col("forced_fumble") == True)
                .then(True)
                .otherwise(False),
            )
        )
        return play_df

    def __add_spread_time(self, play_df):
        play_df = (
            play_df.with_columns(
                pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("homeTeamSpread"))
                .otherwise(-1 * pl.col("homeTeamSpread"))
                .alias("start.pos_team_spread"),
                ((3600 - pl.col("start.adj_TimeSecsRem")) / 3600).clip(0, 3600).alias("start.elapsed_share"),
            )
            .with_columns(
                (pl.col("start.pos_team_spread") * np.exp(-4 * pl.col("start.elapsed_share"))).alias(
                    "start.spread_time",
                ),
                pl.when(pl.col("end.pos_team.id") == pl.col("homeTeamId"))
                .then(pl.col("homeTeamSpread"))
                .otherwise(-1 * pl.col("homeTeamSpread"))
                .alias("end.pos_team_spread"),
                ((3600 - pl.col("end.adj_TimeSecsRem")) / 3600).clip(0, 3600).alias("end.elapsed_share"),
            )
            .with_columns(
                (pl.col("end.pos_team_spread") * np.exp(-4 * pl.col("end.elapsed_share"))).alias("end.spread_time"),
            )
        )
        return play_df

    def __calculate_ep_exp_val(self, matrix):
        return (
            matrix[:, 0] * ep_class_to_score_mapping[0]
            + matrix[:, 1] * ep_class_to_score_mapping[1]
            + matrix[:, 2] * ep_class_to_score_mapping[2]
            + matrix[:, 3] * ep_class_to_score_mapping[3]
            + matrix[:, 4] * ep_class_to_score_mapping[4]
            + matrix[:, 5] * ep_class_to_score_mapping[5]
            + matrix[:, 6] * ep_class_to_score_mapping[6]
        )

    def __process_epa(self, play_df):
        # B5 (0.36-live): a penalty assessed BETWEEN a scoring play and the ensuing
        # kickoff (the 2024 USC/LSU edge) inherits the prior play's field position and
        # earns a large spurious EPA/WPA. Flag it and give it the kickoff-touchback
        # treatment below. The ``penalty_flag`` guard excludes Timeouts that also sit
        # between a score and a kickoff (main already scores those EPA=0); the
        # ``kickoff_vec`` exclusion keeps the flag strictly disjoint from main's
        # existing ``kickoff_vec & penalty_in_text`` path (no double-handling).
        play_df = play_df.with_columns(
            penalty_assessed_on_kickoff=(
                (pl.col("scoring_play").shift(1) == True)
                .and_(pl.col("kickoff_play").shift(-1) == True)
                .and_(pl.col("end.pos_score_diff") != pl.col("start.pos_score_diff"))
                .and_(pl.col("penalty_flag") == True)
                .and_(pl.col("type.text").is_in(kickoff_vec) == False)
            ).fill_null(False),
        )
        # treat the flagged penalty like a kickoff for the start-state substitution
        kick_mask = (pl.col("type.text").is_in(kickoff_vec)).or_(pl.col("penalty_assessed_on_kickoff") == True)
        play_df = (
            play_df.with_columns(
                down=pl.when(kick_mask).then(1).otherwise(pl.col("start.down")),
                down_1=pl.when(kick_mask).then(True).otherwise(pl.col("down_1")),
                down_2=pl.when(kick_mask).then(False).otherwise(pl.col("down_2")),
                down_3=pl.when(kick_mask).then(False).otherwise(pl.col("down_3")),
                down_4=pl.when(kick_mask).then(False).otherwise(pl.col("down_4")),
                distance=pl.when(kick_mask).then(10).otherwise(pl.col("start.distance")),
            )
            .with_columns(
                pl.when(kick_mask).then(1).otherwise(pl.col("start.down")).alias("start.down"),
                pl.when(kick_mask).then(10).otherwise(pl.col("start.distance")).alias("start.distance"),
                pl.lit(99).alias("start.yardsToEndzone.touchback"),
            )
            .with_columns(
                pl.when((kick_mask).and_(pl.col("season") > 2013))
                .then(75)
                .when((kick_mask).and_(pl.col("season") <= 2013))
                .then(80)
                .otherwise(pl.col("start.yardsToEndzone"))
                .alias("start.yardsToEndzone.touchback"),
            )
            .with_columns(
                # B5: the flagged penalty's REAL start yardline is replaced by the
                # touchback yardline so EP_start / wp_before reflect the ensuing
                # kickoff, not the prior scoring play's field position. (Kickoffs keep
                # their real start yardline; only EP_start_touchback uses the 75/80.)
                pl.when(pl.col("penalty_assessed_on_kickoff") == True)
                .then(pl.col("start.yardsToEndzone.touchback"))
                .otherwise(pl.col("start.yardsToEndzone"))
                .alias("start.yardsToEndzone"),
            )
        )

        start_touchback_data = play_df[ep_start_touchback_columns]

        start_touchback_data.columns = ep_final_names
        # self.logger.info(start_data.iloc[[36]].to_json(orient="records"))

        dtest_start_touchback = DMatrix(start_touchback_data)
        EP_start_touchback_parts = ep_model.predict(dtest_start_touchback)
        EP_start_touchback = self.__calculate_ep_exp_val(EP_start_touchback_parts)

        start_data = play_df[ep_start_columns]
        start_data.columns = ep_final_names
        # self.logger.info(start_data.iloc[[36]].to_json(orient="records"))

        dtest_start = DMatrix(start_data)
        EP_start_parts = ep_model.predict(dtest_start)
        EP_start = self.__calculate_ep_exp_val(EP_start_parts)

        play_df = (
            play_df.with_columns(
                pl.when(pl.col("end.TimeSecsRem") <= 0)
                .then(0)
                .otherwise(pl.col("end.TimeSecsRem"))
                .alias("end.TimeSecsRem"),
            )
            .with_columns(
                pl.when((pl.col("end.TimeSecsRem") <= 0).and_(pl.col("period") < 5))
                .then(99)
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
                pl.when((pl.col("end.TimeSecsRem") <= 0).and_(pl.col("period") < 5))
                .then(True)
                .otherwise(pl.col("down_1_end"))
                .alias("down_1_end"),
                pl.when((pl.col("end.TimeSecsRem") <= 0).and_(pl.col("period") < 5))
                .then(False)
                .otherwise(pl.col("down_2_end"))
                .alias("down_2_end"),
                pl.when((pl.col("end.TimeSecsRem") <= 0).and_(pl.col("period") < 5))
                .then(False)
                .otherwise(pl.col("down_3_end"))
                .alias("down_3_end"),
                pl.when((pl.col("end.TimeSecsRem") <= 0).and_(pl.col("period") < 5))
                .then(False)
                .otherwise(pl.col("down_4_end"))
                .alias("down_4_end"),
            )
            .with_columns(
                # B2 (0.36-live): errored-format punt end-state. When a punt changed
                # possession, ESPN's end.yardsToEndzone is frequently wrong; substitute
                # the NEXT play's start yardline (the receiving team's actual field
                # position, same possessing-team perspective). Guarded off OOB punts
                # (ESPN yardline already correct) and penalty plays; the punt_tb
                # override below still wins for touchbacks. ``text`` (raw) carries the
                # leading "(MM:SS)" clock that marks the modern feed format.
                pl.when(
                    (pl.col("punt") == True)
                    .and_(pl.col("text").str.contains(r"^\(\d{1,2}:\d{2}\) "))
                    .and_(pl.col("start.pos_team.id") != pl.col("end.pos_team.id"))
                    .and_(pl.col("punt_oob") == False)
                    .and_(pl.col("penalty_in_text") == False),
                )
                .then(pl.col("start.yardsToEndzone").shift(-1))
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
            )
            .with_columns(
                pl.when(pl.col("end.yardsToEndzone") >= 100)
                .then(99)
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
            )
            .with_columns(
                pl.when(pl.col("end.yardsToEndzone") <= 0)
                .then(99)
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
            )
            .with_columns(
                # B5: the flagged penalty's END state is also a touchback (the ensuing
                # kickoff), alongside the existing kickoff_tb handling.
                pl.when((pl.col("kickoff_tb") == True).or_(pl.col("penalty_assessed_on_kickoff") == True))
                .then(75)
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
                pl.when((pl.col("kickoff_tb") == True).or_(pl.col("penalty_assessed_on_kickoff") == True))
                .then(1)
                .otherwise(pl.col("end.down"))
                .alias("end.down"),
                pl.when((pl.col("kickoff_tb") == True).or_(pl.col("penalty_assessed_on_kickoff") == True))
                .then(10)
                .otherwise(pl.col("end.distance"))
                .alias("end.distance"),
            )
            .with_columns(
                # B5: complete the END-state touchback for the flagged penalty by
                # resetting the EP model's *feature* columns (``down_*_end``,
                # ``pos_score_diff_end``) -- not just the display ``end.down`` above.
                # 0.36-live set only ``end.down``, which the EP model never reads, so a
                # penalty flagged on a 3rd-down play kept a 3rd-down end-state and
                # manufactured a phantom EPA; resetting the feature booleans makes
                # EP_end match EP_start (a true touchback -> EPA ~ 0). Flag-only: the
                # parity-validated kickoff_tb path is left untouched.
                pl.when(pl.col("penalty_assessed_on_kickoff") == True)
                .then(True)
                .otherwise(pl.col("down_1_end"))
                .alias("down_1_end"),
                pl.when(pl.col("penalty_assessed_on_kickoff") == True)
                .then(False)
                .otherwise(pl.col("down_2_end"))
                .alias("down_2_end"),
                pl.when(pl.col("penalty_assessed_on_kickoff") == True)
                .then(False)
                .otherwise(pl.col("down_3_end"))
                .alias("down_3_end"),
                pl.when(pl.col("penalty_assessed_on_kickoff") == True)
                .then(False)
                .otherwise(pl.col("down_4_end"))
                .alias("down_4_end"),
            )
            .with_columns(
                # B5: neutralize the spurious score-diff change ESPN attributes to the
                # penalty play. ``pos_score_diff_end`` is the EP end-state feature and
                # ``end.pos_score_diff`` the WP end-state feature (``wp_end_columns``);
                # set both to the start value so neither model sees a phantom scoring
                # swing on the penalty play.
                pl.when(pl.col("penalty_assessed_on_kickoff") == True)
                .then(pl.col("pos_score_diff_start"))
                .otherwise(pl.col("pos_score_diff_end"))
                .alias("pos_score_diff_end"),
                pl.when(pl.col("penalty_assessed_on_kickoff") == True)
                .then(pl.col("start.pos_score_diff"))
                .otherwise(pl.col("end.pos_score_diff"))
                .alias("end.pos_score_diff"),
            )
            .with_columns(
                pl.when(pl.col("punt_tb") == True).then(1).otherwise(pl.col("end.down")).alias("end.down"),
                pl.when(pl.col("punt_tb") == True).then(10).otherwise(pl.col("end.distance")).alias("end.distance"),
                pl.when(pl.col("punt_tb") == True)
                .then(80)
                .otherwise(pl.col("end.yardsToEndzone"))
                .alias("end.yardsToEndzone"),
            )
        )

        end_data = play_df[ep_end_columns]
        end_data.columns = ep_final_names
        # self.logger.info(end_data.iloc[[36]].to_json(orient="records"))
        dtest_end = DMatrix(end_data)
        EP_end_parts = ep_model.predict(dtest_end)

        EP_end = self.__calculate_ep_exp_val(EP_end_parts)

        # B11: the counterfactual end state for an ACCEPTED penalty on a scrimmage
        # play that STANDS -- where the play would have ended had there been no flag.
        # The penalty's own effect is then EP(actual end) - EP(counterfactual end),
        # which is what EPA_penalty is meant to isolate and what the play-level EPA
        # cannot, since that folds the play and the flag together.
        #
        # What the fields mean here, each verified on 2024 rather than assumed:
        #   - end.yardsToEndzone INCLUDES the enforcement on these rows (78% of
        #     rushes equal yds_rushed +/- the penalty), so it is the actual end.
        #   - yds_rushed / yds_receiving / yds_sacked are the PLAY's own yardage;
        #     statYardage is the NET and is not usable for this.
        #   - the counterfactual is start - play_yards, NOT end + penalty_yards_net.
        #     The two agree on a tack-on enforcement and disagree on a spot-of-foul
        #     one: a 22-yard run with offensive holding ends at start+3, and the
        #     flag's true cost is the 22 yards plus 10, not 10.
        #   - the model reads these eight columns by name, and the published
        #     end.* values ARE the model inputs (instrumented: 181/181 rows of the
        #     end call match the emitted frame), so the same frame is safe to build
        #     the counterfactual features from. down_*_end are NOT derived from
        #     end.down -- B5 resets the feature flags independently -- so the
        #     counterfactual flags are built from scratch below, never from end.down.
        #
        # Scope, deliberately narrow: rush, completion, incompletion and sack only;
        # accepted (not declined, not offsetting); not a no-play (those need no
        # counterfactual -- the start state is it); not a standalone "Penalty" row
        # (a dead-ball foul with no snap, same reason); not a scoring play, a
        # turnover, or an end-of-half row; and not a fourth-down non-conversion,
        # whose counterfactual is the other team's ball. Kicks and returns are out
        # -- their end states are the subject of parts (b)-(e) and not yet safe
        # to build on. Everything outside scope is null, not guessed.
        _cf_scope = (
            (pl.col("penalty_flag") == True)
            .and_(pl.col("penalty_no_play") == False)
            .and_(pl.col("penalty_declined") == False)
            .and_(pl.col("penalty_offset") == False)
            .and_(pl.col("type.text").is_in(["Penalty", "Penalty (Kickoff)"]) == False)
            .and_((pl.col("rush") == True).or_(pl.col("pass") == True))
            .and_(pl.col("scoring_play") != True)
            .and_(pl.col("is_turnover") != True)
            .and_(pl.col("end_of_half") != True)
            .and_(pl.col("start.yardsToEndzone").is_not_null())
            .and_(pl.col("start.distance").is_not_null())
            .and_(pl.col("start.down").is_not_null())
        )
        _play_yards = (
            pl.when(pl.col("sack") == True)
            .then(pl.col("yds_sacked"))
            .when(pl.col("rush") == True)
            .then(pl.col("yds_rushed"))
            .when(pl.col("completion") == True)
            .then(pl.col("yds_receiving"))
            .when(pl.col("pass") == True)
            .then(pl.lit(0))
            .otherwise(None)
        )
        play_df = play_df.with_columns(_cf_play_yards=_play_yards.cast(pl.Float64))
        play_df = play_df.with_columns(
            _cf_converted=(pl.col("_cf_play_yards") >= pl.col("start.distance")),
            _cf_y2ez=(pl.col("start.yardsToEndzone") - pl.col("_cf_play_yards")).clip(1, 99),
        )
        play_df = play_df.with_columns(
            # a fourth-down play that did not convert would have been the other
            # team's ball: out of scope rather than modelled
            _cf_ok=_cf_scope.and_(pl.col("_cf_play_yards").is_not_null()).and_(
                (pl.col("_cf_converted") == True).or_(pl.col("start.down") < 4),
            ),
            _cf_down=pl.when(pl.col("_cf_converted") == True).then(1).otherwise(pl.col("start.down") + 1),
            _cf_distance=pl.when(pl.col("_cf_converted") == True)
            .then(pl.min_horizontal(pl.lit(10.0), pl.col("_cf_y2ez")))
            .otherwise(pl.col("start.distance") - pl.col("_cf_play_yards")),
        )
        cf_data = play_df.select(
            pl.col("end.TimeSecsRem"),
            pl.col("_cf_y2ez"),
            pl.col("_cf_distance"),
            (pl.col("_cf_down") == 1).alias("_cf_d1"),
            (pl.col("_cf_down") == 2).alias("_cf_d2"),
            (pl.col("_cf_down") == 3).alias("_cf_d3"),
            (pl.col("_cf_down") == 4).alias("_cf_d4"),
            pl.col("pos_score_diff_end"),
        )
        cf_data.columns = ep_final_names
        EP_penalty_cf_parts = ep_model.predict(DMatrix(cf_data))
        EP_penalty_cf = self.__calculate_ep_exp_val(EP_penalty_cf_parts)

        play_df = play_df.with_columns(
            EP_start_touchback=pl.lit(EP_start_touchback),
            EP_start=pl.lit(EP_start),
            EP_end=pl.lit(EP_end),
            EP_penalty_cf=pl.when(pl.col("_cf_ok") == True).then(pl.lit(EP_penalty_cf)).otherwise(None),
            penalty_cf_yardsToEndzone=pl.when(pl.col("_cf_ok") == True)
            .then(pl.col("_cf_y2ez").cast(pl.Int32))
            .otherwise(None),
        ).drop(["_cf_play_yards", "_cf_converted", "_cf_y2ez", "_cf_ok", "_cf_down", "_cf_distance"])

        play_df = (
            play_df.with_columns(
                EP_start=pl.when(
                    pl.col("type.text").is_in(
                        [
                            "Extra Point Good",
                            "Extra Point Missed",
                            "Two-Point Conversion Good",
                            "Two-Point Conversion Missed",
                            "Two Point Pass",
                            "Two Point Rush",
                            "Blocked PAT",
                        ],
                    ),
                )
                .then(0.92)
                .otherwise(pl.col("EP_start")),
            )
            .with_columns(
                # End of Half
                EP_end=pl.when(
                    (pl.col("type.text").str.to_lowercase().str.contains(r"end of game")).or_(
                        pl.col("type.text").str.to_lowercase().str.contains(r"end of half"),
                    ),
                )
                .then(0)
                # Defensive 2pt Conversion
                .when(pl.col("type.text").is_in(["Defensive 2pt Conversion"]))
                .then(-2)
                # Safeties
                .when(
                    (pl.col("type.text").is_in(defense_score_vec)).and_(
                        pl.col("text").str.to_lowercase().str.contains(r"(?i)safety"),
                    ),
                )
                .then(-2)
                # Defense TD + Successful Two-Point Conversion
                .when(
                    (pl.col("type.text").is_in(defense_score_vec))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)conversion"))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)failed") == False),
                )
                .then(-8)
                # Defense TD + Failed Two-Point Conversion
                .when(
                    (pl.col("type.text").is_in(defense_score_vec))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)conversion"))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)failed")),
                )
                .then(-6)
                # Defense TD + Kick/PAT Missed
                .when(
                    (pl.col("type.text").is_in(defense_score_vec))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"PAT"))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)missed")),
                )
                .then(-6)
                # Defense TD + Kick/PAT Good
                .when(
                    (pl.col("type.text").is_in(defense_score_vec)).and_(
                        pl.col("text").str.to_lowercase().str.contains(r"kick\)"),
                    ),
                )
                .then(-7)
                # Defense TD
                .when(pl.col("type.text").is_in(defense_score_vec))
                .then(-6.92)
                # Offense TD + Failed Two-Point Conversion
                .when(
                    (pl.col("type.text").is_in(offense_score_vec))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)conversion"))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)failed")),
                )
                .then(6)
                # Offense TD + Successful Two-Point Conversion
                .when(
                    (pl.col("type.text").is_in(offense_score_vec))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)conversion"))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)failed") == False),
                )
                .then(8)
                # Offense Made FG
                .when(
                    (pl.col("type.text").is_in(offense_score_vec))
                    .and_(pl.col("type.text").str.to_lowercase().str.contains(r"(?i)field goal"))
                    .and_(pl.col("type.text").str.to_lowercase().str.contains(r"(?i)good")),
                )
                .then(3)
                # Offense TD + Kick/PAT Missed
                .when(
                    (pl.col("type.text").is_in(offense_score_vec))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"PAT"))
                    .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)missed")),
                )
                .then(6)
                # Offense TD + Kick/PAT Good
                .when(
                    (pl.col("type.text").is_in(offense_score_vec)).and_(
                        pl.col("text").str.to_lowercase().str.contains(r"kick\)"),
                    ),
                )
                .then(7)
                # Offense TD
                .when(pl.col("type.text").is_in(offense_score_vec))
                .then(6.92)
                # Extra Point Good
                .when(pl.col("type.text").is_in(["Extra Point Good"]))
                .then(1)
                # Extra Point Missed
                .when(pl.col("type.text").is_in(["Extra Point Missed"]))
                .then(0)
                # Two-Point Conversion Good
                .when(pl.col("type.text").is_in(["Two-Point Conversion Good"]))
                .then(2)
                # Two-Point Conversion Missed
                .when(pl.col("type.text").is_in(["Two-Point Conversion Missed"]))
                .then(0)
                # Two Point Pass/Rush Missed (Pre-2014 Data)
                .when(
                    (pl.col("type.text").is_in(["Two Point Pass", "Two Point Rush"])).and_(
                        pl.col("text").str.to_lowercase().str.contains(r"(?i)no good"),
                    ),
                )
                .then(0)
                # Two Point Pass/Rush Good (Pre-2014 Data)
                .when(
                    (pl.col("type.text").is_in(["Two Point Pass", "Two Point Rush"])).and_(
                        pl.col("text").str.to_lowercase().str.contains(r"(?i)no good") == False,
                    ),
                )
                .then(2)
                # Blocked PAT
                .when(pl.col("type.text").is_in(["Blocked PAT"]))
                .then(0)
                # Flips for Turnovers that aren't kickoffs
                .when(
                    ((pl.col("type.text").is_in(end_change_vec)).or_(pl.col("downs_turnover") == True)).and_(
                        pl.col("type.text").is_in(kickoff_vec) == False,
                    ),
                )
                .then(pl.col("EP_end") * -1)
                # Flips for Turnovers that are kickoffs
                .when(pl.col("type.text").is_in(kickoff_turnovers))
                .then(pl.col("EP_end") * -1)
                # Onside kicks
                .when((pl.col("kickoff_onside") == True).and_(pl.col("change_of_pos_team") == True))
                .then(pl.col("EP_end") * -1)
                .otherwise(pl.col("EP_end")),
            )
            .with_columns(
                # B12: the lag must not read a Timeout row's EP_end. That value is the
                # model scored on whatever state ESPN left on the Timeout row, and the
                # 2025 vendor template leaves a STALE one -- "Timeout TCU" at 4th & 9
                # on the 31 carried 2nd & 5 on the 35 from two plays earlier. The
                # Timeout's own EP_end is overridden to its EP_start further down, but
                # this lag was taken first, so the play after the Timeout saw a phantom
                # +2.3 discontinuity and, being a penalty play, folded it into EPA:
                # 401856766 p76, a safety published at -3.09 against -0.78 real.
                #
                # Across published 2025, penalty plays following a Timeout carried
                # |EP_between| mean 1.88 and p90 4.66 against 0.18 / 0.24 elsewhere,
                # with 25 above |EPA| 3. 2022-24 show 0.16 / 0.24, which is why it went
                # unnoticed until the new template. Forward-filling the last real play's
                # EP_end across any run of Timeouts is the same correction the override
                # below applies to the Timeout row itself.
                lag_EP_end=pl.when(pl.col("type.text") == "Timeout")
                .then(None)
                .otherwise(pl.col("EP_end"))
                .forward_fill()
                .shift(1),
                lag_change_of_pos_team=pl.col("change_of_pos_team").shift(1),
            )
            .with_columns(
                lag_change_of_pos_team=pl.when(pl.col("lag_change_of_pos_team").is_null())
                .then(False)
                .otherwise(pl.col("lag_change_of_pos_team")),
            )
            .with_columns(
                # B8: EP_between measures the UNEXPLAINED discontinuity since the last
                # play ended, and penalty EPA folds it in on the assumption that a gap
                # means someone's end state was wrong. After a score there is no
                # unexplained gap -- the points explain it -- and lag_EP_end is a
                # realized 7.00 rather than a field-position expectation. Adding it (the
                # possession-change branch does add) hands the next play a fictitious
                # swing of about eight points.
                #
                # 23 scrimmage penalties across 2005-2025 fall immediately after a
                # score, and 13 of them (57%) published |EPA| above 4, against a base
                # rate near 5% for scrimmage penalties generally. Zeroing the term there
                # leaves EPA as EP_end - EP_start, which is what the play itself did.
                #
                # lag_scoringPlay, not lag_scoring_play: the derived flag is not built
                # until well after __process_epa, while ESPN's own is set at line 2387.
                EP_between=pl.when(pl.col("lag_scoringPlay") == True)
                .then(0.0)
                .when(pl.col("lag_change_of_pos_team") == True)
                .then(pl.col("EP_start") + pl.col("lag_EP_end"))
                .otherwise(pl.col("EP_start") - pl.col("lag_EP_end")),
                EP_start=pl.when(
                    (pl.col("type.text").is_in(["Timeout", "End Period"])).and_(
                        pl.col("lag_change_of_pos_team") == False,
                    ),
                )
                .then(pl.col("lag_EP_end"))
                .otherwise(pl.col("EP_start")),
            )
            .with_columns(
                EP_start=pl.when(pl.col("type.text").is_in(kickoff_vec))
                .then(pl.col("EP_start_touchback"))
                .otherwise(pl.col("EP_start")),
            )
            .with_columns(
                EP_end=pl.when(pl.col("type.text").is_in(["Timeout"]))
                .then(pl.col("EP_start"))
                .otherwise(pl.col("EP_end")),
            )
            .with_columns(
                EPA=pl.when(pl.col("type.text").is_in(["Timeout"]))
                .then(0)
                .when((pl.col("scoring_play") == False).and_(pl.col("end_of_half") == True))
                .then(-1 * pl.col("EP_start"))
                .when((pl.col("type.text").is_in(kickoff_vec)).and_(pl.col("penalty_in_text") == True))
                .then(pl.col("EP_end") - pl.col("EP_start"))
                .when(
                    (pl.col("penalty_in_text") == True)
                    .and_(pl.col("type.text").is_in(["Penalty"]) == False)
                    .and_(pl.col("type.text").is_in(kickoff_vec) == False),
                )
                .then(pl.col("EP_end") - pl.col("EP_start") + pl.col("EP_between"))
                .otherwise(pl.col("EP_end") - pl.col("EP_start")),
            )
            .with_columns(
                def_EPA=pl.col("EPA") * -1,
                # --- EPA Summary flags ----
                EPA_scrimmage=pl.when(pl.col("scrimmage_play") == True).then(pl.col("EPA")).otherwise(None),
                EPA_rush=pl.when((pl.col("rush") == True).and_(pl.col("penalty_in_text") == True))
                .then(pl.col("EPA"))
                .when((pl.col("rush") == True).and_(pl.col("penalty_in_text") == False))
                .then(pl.col("EPA"))
                .otherwise(None),
                EPA_pass=pl.when(pl.col("pass") == True).then(pl.col("EPA")).otherwise(None),
                EPA_explosive=pl.when((pl.col("pass") == True).and_(pl.col("EPA") >= 2.4))
                .then(True)
                .when(((pl.col("rush") == True).and_(pl.col("EPA") >= 1.8)))
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                EPA_non_explosive=pl.when(pl.col("EPA_explosive") == False).then(pl.col("EPA")).otherwise(None),
                EPA_explosive_pass=pl.when((pl.col("pass") == True).and_(pl.col("EPA") >= 2.4))
                .then(True)
                .otherwise(False),
                EPA_explosive_rush=pl.when((pl.col("rush") == True).and_(pl.col("EPA") >= 1.8))
                .then(True)
                .otherwise(False),
                first_down_created=pl.when(
                    (pl.col("scrimmage_play") == True)
                    .and_(pl.col("end.down") == 1)
                    .and_(pl.col("start.pos_team.id") == pl.col("end.pos_team.id")),
                )
                .then(True)
                .otherwise(False),
                EPA_success=pl.when(pl.col("EPA") > 0).then(True).otherwise(False),
                EPA_success_early_down=pl.when((pl.col("EPA") > 0).and_(pl.col("early_down") == True))
                .then(True)
                .otherwise(False),
                EPA_success_early_down_pass=pl.when(
                    (pl.col("pass") == True).and_(pl.col("EPA") > 0).and_(pl.col("early_down") == True),
                )
                .then(True)
                .otherwise(False),
                EPA_success_early_down_rush=pl.when(
                    (pl.col("rush") == True).and_(pl.col("EPA") > 0).and_(pl.col("early_down") == True),
                )
                .then(True)
                .otherwise(False),
                EPA_success_late_down=pl.when((pl.col("EPA") > 0).and_(pl.col("late_down") == True))
                .then(True)
                .otherwise(False),
                EPA_success_late_down_pass=pl.when(
                    (pl.col("pass") == True).and_(pl.col("EPA") > 0).and_(pl.col("late_down") == True),
                )
                .then(True)
                .otherwise(False),
                EPA_success_late_down_rush=pl.when(
                    (pl.col("rush") == True).and_(pl.col("EPA") > 0).and_(pl.col("late_down") == True),
                )
                .then(True)
                .otherwise(False),
                EPA_success_standard_down=pl.when((pl.col("EPA") > 0).and_(pl.col("standard_down") == True))
                .then(True)
                .otherwise(False),
                EPA_success_passing_down=pl.when((pl.col("EPA") > 0).and_(pl.col("passing_down") == True))
                .then(True)
                .otherwise(False),
                EPA_success_pass=pl.when((pl.col("EPA") > 0).and_(pl.col("pass") == True)).then(True).otherwise(False),
                EPA_success_rush=pl.when((pl.col("EPA") > 0).and_(pl.col("rush") == True)).then(True).otherwise(False),
                EPA_success_EPA=pl.when(pl.col("EPA") > 0).then(pl.col("EPA")).otherwise(None),
                EPA_success_standard_down_EPA=pl.when((pl.col("EPA") > 0).and_(pl.col("standard_down") == True))
                .then(pl.col("EPA"))
                .otherwise(None),
                EPA_success_passing_down_EPA=pl.when((pl.col("EPA") > 0).and_(pl.col("passing_down") == True))
                .then(pl.col("EPA"))
                .otherwise(None),
                EPA_success_pass_EPA=pl.when((pl.col("EPA") > 0).and_(pl.col("pass") == True))
                .then(pl.col("EPA"))
                .otherwise(None),
                EPA_success_rush_EPA=pl.when((pl.col("EPA") > 0).and_(pl.col("rush") == True))
                .then(pl.col("EPA"))
                .otherwise(None),
                EPA_middle_8_success=pl.when((pl.col("EPA") > 0).and_(pl.col("middle_8") == True))
                .then(True)
                .otherwise(False),
                EPA_middle_8_success_pass=pl.when(
                    (pl.col("pass") == True).and_(pl.col("EPA") > 0).and_(pl.col("middle_8") == True),
                )
                .then(True)
                .otherwise(False),
                EPA_middle_8_success_rush=pl.when(
                    (pl.col("rush") == True).and_(pl.col("EPA") > 0).and_(pl.col("middle_8") == True),
                )
                .then(True)
                .otherwise(False),
                # B9: EPA_penalty is meant to isolate the PENALTY's effect, so a flag
                # that had no effect must read zero. A declined penalty leaves the play
                # exactly as it was and an offsetting pair replays the down, yet both
                # were taking the branches below and inheriting the PLAY's EP swing:
                # "Old Dominion Penalty, Defensive Holding (Bryce Duke) declined"
                # published +3.50. Roughly 97% of declined and offsetting rows carried a
                # nonzero value -- 286 of 296 in 2022, 158 of 161 in 2024, 314 of 319 in
                # 2025 -- with a dozen per season above |2|.
                #
                # This is as far as the direct calculation goes for now. The remaining
                # case, an ACCEPTED penalty on a play that stands, needs the counter-
                # factual end spot, which is the actual end spot less the penalty's
                # SIGNED yardage -- and that sign is not available. penalty_yards_signed
                # carries a reliable magnitude (|start - end| matches it on 96.8-97.2% of
                # no-play rows) but not a reliable sign: it agrees with the observed
                # direction on only 45% of them, and deriving the sign from
                # penalized_team gets to 88.8-90.4%. An 11% sign error mirrors the
                # yardage, which is the defect class parts (c) through (e) exist to
                # repair, so it is not built on that.
                #
                # No-play penalties need no counterfactual: the play never happened, so
                # the state that would have stood IS the start state, and EP_end -
                # EP_start already measures exactly the flag.
                EPA_penalty=pl.when(
                    (pl.col("penalty_declined") == True).or_(pl.col("penalty_offset") == True),
                )
                .then(0.0)
                .when(pl.col("type.text").is_in(["Penalty", "Penalty (Kickoff)"]))
                .then(pl.col("EPA"))
                .when(pl.col("penalty_in_text") == True)
                .then(pl.col("EP_end") - pl.col("EP_start"))
                .otherwise(None),
                # B11: the penalty's OWN effect, isolated from the play it rode on.
                # Declined / offsetting: zero. No-play and standalone rows: the flag
                # is the whole change, so EP_end - EP_start already is it. Scrimmage
                # plays that stand: actual end against the counterfactual end. Kicks,
                # returns, turnovers, scoring plays: null -- not yet safe to state.
                EPA_penalty_direct=pl.when(
                    (pl.col("penalty_declined") == True).or_(pl.col("penalty_offset") == True),
                )
                .then(0.0)
                .when(pl.col("EP_penalty_cf").is_not_null())
                .then(pl.col("EP_end") - pl.col("EP_penalty_cf"))
                .when(
                    (pl.col("penalty_flag") == True).and_(
                        (pl.col("penalty_no_play") == True).or_(
                            pl.col("type.text").is_in(["Penalty", "Penalty (Kickoff)"]),
                        ),
                    ),
                )
                .then(pl.col("EP_end") - pl.col("EP_start"))
                .otherwise(None),
                EPA_sp=pl.when(
                    (pl.col("fg_attempt") == True).or_(pl.col("punt") == True).or_(pl.col("kickoff_play") == True),
                )
                .then(pl.col("EPA"))
                .otherwise(False),
                EPA_fg=pl.when(pl.col("fg_attempt") == True).then(pl.col("EPA")).otherwise(None),
                EPA_punt=pl.when(pl.col("punt") == True).then(pl.col("EPA")).otherwise(None),
                EPA_kickoff=pl.when(pl.col("kickoff_play") == True).then(pl.col("EPA")).otherwise(None),
            )
        )
        return play_df

    def __process_qbr(self, play_df):
        play_df = (
            play_df.with_columns(
                qbr_epa=pl.when(pl.col("EPA") < -5.0)
                .then(-5.0)
                .when(pl.col("fumble_vec") == True)
                .then(-3.5)
                .otherwise(pl.col("EPA")),
                weight=pl.when(pl.col("home_wp_before") < 0.1)
                .then(0.6)
                .when((pl.col("home_wp_before") >= 0.1).and_(pl.col("home_wp_before") < 0.2))
                .then(0.9)
                .when((pl.col("home_wp_before") >= 0.8).and_(pl.col("home_wp_before") < 0.9))
                .then(0.9)
                .when(pl.col("home_wp_before") > 0.9)
                .then(0.6)
                .otherwise(1),
                non_fumble_sack=pl.when((pl.col("sack_vec") == True).and_(pl.col("fumble_vec") == False))
                .then(True)
                .otherwise(False),
            )
            .with_columns(
                sack_epa=pl.when(pl.col("non_fumble_sack") == True).then(pl.col("qbr_epa")).otherwise(None),
                pass_epa=pl.when(pl.col("pass") == True).then(pl.col("qbr_epa")).otherwise(None),
                rush_epa=pl.when(pl.col("rush") == True).then(pl.col("qbr_epa")).otherwise(None),
                pen_epa=pl.when(pl.col("penalty_flag") == True).then(pl.col("qbr_epa")).otherwise(None),
            )
            .with_columns(
                sack_weight=pl.when(pl.col("non_fumble_sack") == True).then(pl.col("weight")).otherwise(None),
                pass_weight=pl.when(pl.col("pass") == True).then(pl.col("weight")).otherwise(None),
                rush_weight=pl.when(pl.col("rush") == True).then(pl.col("weight")).otherwise(None),
                pen_weight=pl.when(pl.col("penalty_flag") == True).then(pl.col("weight")).otherwise(None),
            )
            .with_columns(
                action_play=pl.col("EPA") != 0,
                athlete_name=pl.when(pl.col("passer_player_name").is_not_null())
                .then(pl.col("passer_player_name"))
                .when(pl.col("rusher_player_name").is_not_null())
                .then(pl.col("rusher_player_name"))
                .otherwise(None),
            )
        )
        return play_df

    def __process_cpoe(self, play_df):
        """Completion probability + CPOE (nflfastR ``cp`` / ``cpoe`` analogue).

        Scores ``cp`` = P(complete pass) on pass plays via the bundled 8-feature
        completion-probability booster, then ``cpoe = 100 * (completion - cp)``
        (percentage-point scale). ``cp`` / ``cpoe`` are null on non-pass plays.
        Degrades to null columns if any source column is missing.
        """
        cp_sources = {
            "down": "start.down",
            "distance": "start.distance",
            "yards_to_goal": "start.yardsToEndzone",
            "score_diff": "pos_score_diff_start",
            "seconds_remaining": "start.TimeSecsRem",
            "is_home": "start.is_home",
            "period": "period",
            "passing_down": "passing_down",
        }
        required = list(cp_sources.values()) + ["pass", "completion"]
        if any(c not in play_df.columns for c in required):
            return play_df.with_columns(
                pl.lit(None, dtype=pl.Float64).alias("cp"),
                pl.lit(None, dtype=pl.Float64).alias("cpoe"),
            )
        try:
            feat = play_df.select(
                [pl.col(src).cast(pl.Float64).alias(name) for name, src in cp_sources.items()],
            ).to_pandas()[CP_FEATURES]
            cp_raw = cp_model.predict(DMatrix(feat, feature_names=CP_FEATURES))
            play_df = play_df.with_columns(
                pl.when(pl.col("pass") == True)
                .then(pl.Series("cp", cp_raw, dtype=pl.Float64))
                .otherwise(None)
                .alias("cp"),
            ).with_columns(
                pl.when(pl.col("cp").is_not_null())
                .then(100.0 * (pl.col("completion").cast(pl.Float64) - pl.col("cp")))
                .otherwise(None)
                .alias("cpoe"),
            )
        except Exception as err:  # noqa: BLE001 — degrade to null columns, never raise
            logger.warning("%s: __process_cpoe failed (%s); emitting null cp/cpoe", self.gameId, err)
            play_df = play_df.with_columns(
                pl.lit(None, dtype=pl.Float64).alias("cp"),
                pl.lit(None, dtype=pl.Float64).alias("cpoe"),
            )
        return play_df

    def __process_xpass(self, play_df):
        """Expected pass + pass-over-expected (nflfastR ``xpass`` / ``pass_oe``).

        Scores ``xpass`` = P(pass) on scrimmage rush-or-pass plays via the bundled
        7-feature expected-pass booster, then ``pass_oe = 100 * (pass - xpass)``
        (percentage-point scale). ``xpass`` / ``pass_oe`` are null elsewhere.
        Degrades to null columns if any source column is missing.
        """
        xpass_sources = {
            "down": "start.down",
            "distance": "start.distance",
            "yards_to_goal": "start.yardsToEndzone",
            "pos_score_diff": "pos_score_diff_start",
            "TimeSecsRem": "start.TimeSecsRem",
            "period": "period",
        }
        required = list(xpass_sources.values()) + ["pass", "rush", "season"]
        if any(c not in play_df.columns for c in required):
            return play_df.with_columns(
                pl.lit(None, dtype=pl.Float64).alias("xpass"),
                pl.lit(None, dtype=pl.Float64).alias("pass_oe"),
            )
        try:
            # Ordinal era from season (cuts: <=2006->0, <=2013->1, <=2017->2, else 3).
            play_df = play_df.with_columns(
                pl.when(pl.col("season") <= 2006)
                .then(0)
                .when(pl.col("season") <= 2013)
                .then(1)
                .when(pl.col("season") <= 2017)
                .then(2)
                .otherwise(3)
                .alias("era"),
            )
            feat = play_df.select(
                [pl.col(src).cast(pl.Float64).alias(name) for name, src in xpass_sources.items()]
                + [pl.col("era").cast(pl.Float64).alias("era")],
            ).to_pandas()[XPASS_FEATURES]
            xpass_raw = xpass_model.predict(DMatrix(feat, feature_names=XPASS_FEATURES))
            scrimmage = (pl.col("pass") == True).or_(pl.col("rush") == True)
            play_df = play_df.with_columns(
                pl.when(scrimmage).then(pl.Series("xpass", xpass_raw, dtype=pl.Float64)).otherwise(None).alias("xpass"),
            ).with_columns(
                pl.when(pl.col("xpass").is_not_null())
                .then(100.0 * (pl.col("pass").cast(pl.Float64) - pl.col("xpass")))
                .otherwise(None)
                .alias("pass_oe"),
            )
        except Exception as err:  # noqa: BLE001 — degrade to null columns, never raise
            logger.warning("%s: __process_xpass failed (%s); emitting null xpass/pass_oe", self.gameId, err)
            play_df = play_df.with_columns(
                pl.lit(None, dtype=pl.Float64).alias("xpass"),
                pl.lit(None, dtype=pl.Float64).alias("pass_oe"),
            )
        return play_df

    def __process_wpa(self, play_df):
        # ---- prepare variables for wp_before calculations ----
        play_df = (
            play_df.with_columns(
                # B5: the penalty-assessed-on-kickoff play takes the touchback wp_before
                # (see _apply_wp_derivation); its touchback ExpScoreDiff must reflect the
                # real score state (like a kickoff), not the neutral 0.0 default, or the
                # substituted wp_before would imply an even game on a non-even one.
                pl.when((pl.col("type.text").is_in(kickoff_vec)).or_(pl.col("penalty_assessed_on_kickoff") == True))
                .then(pl.col("pos_score_diff_start") + pl.col("EP_start_touchback"))
                .otherwise(0.000)
                .alias("start.ExpScoreDiff_touchback"),
                pl.when((pl.col("penalty_in_text") == True).and_(pl.col("type.text").is_in(["Penalty"]) == False))
                .then(pl.col("pos_score_diff_start") + pl.col("EP_start") - pl.col("EP_between"))
                .when((pl.col("type.text") == "Timeout").and_(pl.col("lag_scoringPlay") == True))
                .then(pl.col("pos_score_diff_start") + 0.92)
                .otherwise(pl.col("pos_score_diff_start") + pl.col("EP_start"))
                .alias("start.ExpScoreDiff"),
            )
            .with_columns(
                (pl.col("start.ExpScoreDiff_touchback") / (pl.col("start.adj_TimeSecsRem") + 1)).alias(
                    "start.ExpScoreDiff_Time_Ratio_touchback",
                ),
                (pl.col("start.ExpScoreDiff") / (pl.col("start.adj_TimeSecsRem") + 1)).alias(
                    "start.ExpScoreDiff_Time_Ratio",
                ),
                # ---- prepare variables for wp_after calculations ----
                pl.when(
                    ((pl.col("type.text").is_in(end_change_vec)).or_(pl.col("downs_turnover") == True))
                    .and_(pl.col("kickoff_play") == False)
                    .and_(pl.col("scoringPlay") == False),
                )
                .then(pl.col("pos_score_diff_end") - pl.col("EP_end"))
                .when(pl.col("type.text").is_in(kickoff_turnovers).and_(pl.col("scoringPlay") == False))
                .then(pl.col("pos_score_diff_end") + pl.col("EP_end"))
                .when((pl.col("scoringPlay") == False).and_(pl.col("type.text") != "Timeout"))
                .then(pl.col("pos_score_diff_end") + pl.col("EP_end"))
                .when((pl.col("scoringPlay") == False).and_(pl.col("type.text") == "Timeout"))
                .then(pl.col("pos_score_diff_end") + pl.col("EP_end"))
                .when(
                    (pl.col("scoringPlay") == True)
                    .and_(pl.col("td_play") == True)
                    .and_(pl.col("type.text").is_in(defense_score_vec))
                    .and_(pl.col("season") <= 2013),
                )
                .then(pl.col("pos_score_diff_end") - 0.92)
                .when(
                    (pl.col("scoringPlay") == True)
                    .and_(pl.col("td_play") == True)
                    .and_(pl.col("type.text").is_in(offense_score_vec))
                    .and_(pl.col("season") <= 2013),
                )
                .then(pl.col("pos_score_diff_end") + 0.92)
                .when(
                    (pl.col("type.text") == "Timeout")
                    .and_(pl.col("lag_scoringPlay") == True)
                    .and_(pl.col("season") <= 2013),
                )
                .then(pl.col("pos_score_diff_end") + 0.92)
                .otherwise(pl.col("pos_score_diff_end"))
                .alias("end.ExpScoreDiff"),
            )
            .with_columns(
                (pl.col("end.ExpScoreDiff") / (pl.col("end.adj_TimeSecsRem") + 1)).alias("end.ExpScoreDiff_Time_Ratio"),
            )
        )

        # ---- raw model predictions: spread (13-feat) + naive / spread-free (12-feat) ----
        WP_start_touchback, WP_start, WP_end = _wp_predict(
            play_df,
            wp_model,
            wp_final_names,
            wp_start_touchback_columns,
            wp_start_columns,
            wp_end_columns,
        )
        WP_tb_naive, WP_start_naive, WP_end_naive = _wp_predict(
            play_df,
            wp_naive_model,
            wp_naive_final_names,
            wp_naive_start_touchback_columns,
            wp_naive_start_columns,
            wp_naive_end_columns,
        )

        # ---- derive wp_before / wp_after / wpa (+ home/away/def) for each model ----
        # The spread surface keeps the canonical un-suffixed column names; the
        # spread-free surface mirrors it under the ``_naive`` suffix.
        # B14: the same end-state prediction with the score differential restated in
        # the END team's perspective (see _apply_wp_derivation). Only consumed on the
        # last row of a game in progress.
        _flip_df = play_df.with_columns((-pl.col("end.pos_score_diff")).alias("end.pos_score_diff"))
        _, _, WP_end_flip = _wp_predict(
            _flip_df, wp_model, wp_final_names, wp_start_touchback_columns, wp_start_columns, wp_end_columns
        )
        _, _, WP_end_flip_naive = _wp_predict(
            _flip_df,
            wp_naive_model,
            wp_naive_final_names,
            wp_naive_start_touchback_columns,
            wp_naive_start_columns,
            wp_naive_end_columns,
        )
        play_df = _apply_wp_derivation(
            play_df, WP_start, WP_start_touchback, WP_end, suffix="", wp_after_flip_raw=WP_end_flip
        )
        play_df = _apply_wp_derivation(
            play_df,
            WP_start_naive,
            WP_tb_naive,
            WP_end_naive,
            suffix="_naive",
            wp_after_flip_raw=WP_end_flip_naive,
        )
        return play_df

    def __add_drive_data(self, play_df):
        play_df = (
            play_df.with_columns(
                (
                    pl.when(pl.col("drive.result").is_null())
                    .then(pl.lit("Not provided"))
                    .otherwise(pl.col("drive.result"))
                )
                .cast(pl.Utf8)
                .alias("drive.result"),
            )
            .with_columns(
                drive_start=pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
                .then(100 - pl.col("drive.start.yardLine"))
                .otherwise(pl.col("drive.start.yardLine")),
                drive_stopped=pl.when(pl.col("drive.result").is_null())
                .then(False)
                .otherwise(
                    pl.col("drive.result").str.to_lowercase().str.contains(r"(?i)punt|fumble|interception|downs"),
                ),
            )
            .with_columns(
                drive_start=pl.col("drive_start").cast(pl.Float32),
            )
            .with_columns(
                drive_play_index=pl.col("scrimmage_play").cum_sum().over("drive.id"),
            )
            .with_columns(
                drive_offense_plays=pl.when((pl.col("sp") == False).and_(pl.col("scrimmage_play") == True))
                .then(pl.col("play").cast(pl.Int32))
                .otherwise(0),
                prog_drive_EPA=pl.col("EPA_scrimmage").cum_sum().over("drive.id"),
                prog_drive_WPA=pl.col("wpa").cum_sum().over("drive.id"),
                # A3 (0.36-live): exclude interception plays -- a pick-six's RETURN
                # yardage rides in statYardage on the offensive INT play, so without
                # this it inflates the drive's offensive total.
                drive_offense_yards=pl.when(
                    (pl.col("sp") == False).and_(pl.col("scrimmage_play") == True).and_(pl.col("int") == False),
                )
                .then(pl.col("statYardage"))
                .otherwise(0),
            )
            .with_columns(
                drive_total_yards=pl.col("drive_offense_yards").cum_sum().over("drive.id"),
            )
        )
        return play_df

    def __cast_box_score_column(self, play_df, column, target_type):
        if column in play_df.columns:
            play_df = play_df.with_columns(pl.col(column).cast(target_type).alias(column))
        else:
            play_df = play_df.with_columns((pl.Null).alias(column))
        return play_df

    def create_box_score(self, play_df):
        """Build a per-team and per-player advanced box score from a processed
        plays frame.

        Triggers :meth:`run_processing_pipeline` first if it hasn't already run,
        so the input ``play_df`` is expected to be the post-pipeline plays frame.

        Args:
            play_df (pl.DataFrame): The plays frame produced by
                :meth:`run_processing_pipeline` (with EPA, WPA and play-type
                flags already populated).

        Returns:
            dict: Box-score sections, each a list of records — ``"pass"`` /
            ``"rush"`` / ``"receiver"`` (per-player advanced + EPA lines),
            ``"team"`` and ``"situational"`` (per-team), ``"defensive"`` and
            ``"defensive_players"`` (team- and player-level havoc),
            ``"specialists"`` (kicking / punting / return players),
            ``"turnover"``, ``"drives"``, and the ESPN-sourced ``"espn_team"`` /
            ``"espn_players"`` totals.

        Example:
            Quick start::

                from sportsdataverse.cfb import CFBPlayProcess
                game = CFBPlayProcess(gameId=401628334)
                game.espn_cfb_pbp()
                processed = game.run_processing_pipeline()
                box = game.create_box_score(game.plays_json)
                print(list(box.keys()))

            See Also:
                * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package
        """
        # have to run the pipeline before pulling this in
        if self.ran_pipeline == False:
            self.run_processing_pipeline()

        box_score_columns = [
            "completion",
            "target",
            "yds_receiving",
            "yds_rushed",
            "rush",
            "rush_td",
            "pass",
            "pass_td",
            "EPA",
            "wpa",
            "int",
            "int_td",
            "def_EPA",
            "EPA_rush",
            "EPA_pass",
            "EPA_success",
            "EPA_success_pass",
            "EPA_success_rush",
            "EPA_success_standard_down",
            "EPA_success_passing_down",
            "middle_8",
            "rz_play",
            "scoring_opp",
            "stuffed_run",
            "stopped_run",
            "opportunity_run",
            "highlight_run",
            "short_rush_success",
            "short_rush_attempt",
            "power_rush_success",
            "power_rush_attempt",
            "EPA_explosive",
            "EPA_explosive_pass",
            "EPA_explosive_rush",
            "standard_down",
            "passing_down",
            "fumble_vec",
            "sack",
            "penalty_flag",
            "play",
            "scrimmage_play",
            "sp",
            "kickoff_play",
            "punt",
            "fg_attempt",
            "EPA_penalty",
            "EPA_sp",
            "EPA_fg",
            "EPA_punt",
            "EPA_kickoff",
            "TFL",
            "TFL_pass",
            "TFL_rush",
            "havoc",
        ]
        for item in box_score_columns:
            self.__cast_box_score_column(play_df, item, pl.Float32)

        pass_box = play_df.filter((pl.col("pass") == True) & (pl.col("scrimmage_play") == True))
        rush_box = play_df.filter((pl.col("rush") == True) & (pl.col("scrimmage_play") == True))
        # pass_box.yds_receiving.fillna(0.0, inplace=True)
        passer_box = (
            _fill_missing(pass_box)
            .group_by(["pos_team", "passer_player_name"])
            .agg(
                Comp=pl.col("completion").sum(),
                Att=pl.col("pass_attempt").sum(),
                xComp=pl.col("cp").sum(),
                Yds=pl.col("yds_receiving").sum(),
                Pass_TD=pl.col("pass_td").sum(),
                Int=pl.col("int").sum(),
                YPA=pl.col("yds_receiving").mean(),
                EPA=pl.col("EPA").sum(),
                EPA_per_Play=pl.col("EPA").mean(),
                WPA=pl.col("wpa").sum(),
                SR=pl.col("EPA_success").mean(),
                Sck=pl.col("sack_vec").sum(),
            )
            .with_columns(
                CompPct=(pl.when(pl.col("Att") == pl.lit(0)).then(0).otherwise(pl.col("Comp") / pl.col("Att"))),
                xCompPct=(pl.when(pl.col("Att") == pl.lit(0)).then(0).otherwise(pl.col("xComp") / pl.col("Att"))),
            )
            .with_columns(
                CPOE=(pl.col("CompPct") - pl.col("xCompPct")),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )
        # passer_box = passer_box.replace(pl.all(), pl.Null)
        qbs_list = passer_box["passer_player_name"].to_list()

        # Recompute athlete_name here rather than trusting the copy made during
        # QBR feature setup. That earlier copy is taken before the participants
        # join rewrites passer_player_name / rusher_player_name with cleaned
        # names, so it keeps the raw participant text ("No Huddle-Shotgun #2
        # E.Buehler") while qbs_list holds the cleaned name ("Eddie Buehler").
        # The is_in below then matched nothing and every passer vanished from
        # the box score -- see tests/cfb/test_cfb_box_score.py.
        play_df = play_df.with_columns(
            athlete_name=pl.coalesce([pl.col("passer_player_name"), pl.col("rusher_player_name")])
        )

        pass_qbr_box = play_df.filter(
            (pl.col("athlete_name").is_not_null() == True)
            & (pl.col("scrimmage_play") == True)
            & (pl.col("athlete_name").is_in(qbs_list)),
        )
        pass_qbr = (
            pass_qbr_box.group_by(["pos_team", "athlete_name"])
            .agg(
                qbr_epa=(pl.col("qbr_epa") * pl.col("weight")).sum() / pl.col("weight").sum(),
                sack_epa=(pl.col("sack_epa") * pl.col("sack_weight")).sum() / pl.col("sack_weight").sum(),
                pass_epa=(pl.col("pass_epa") * pl.col("pass_weight")).sum() / pl.col("pass_weight").sum(),
                rush_epa=(pl.col("rush_epa") * pl.col("rush_weight")).sum() / pl.col("rush_weight").sum(),
                pen_epa=(pl.col("pen_epa") * pl.col("pen_weight")).sum() / pl.col("pen_weight").sum(),
                spread=(pl.col("start.pos_team_spread").first()),
            )
            .pipe(_fill_missing)
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )
        # One-hot rule-era dummies (era0..era3, cuts 2006/2013/2020). Era is constant
        # within a game, so they are added as literal columns from the game's season.
        _qbr_season = int(play_df["season"].drop_nulls().max())
        pass_qbr = pass_qbr.with_columns(
            era0=pl.lit(1 if _qbr_season <= 2006 else 0, dtype=pl.Int32),
            era1=pl.lit(1 if 2006 < _qbr_season <= 2013 else 0, dtype=pl.Int32),
            era2=pl.lit(1 if 2013 < _qbr_season <= 2020 else 0, dtype=pl.Int32),
            era3=pl.lit(1 if _qbr_season > 2020 else 0, dtype=pl.Int32),
        )
        # # self.logger.info(pass_qbr)

        dtest_qbr = DMatrix(pass_qbr[qbr_vars])
        qbr_result = qbr_model.predict(dtest_qbr)
        pass_qbr = pass_qbr.with_columns(exp_qbr=pl.lit(qbr_result))
        # LEFT, not inner: QBR is an enrichment. An inner join means any failure
        # to score QBR deletes the passing box score outright, which is how a
        # stale athlete_name silently emptied the passers table on every game.
        passer_box = passer_box.join(
            pass_qbr,
            left_on=["passer_player_name", "pos_team"],
            right_on=["athlete_name", "pos_team"],
            how="left",
        ).sort("Att", descending=True)  # 0.36-live: box tables sorted by volume

        rusher_box = (
            _fill_missing(rush_box)
            .group_by(["pos_team", "rusher_player_name"])
            .agg(
                Car=pl.col("rush").sum(),
                Yds=pl.col("yds_rushed").sum(),
                Rush_TD=pl.col("rush_td").sum(),
                YPC=pl.col("yds_rushed").mean(),
                EPA=pl.col("EPA").sum(),
                EPA_per_Play=pl.col("EPA").mean(),
                WPA=pl.col("wpa").sum(),
                SR=pl.col("EPA_success").mean(),
                Fum=pl.col("fumble_vec").sum(),
                Fum_Lost=pl.col("fumble_lost").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
            .sort("Car", descending=True)  # 0.36-live: box tables sorted by volume
        )
        # rusher_box = rusher_box.replace({np.nan: None})

        receiver_box = (
            _fill_missing(pass_box)
            .group_by(["pos_team", "receiver_player_name"])
            .agg(
                Rec=pl.col("completion").sum(),
                Tar=pl.col("target").sum(),
                Yds=pl.col("yds_receiving").sum(),
                Rec_TD=pl.col("pass_td").sum(),
                YPT=pl.col("yds_receiving").mean(),
                EPA=pl.col("EPA").sum(),
                EPA_per_Play=pl.col("EPA").mean(),
                WPA=pl.col("wpa").sum(),
                SR=pl.col("EPA_success").mean(),
                Fum=pl.col("fumble_vec").sum(),
                Fum_Lost=pl.col("fumble_lost").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
            .sort("Tar", descending=True)  # 0.36-live: box tables sorted by volume
        )

        team_base_box = (
            play_df.group_by(["pos_team"])
            .agg(
                EPA_plays=pl.col("play").sum(),
                # A3 (0.36-live): exclude interception plays so a pick-six's RETURN
                # yardage isn't attributed to the offense that threw it -- consistent
                # with the off_yards/drive exclusions above.
                total_yards=pl.when(pl.col("int") == False).then(pl.col("statYardage")).otherwise(0).sum(),
                EPA_overall_total=pl.col("EPA").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_pen_box = (
            play_df.filter(pl.col("penalty_flag") == True)
            .group_by(["pos_team"])
            .agg(
                total_pen_yards=pl.col("statYardage").sum(),
                EPA_penalty=pl.col("EPA_penalty").sum(),
                # Only ACCEPTED penalties award a first down; a declined/offsetting penalty
                # whose text mentions "1st down" earned it from the play (counted as a
                # passing/rushing first down), so excluding them avoids double-counting.
                penalty_first_downs_created=(
                    (pl.col("penalty_1st_conv") == True)
                    & (pl.col("penalty_declined") == False)
                    & (pl.col("penalty_offset") == False)
                ).sum(),
                penalty_first_downs_created_rate=(
                    (pl.col("penalty_1st_conv") == True)
                    & (pl.col("penalty_declined") == False)
                    & (pl.col("penalty_offset") == False)
                ).mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_penalized_box = (
            play_df.filter(
                (pl.col("penalty_flag") == True)
                & (pl.col("penalty_declined") == False)
                & (pl.col("penalty_offset") == False)
                & (pl.col("penalized_team").is_not_null()),
            )
            .group_by(["penalized_team"])
            .agg(
                penalties=pl.len(),
                penalty_yards=pl.col("penalty_yards_signed").sum(),
            )
            .rename({"penalized_team": "pos_team"})
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_scrimmage_box = (
            play_df.filter(pl.col("scrimmage_play") == True)
            .group_by(["pos_team"])
            .agg(
                scrimmage_plays=pl.col("scrimmage_play").sum(),
                EPA_overall_off=pl.col("EPA").sum(),
                EPA_overall_offense=pl.col("EPA").sum(),
                EPA_per_play=pl.col("EPA").mean(),
                EPA_non_explosive=pl.col("EPA_non_explosive").sum(),
                EPA_non_explosive_per_play=pl.col("EPA_non_explosive").mean(),
                EPA_explosive=pl.col("EPA_explosive").sum(),
                EPA_explosive_rate=pl.col("EPA_explosive").mean(),
                passes_rate=pl.col("pass").mean(),
                # A3 (0.36-live): exclude interception plays so a pick-six's RETURN
                # yardage (carried in statYardage on the offensive INT play) isn't
                # counted as offense. Zeroing keeps the INT as a 0-yard play, so the
                # yards_per_play denominator is unchanged (matches 0.36-live's
                # statYardage zeroing) without mutating the shared statYardage column.
                off_yards=pl.when(pl.col("int") == False).then(pl.col("statYardage")).otherwise(0).sum(),
                total_off_yards=pl.when(pl.col("int") == False).then(pl.col("statYardage")).otherwise(0).sum(),
                yards_per_play=pl.when(pl.col("int") == False).then(pl.col("statYardage")).otherwise(0).mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_sp_box = (
            play_df.filter(pl.col("sp") == True)
            .group_by(["pos_team"])
            .agg(
                special_teams_plays=pl.col("sp").sum(),
                EPA_sp=pl.col("EPA_sp").sum(),
                EPA_special_teams=pl.col("EPA_sp").sum(),
                field_goals=pl.col("fg_attempt").sum(),
                EPA_fg=pl.col("EPA_fg").sum(),
                punt_plays=pl.col("punt_play").sum(),
                EPA_punt=pl.col("EPA_punt").sum(),
                kickoff_plays=pl.col("kickoff_play").sum(),
                EPA_kickoff=pl.col("EPA_kickoff").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_scrimmage_box_pass = (
            play_df.filter((pl.col("pass") == True) & (pl.col("scrimmage_play") == True))
            .pipe(_fill_missing)
            .group_by(["pos_team"])
            .agg(
                passes=pl.col("pass").sum(),
                pass_yards=pl.col("yds_receiving").sum(),
                yards_per_pass=pl.col("yds_receiving").mean(),
                passing_first_downs_created=pl.col("first_down_created").sum(),
                passing_first_downs_created_rate=pl.col("first_down_created").mean(),
                EPA_passing_overall=pl.col("EPA").sum(),
                EPA_passing_per_play=pl.col("EPA").mean(),
                EPA_explosive_passing=pl.col("EPA_explosive").sum(),
                EPA_explosive_passing_rate=pl.col("EPA_explosive").mean(),
                EPA_non_explosive_passing=pl.col("EPA_non_explosive").sum(),
                EPA_non_explosive_passing_per_play=pl.col("EPA_non_explosive").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_scrimmage_box_rush = (
            play_df.filter((pl.col("rush") == True) & (pl.col("scrimmage_play") == True))
            .pipe(_fill_missing)
            .group_by(["pos_team"])
            .agg(
                rushes=pl.col("rush").sum(),
                rush_yards=pl.col("yds_rushed").sum(),
                yards_per_rush=pl.col("yds_rushed").mean(),
                rushing_power_rate=pl.col("power_rush_attempt").mean(),
                rushing_first_downs_created=pl.col("first_down_created").sum(),
                rushing_first_downs_created_rate=pl.col("first_down_created").mean(),
                EPA_rushing_overall=pl.col("EPA").sum(),
                EPA_rushing_per_play=pl.col("EPA").mean(),
                EPA_explosive_rushing=pl.col("EPA_explosive").sum(),
                EPA_explosive_rushing_rate=pl.col("EPA_explosive").mean(),
                EPA_non_explosive_rushing=pl.col("EPA_non_explosive").sum(),
                EPA_non_explosive_rushing_per_play=pl.col("EPA_non_explosive").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_rush_base_box = (
            play_df.filter((pl.col("scrimmage_play") == True))
            .pipe(_fill_missing)
            .group_by(["pos_team"])
            .agg(
                rushes_rate=pl.col("rush").mean(),
                first_downs_created=pl.col("first_down_created").sum(),
                first_downs_created_rate=pl.col("first_down_created").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_rush_power_box = (
            play_df.filter((pl.col("power_rush_attempt") == True) & (pl.col("scrimmage_play") == True))
            .pipe(_fill_missing)
            .group_by(["pos_team"])
            .agg(
                EPA_rushing_power=pl.col("EPA").sum(),
                EPA_rushing_power_per_play=pl.col("EPA").mean(),
                rushing_power_success=pl.col("power_rush_success").sum(),
                rushing_power_success_rate=pl.col("power_rush_success").mean(),
                rushing_power=pl.col("power_rush_attempt").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        play_df = play_df.with_columns(
            opp_highlight_yards=pl.col("opp_highlight_yards").cast(pl.Float32),
            highlight_yards=pl.col("highlight_yards").cast(pl.Float32),
            line_yards=pl.col("line_yards").cast(pl.Float32),
            second_level_yards=pl.col("second_level_yards").cast(pl.Float32),
            open_field_yards=pl.col("open_field_yards").cast(pl.Float32),
        )

        team_rush_box = (
            play_df.filter((pl.col("rush") == True) & (pl.col("scrimmage_play") == True))
            .pipe(_fill_missing)
            .group_by(["pos_team"])
            .agg(
                rushing_stuff=pl.col("stuffed_run").sum(),
                rushing_stuff_rate=pl.col("stuffed_run").mean(),
                rushing_stopped=pl.col("stopped_run").sum(),
                rushing_stopped_rate=pl.col("stopped_run").mean(),
                rushing_opportunity=pl.col("opportunity_run").sum(),
                rushing_opportunity_rate=pl.col("opportunity_run").mean(),
                rushing_highlight=pl.col("highlight_run").sum(),
                rushing_highlight_rate=pl.col("highlight_run").mean(),
                rushing_highlight_yards=pl.col("highlight_yards").sum(),
                line_yards=pl.col("line_yards").sum(),
                line_yards_per_carry=pl.col("line_yards").mean(),
                second_level_yards=pl.col("second_level_yards").sum(),
                open_field_yards=pl.col("open_field_yards").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_rush_opp_box = (
            play_df.filter(
                (pl.col("rush") == True) & (pl.col("scrimmage_play") == True) & (pl.col("opportunity_run") == True),
            )
            .pipe(_fill_missing)
            .group_by(["pos_team"])
            .agg(
                rushing_highlight_yards_per_opp=pl.col("opp_highlight_yards").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_data_frames = [
            team_rush_opp_box,
            team_pen_box,
            team_penalized_box,
            team_sp_box,
            team_scrimmage_box_rush,
            team_scrimmage_box_pass,
            team_scrimmage_box,
            team_base_box,
            team_rush_base_box,
            team_rush_power_box,
            team_rush_box,
        ]
        team_box = reduce(
            lambda left, right: left.join(right, on=["pos_team"], how="full", coalesce=True),
            team_data_frames,
        )

        situation_box_normal = (
            play_df.filter(pl.col("scrimmage_play") == True)
            .group_by(["pos_team"])
            .agg(
                EPA_success=pl.col("EPA_success").sum(),
                EPA_success_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_rz = (
            play_df.filter((pl.col("rz_play") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                EPA_success_rz=pl.col("EPA_success").sum(),
                EPA_success_rate_rz=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_third = (
            play_df.filter((pl.col("start.down") == 3) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                EPA_success_third=pl.col("EPA_success").sum(),
                EPA_success_rate_third=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_pass = (
            play_df.filter((pl.col("pass") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                EPA_success_pass=pl.col("EPA_success").sum(),
                EPA_success_pass_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_rush = (
            play_df.filter((pl.col("rush") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                EPA_success_rush=pl.col("EPA_success").sum(),
                EPA_success_rush_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        situation_box_middle8 = (
            play_df.filter((pl.col("middle_8") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                middle_8=pl.col("middle_8").sum(),
                middle_8_pass_rate=pl.col("pass").mean(),
                middle_8_rush_rate=pl.col("rush").mean(),
                EPA_middle_8=pl.col("EPA").sum(),
                EPA_middle_8_per_play=pl.col("EPA").mean(),
                EPA_middle_8_success=pl.col("EPA_success").sum(),
                EPA_middle_8_success_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_middle8_pass = (
            play_df.filter((pl.col("pass") == True) & (pl.col("middle_8") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                middle_8_pass=pl.col("pass").sum(),
                EPA_middle_8_pass=pl.col("EPA").sum(),
                EPA_middle_8_pass_per_play=pl.col("EPA").mean(),
                EPA_middle_8_success_pass=pl.col("EPA_success").sum(),
                EPA_middle_8_success_pass_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_middle8_rush = (
            play_df.filter((pl.col("rush") == True) & (pl.col("middle_8") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                middle_8_rush=pl.col("rush").sum(),
                EPA_middle_8_rush=pl.col("EPA").sum(),
                EPA_middle_8_rush_per_play=pl.col("EPA").mean(),
                EPA_middle_8_success_rush=pl.col("EPA_success").sum(),
                EPA_middle_8_success_rush_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_early = (
            play_df.filter((pl.col("early_down") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                EPA_success_early_down=pl.col("EPA_success").sum(),
                EPA_success_early_down_rate=pl.col("EPA_success").mean(),
                early_downs=pl.col("early_down").sum(),
                early_down_pass_rate=pl.col("pass").mean(),
                early_down_rush_rate=pl.col("rush").mean(),
                EPA_early_down=pl.col("EPA").sum(),
                EPA_early_down_per_play=pl.col("EPA").mean(),
                early_down_first_down=pl.col("first_down_created").sum(),
                early_down_first_down_rate=pl.col("first_down_created").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_early_pass = (
            play_df.filter(
                (pl.col("pass") == True) & (pl.col("early_down") == True) & (pl.col("scrimmage_play") == True),
            )
            .group_by(["pos_team"])
            .agg(
                early_down_pass=pl.col("pass").sum(),
                EPA_early_down_pass=pl.col("EPA").sum(),
                EPA_early_down_pass_per_play=pl.col("EPA").mean(),
                EPA_success_early_down_pass=pl.col("EPA_success").sum(),
                EPA_success_early_down_pass_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_early_rush = (
            play_df.filter(
                (pl.col("rush") == True) & (pl.col("early_down") == True) & (pl.col("scrimmage_play") == True),
            )
            .group_by(["pos_team"])
            .agg(
                early_down_rush=pl.col("rush").sum(),
                EPA_early_down_rush=pl.col("EPA").sum(),
                EPA_early_down_rush_per_play=pl.col("EPA").mean(),
                EPA_success_early_down_rush=pl.col("EPA_success").sum(),
                EPA_success_early_down_rush_rate=pl.col("EPA_success").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_late = (
            play_df.filter((pl.col("late_down") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                EPA_success_late_down=pl.col("EPA_success_late_down").sum(),
                EPA_success_late_down_pass=pl.col("EPA_success_late_down_pass").sum(),
                EPA_success_late_down_rush=pl.col("EPA_success_late_down_rush").sum(),
                late_downs=pl.col("late_down").sum(),
                late_down_pass=pl.col("late_down_pass").sum(),
                late_down_rush=pl.col("late_down_rush").sum(),
                EPA_late_down=pl.col("EPA").sum(),
                EPA_late_down_per_play=pl.col("EPA").mean(),
                EPA_success_late_down_rate=pl.col("EPA_success_late_down").mean(),
                EPA_success_late_down_pass_rate=pl.col("EPA_success_late_down_pass").mean(),
                EPA_success_late_down_rush_rate=pl.col("EPA_success_late_down_rush").mean(),
                late_down_pass_rate=pl.col("late_down_pass").mean(),
                late_down_rush_rate=pl.col("late_down_rush").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_standard = (
            play_df.filter((pl.col("standard_down") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                EPA_success_standard_down=pl.col("EPA_success").sum(),
                EPA_success_standard_down_rate=pl.col("EPA_success").mean(),
                EPA_standard_down=pl.col("EPA").sum(),
                EPA_standard_down_per_play=pl.col("EPA").mean(),
                standard_downs=pl.col("standard_down").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_box_passing = (
            play_df.filter((pl.col("passing_down") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["pos_team"])
            .agg(
                EPA_success_passing_down=pl.col("EPA_success").sum(),
                EPA_success_passing_down_rate=pl.col("EPA_success").mean(),
                EPA_passing_down=pl.col("EPA").sum(),
                EPA_passing_down_per_play=pl.col("EPA").mean(),
                passing_downs=pl.col("passing_down").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        situation_data_frames = [
            situation_box_normal,
            situation_box_pass,
            situation_box_rush,
            situation_box_rz,
            situation_box_third,
            situation_box_early,
            situation_box_early_pass,
            situation_box_early_rush,
            situation_box_middle8,
            situation_box_middle8_pass,
            situation_box_middle8_rush,
            situation_box_late,
            situation_box_standard,
            situation_box_passing,
        ]

        situation_box = reduce(
            lambda left, right: left.join(right, on=["pos_team"], how="full", coalesce=True),
            situation_data_frames,
        )

        play_df = play_df.with_columns(
            drive_stopped=pl.col("drive_stopped").cast(pl.Float32),
            drive_start=pl.col("drive_start").cast(pl.Float32),
        )

        def_base_box = (
            play_df.filter(pl.col("scrimmage_play") == True)
            .group_by(["def_pos_team"])
            .agg(
                scrimmage_plays=pl.col("scrimmage_play").sum(),
                TFL=pl.col("TFL").sum(),
                TFL_pass=pl.col("TFL_pass").sum(),
                TFL_rush=pl.col("TFL_rush").sum(),
                havoc_total=pl.col("havoc").sum(),
                havoc_total_rate=pl.col("havoc").mean(),
                fumbles=pl.col("forced_fumble").sum(),
                def_int=pl.col("int").sum(),
                drive_stopped_rate=100 * pl.col("drive_stopped").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                def_pos_team=pl.col("def_pos_team").cast(pl.Int32),
            )
        )

        def_box_havoc_pass = (
            play_df.filter((pl.col("pass") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["def_pos_team"])
            .agg(
                num_pass_plays=pl.col("pass").sum(),
                havoc_total_pass=pl.col("havoc").sum(),
                havoc_total_pass_rate=pl.col("havoc").mean(),
                sacks=pl.col("sack_vec").sum(),
                sacks_rate=pl.col("sack_vec").mean(),
                pass_breakups=pl.col("pass_breakup").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                def_pos_team=pl.col("def_pos_team").cast(pl.Int32),
            )
        )

        def_box_havoc_rush = (
            play_df.filter((pl.col("rush") == True) & (pl.col("scrimmage_play") == True))
            .group_by(["def_pos_team"])
            .agg(
                havoc_total_rush=pl.col("havoc").sum(),
                havoc_total_rush_rate=pl.col("havoc").mean(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                def_pos_team=pl.col("def_pos_team").cast(pl.Int32),
            )
        )

        def_data_frames = [def_base_box, def_box_havoc_pass, def_box_havoc_rush]
        def_box = reduce(
            lambda left, right: left.join(right, on=["def_pos_team"], how="full", coalesce=True),
            def_data_frames,
        )
        def_box_json = json.loads(def_box.write_json())

        # Per-side turnover events. `is_pos_team_turnover` / `is_def_pos_team_turnover` are
        # set PER PLAY and a single play can set BOTH -- e.g. an interception returned and
        # fumbled back, or offense fumbles then the recovering defense fumbles it back. Emit
        # one event row per side that fired, keyed by the LOSING team, so team turnovers
        # match the official box's per-event accounting (a play can be a turnover for both).
        pos_ev = play_df.filter(pl.col("is_pos_team_turnover") == True).select(
            team=pl.col("pos_team").cast(pl.Int32),
            is_int=pl.col("int_turnover"),
            is_st=(pl.col("pos_fumble_lost") & pl.col("is_st_turnover")),
        )
        def_ev = play_df.filter(pl.col("is_def_pos_team_turnover") == True).select(
            team=pl.col("def_pos_team").cast(pl.Int32),
            is_int=pl.lit(False),
            is_st=(pl.col("def_fumble_lost") & pl.col("is_st_turnover")),
        )
        to_events = pl.concat([pos_ev, def_ev], how="vertical")

        to_lost = (
            to_events.group_by(["team"])
            .agg(
                turnovers=pl.len(),
                st_turnovers_lost=pl.col("is_st").sum(),
                Int=pl.col("is_int").sum(),
                fumbles_lost=(pl.col("is_int") == False).sum(),
            )
            .rename({"team": "pos_team"})
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )
        to_aux = (
            play_df.filter(pl.col("scrimmage_play") == True)
            .group_by(["pos_team"])
            .agg(
                pass_breakups=pl.col("pass_breakup").sum(),
                total_fumbles=pl.col("fumble_or_muff").sum(),
                fumbles_recovered=((pl.col("fumble_or_muff") == True) & (pl.col("is_turnover") == False)).sum(),
            )
            .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
        )

        team_ids = [int(self.homeTeamId), int(self.awayTeamId)]
        turnover_box = (
            pl.DataFrame({"pos_team": team_ids}, schema={"pos_team": pl.Int32})
            .join(to_lost, on="pos_team", how="left")
            .join(to_aux, on="pos_team", how="left")
            .fill_null(0)
            .with_columns(team_id=pl.col("pos_team"))
        )
        turnover_box_json = json.loads(turnover_box.write_json())

        # identity-keyed margins / luck (never list index).
        # Int here is all-play (from to_lost); pass_breakups/total_fumbles are scrimmage-only (to_aux).
        # Gained-side fields are the opponent's lost-side fields (a 2-team game: every
        # turnover one team loses, the other gains).
        by_id = {int(r["pos_team"]): r for r in turnover_box_json}

        # Source the countable turnover totals DIRECTLY from ESPN's official box where it
        # is available (authoritative). The play-by-play derivation -- which we still
        # compute and validate against ESPN -- is retained under ``*_pbp`` keys and is the
        # fallback for games ESPN does not cover.
        espn_box = self.json.get("boxscore", {}) if isinstance(self.json, dict) else {}
        espn_team_box = _parse_espn_team_box(espn_box)
        for tid, r in by_id.items():
            r["turnovers_pbp"] = r.get("turnovers", 0)
            r["Int_pbp"] = int(r.get("Int", 0))
            r["fumbles_lost_pbp"] = r.get("fumbles_lost", 0)
            e = espn_team_box.get(tid)
            r["espn_sourced"] = bool(e)
            if not e:
                continue
            if isinstance(e.get("turnovers"), int):
                r["turnovers"] = e["turnovers"]
            if isinstance(e.get("interceptions"), int):
                r["Int"] = e["interceptions"]
            if isinstance(e.get("fumblesLost"), int):
                r["fumbles_lost"] = e["fumblesLost"]

        for tid, r in by_id.items():
            r["Int"] = int(r.get("Int", 0))
            r["expected_turnovers"] = (0.5 * r.get("total_fumbles", 0)) + (
                0.22 * (r.get("pass_breakups", 0) + r.get("Int", 0))
            )
        for tid, r in by_id.items():
            others = [x for x in team_ids if x != tid]
            opp = by_id[others[0]] if others else r  # degenerate (home==away id): self as opponent
            r["expected_turnover_margin"] = opp["expected_turnovers"] - r["expected_turnovers"]
            r["turnover_margin"] = opp["turnovers"] - r["turnovers"]
            r["turnover_luck"] = 5.0 * (r["turnover_margin"] - r["expected_turnover_margin"])
            # takeaways gained = turnovers the opponent lost
            r["takeaways"] = opp["turnovers"]
            r["st_turnovers_gained"] = opp["st_turnovers_lost"]
            r["fumble_recoveries_gained"] = opp["fumbles_lost"]
        turnover_box_json = [by_id[t] for t in team_ids]

        drives_data = (
            play_df.filter(pl.col("scrimmage_play") == True)
            .group_by(["pos_team"])
            .agg(
                drive_total_available_yards=pl.col("drive_start").sum(),
                drive_total_gained_yards=pl.col("drive.yards").sum(),
                avg_field_position=pl.col("drive_start").mean(),
                plays_per_drive=pl.col("drive.offensivePlays").mean(),
                yards_per_drive=pl.col("drive.yards").mean(),
                drives=pl.col("drive.id").n_unique(),
                drive_total_gained_yards_rate=100 * pl.col("drive.yards").sum() / pl.col("drive_start").sum(),
            )
            .with_columns(pl.col(pl.Float32).round(2))
            .with_columns(
                pos_team=pl.col("pos_team").cast(pl.Int32),
            )
        )

        # --- defensive players (0.0.53): per-defender havoc events, attributed by player ---
        def _player_event_box(name_col, out, team_col, yds_col=None, team_out=None, extra_filter=None):
            """Count non-null occurrences of `name_col` per (team, player); sum `yds_col`.

            `team_col` is the column to group by (the resolved credited-team). `team_out`
            optionally renames it back to the section's canonical join key (e.g. group by
            `fumble_recovery_team` but emit it as `def_pos_team` so the section reduce-join
            still aligns). `extra_filter` optionally narrows the qualifying plays (e.g.
            takeaway-only fumble recoveries).
            """
            if name_col not in play_df.columns or team_col not in play_df.columns:
                return None
            f = play_df.filter(pl.col(name_col).is_not_null() & pl.col(team_col).is_not_null())
            if extra_filter is not None:
                f = f.filter(extra_filter)
            if f.height == 0:
                return None
            aggs = [pl.len().alias(out)]
            if yds_col is not None and yds_col in play_df.columns:
                aggs.append(pl.col(yds_col).sum().alias(f"{out}_yards"))
            g = f.group_by([team_col, name_col]).agg(aggs).rename({name_col: "player_name"})
            if team_out and team_out != team_col:
                g = g.rename({team_col: team_out})
            return g

        def _sack_part():
            """Per-player sacks with the official split-sack convention (#93).

            An assisted/split sack (``sack_player_name2`` populated) credits 0.5
            sacks -- and half the sack yardage -- to each participant, so the sum
            of player sacks always equals the team's sack-play count.
            """
            if "sack_player_name" not in play_df.columns or "def_pos_team" not in play_df.columns:
                return None
            has2 = "sack_player_name2" in play_df.columns
            split = pl.col("sack_player_name2").is_not_null() if has2 else pl.lit(False)
            credit = pl.when(split).then(pl.lit(0.5)).otherwise(pl.lit(1.0))
            yds = (
                pl.col("yds_sacked").cast(pl.Float64).fill_null(0.0) if "yds_sacked" in play_df.columns else pl.lit(0.0)
            )
            base = play_df.filter(pl.col("sack_player_name").is_not_null() & pl.col("def_pos_team").is_not_null())
            frames = [
                base.select(
                    pl.col("def_pos_team"),
                    pl.col("sack_player_name").alias("player_name"),
                    credit.alias("sacks"),
                    (yds * credit).alias("sacks_yards"),
                ),
            ]
            if has2:
                second = play_df.filter(
                    pl.col("sack_player_name2").is_not_null() & pl.col("def_pos_team").is_not_null(),
                )
                frames.append(
                    second.select(
                        pl.col("def_pos_team"),
                        pl.col("sack_player_name2").alias("player_name"),
                        pl.lit(0.5).alias("sacks"),
                        (yds * 0.5).alias("sacks_yards"),
                    ),
                )
            long = pl.concat(frames)
            if long.height == 0:
                return None
            return long.group_by(["def_pos_team", "player_name"]).agg(
                pl.col("sacks").sum(),
                pl.col("sacks_yards").sum(),
            )

        # #93: exclude own recoveries -- a recovery is a defensive (takeaway) event
        # only when the recovering side is not the fumbling side. Rows where the
        # fumbling side could not be parsed are kept (cannot prove own recovery).
        takeaway_only = (
            pl.col("fumbling_team").is_null() | (pl.col("fumble_recovery_team") != pl.col("fumbling_team"))
            if "fumbling_team" in play_df.columns
            else None
        )
        def_parts = [
            _sack_part(),
            _player_event_box("pass_breakup_player_name", "pass_breakups", "def_pos_team"),
            _player_event_box("interception_player_name", "interceptions", "def_pos_team", "yds_int_return"),
            # #93: group by forced_fumble_team (opposite the fumbling side) so a
            # coverage player forcing a punt/kick-return fumble is credited to the
            # kicking team, not blindly to def_pos_team.
            _player_event_box(
                "fumble_forced_player_name",
                "forced_fumbles",
                "forced_fumble_team",
                team_out="def_pos_team",
            ),
            _player_event_box(
                "fumble_recovered_player_name",
                "fumble_recoveries",
                "fumble_recovery_team",
                "yds_fumble_return",
                team_out="def_pos_team",
                extra_filter=takeaway_only,
            ),
        ]
        def_parts = [d for d in def_parts if d is not None]
        if def_parts:
            defensive_players = (
                reduce(
                    lambda left, right: left.join(right, on=["def_pos_team", "player_name"], how="full", coalesce=True),
                    def_parts,
                )
                .fill_null(0)
                .with_columns(def_pos_team=pl.col("def_pos_team").cast(pl.Int32))
            )
            defensive_players_json = json.loads(defensive_players.write_json())
        else:
            defensive_players_json = []

        # --- specialists (0.0.53): kicking / punting / return players, attributed by player ---
        # #93: credit kickoff returns to the RECEIVING team the same way punt returns
        # are -- prefer the parsed `kick_return_team`, coalescing to `pos_team` (ESPN
        # files a kickoff under the receiving team's possession) so a return is never
        # dropped just because the returning team abbreviation did not parse.
        if "kick_return_team" in play_df.columns:
            play_df = play_df.with_columns(
                _kick_return_credit_team=pl.coalesce(pl.col("kick_return_team"), pl.col("pos_team")),
            )
            _kick_return_team_col = "_kick_return_credit_team"
        else:
            _kick_return_team_col = "pos_team"
        spec_parts = [
            _player_event_box("fg_kicker_player_name", "field_goals", "pos_team", "yds_fg"),
            _player_event_box("punter_player_name", "punts", "pos_team", "yds_punted"),
            _player_event_box(
                "kickoff_return_player_name",
                "kick_returns",
                _kick_return_team_col,
                "yds_kickoff_return",
                team_out="pos_team",
            ),
            _player_event_box(
                "punt_return_player_name",
                "punt_returns",
                "punt_return_team",
                "yds_punt_return",
                team_out="pos_team",
            ),
        ]
        spec_parts = [s for s in spec_parts if s is not None]
        if spec_parts:
            specialists = (
                reduce(
                    lambda left, right: left.join(right, on=["pos_team", "player_name"], how="full", coalesce=True),
                    spec_parts,
                )
                .fill_null(0)
                .with_columns(pos_team=pl.col("pos_team").cast(pl.Int32))
            )
            specialists_json = json.loads(specialists.write_json())
        else:
            specialists_json = []

        # ESPN's official team + player box -- the authoritative source for countable
        # totals (turnovers, fumbles, interceptions, total/passing/rushing yards,
        # penalties, first downs, player stat lines). Surfaced as dedicated sections so
        # downstream consumers can take totals straight from ESPN; the computed sections
        # above retain the advanced/EPA metrics ESPN does not provide.
        espn_players = _parse_espn_player_box(espn_box)

        return {
            "pass": json.loads(passer_box.write_json()),
            "rush": json.loads(rusher_box.write_json()),
            "receiver": json.loads(receiver_box.write_json()),
            "team": json.loads(team_box.write_json()),
            "situational": json.loads(situation_box.write_json()),
            "defensive": def_box_json,
            "defensive_players": defensive_players_json,
            "specialists": specialists_json,
            "turnover": turnover_box_json,
            "drives": json.loads(drives_data.write_json()),
            "espn_team": list(espn_team_box.values()),
            "espn_players": espn_players,
        }

    def __join_participants(self, play_df):
        """Join ESPN play participants to overwrite regex-extracted player names with clean display names.

        Fetches participant data from :func:`~sportsdataverse.cfb.cfb_play_participants.espn_cfb_play_participants`
        and coalesces the ESPN-provided display names over the regex-extracted names wherever the participant
        name is non-null. Falls back to the original ``play_df`` on any failure (network error, parse error,
        empty result, missing join key) so offline/test paths are unaffected.

        Mapping (pbp_regex_col <- participant_col):
            - ``passer_player_name``           <- ``passer_player_name``
            - ``rusher_player_name``           <- ``rusher_player_name``
            - ``receiver_player_name``         <- ``receiver_player_name``
            - ``punter_player_name``           <- ``punter_player_name``
            - ``fg_kicker_player_name``        <- ``kicker_player_name``
            - ``sack_player_name``             <- ``sacked_by_player_name``
            - ``fumble_forced_player_name``    <- ``forced_by_player_name``
            - ``fumble_recovered_player_name`` <- ``recoverer_player_name`` (skipped if absent)
            - ``pass_breakup_player_name``     <- ``pass_defender_player_name`` (non-interception plays only)
            - ``interception_player_name``     <- ``pass_defender_player_name`` (interception plays — ESPN files the interceptor as the pass defender)
            - ``punt_return_player_name``      <- ``returner_player_name``
            - ``kickoff_return_player_name``   <- ``returner_player_name``

        Note: ``recoverer_player_name`` was not present in the verified participant frame for game 401135269;
        the mapping silently skips any participant column not found in the joined frame.

        Set the instance attribute ``join_participants = False`` to skip the (network) fetch
        entirely -- used by offline/disk reprocessing and the offline test suite so neither
        hits ESPN. Defaults to enabled for the normal live path.
        """
        passed = getattr(self, "participants", None)
        if passed is None and not getattr(self, "join_participants", True):
            return play_df
        original_play_df = play_df
        try:
            if passed is not None:
                # Caller-supplied participants (offline reprocess) -- avoids the
                # network fetch AND keeps 2014+ clean names when join_participants
                # is off. Accepts a polars frame, {"data": [...]}, or a row list.
                parts = (
                    passed
                    if hasattr(passed, "height")
                    else pl.from_dicts(
                        passed.get("data") if isinstance(passed, dict) else (passed or []),
                        infer_schema_length=None,
                    )
                )
            else:
                from sportsdataverse.cfb.cfb_play_participants import espn_cfb_play_participants

                parts = espn_cfb_play_participants(self.gameId)

            # Graceful fallback conditions
            if parts is None or parts.height == 0 or "id" not in play_df.columns:
                logging.debug(
                    f"{self.gameId}: __join_participants skipped -- "
                    f"parts={None if parts is None else parts.height} rows, id_in_df={'id' in play_df.columns}",
                )
                return play_df

            # Select only the scalar _player_name columns (not _player_names list columns)
            # plus the join key. We explicitly list the columns we want to avoid pulling in
            # list-type columns (kicker_player_names, etc.) or _player_id columns.
            participant_name_cols = [
                "kicker_player_name",
                "returner_player_name",
                "passer_player_name",
                "receiver_player_name",
                "rusher_player_name",
                "punter_player_name",
                "pass_defender_player_name",
                "sacked_by_player_name",
                "forced_by_player_name",
                # recoverer_player_name is NOT in verified participant frame; skip.
            ]
            # Only keep participant columns that actually exist in the frame
            available_part_cols = [c for c in participant_name_cols if c in parts.columns]
            parts_slim = parts.select(["play_id"] + available_part_cols)

            # Both play_df["id"] and parts["play_id"] are Int64 -- join directly.
            # Rename participant columns with a _part suffix before join to avoid collision
            # with existing pbp columns that share names (passer_player_name, etc.).
            rename_map = {c: f"{c}_part" for c in available_part_cols}
            parts_slim = parts_slim.rename(rename_map)

            play_df = play_df.join(
                parts_slim,
                how="left",
                left_on="id",
                right_on="play_id",
            )

            # Mapping: (pbp_regex_col, participant_col_after_rename, populate_if_null)
            #
            # populate_if_null=True:  participant name is authoritative; fill even when regex
            #   found nothing (safe for roles where team attribution is derived from pos_team /
            #   def_pos_team, which is always correct).
            # populate_if_null=False: only CLEAN UP an existing non-null regex name; never
            #   introduce a new player name where regex was silent.  Used for the shared
            #   `returner_player_name` participant role because it maps to both punt-return and
            #   kickoff-return columns whose team attribution (punt_return_team / pos_team) can
            #   point to the KICKING team for certain play types, causing mis-attribution when
            #   we populate a previously-null name.
            coalesce_pairs = [
                ("passer_player_name", "passer_player_name_part", True),
                ("rusher_player_name", "rusher_player_name_part", True),
                ("receiver_player_name", "receiver_player_name_part", True),
                ("punter_player_name", "punter_player_name_part", True),
                ("fg_kicker_player_name", "kicker_player_name_part", True),
                ("sack_player_name", "sacked_by_player_name_part", True),
                ("fumble_forced_player_name", "forced_by_player_name_part", True),
                # pass_defender is handled below with int-play routing, not here.
                # returner maps to two columns; use cleanup-only mode to avoid
                # injecting a name onto plays where punt_return_team / return_team
                # is set to the kicking team rather than the receiving team.
                ("punt_return_player_name", "returner_player_name_part", False),
                ("kickoff_return_player_name", "returner_player_name_part", False),
            ]

            coalesce_exprs = []
            for pbp_col, part_col, populate_if_null in coalesce_pairs:
                # Only coalesce if BOTH columns exist in the joined frame
                if pbp_col in play_df.columns and part_col in play_df.columns:
                    if populate_if_null:
                        # Participant name wins when non-null; regex name is fallback.
                        expr = pl.coalesce(
                            pl.col(part_col).str.strip_chars(),
                            pl.col(pbp_col),
                        ).alias(pbp_col)
                    else:
                        # Only replace when the regex already extracted a name AND the
                        # participant name is also non-null (clean up, never introduce).
                        expr = (
                            pl.when(pl.col(pbp_col).is_not_null() & pl.col(part_col).is_not_null())
                            .then(pl.col(part_col).str.strip_chars())
                            .otherwise(pl.col(pbp_col))
                            .alias(pbp_col)
                        )
                    coalesce_exprs.append(expr)

            # ESPN files the interceptor as the `pass_defender` participant on
            # interception plays (verified: 93.9% last-name identity against
            # the scoreboard-text interceptor over the 313 INT-return-TD rows
            # where both exist; the residue is suffix/nickname variance of the
            # same athlete). Route it to interception credit on int plays and
            # keep PBU attribution for non-interception plays only — a pick is
            # not a pass breakup.
            pd_part = "pass_defender_player_name_part"
            if pd_part in play_df.columns:
                is_int = (pl.col("int") == True) if "int" in play_df.columns else pl.lit(False)  # noqa: E712
                if "interception_player_name" in play_df.columns:
                    coalesce_exprs.append(
                        pl.when(is_int)
                        .then(
                            pl.coalesce(
                                pl.col(pd_part).str.strip_chars(),
                                pl.col("interception_player_name"),
                            ),
                        )
                        .otherwise(pl.col("interception_player_name"))
                        .alias("interception_player_name"),
                    )
                if "pass_breakup_player_name" in play_df.columns:
                    # On an interception the participant name IS the interceptor,
                    # so it must not be injected here. A regex-extracted PBU is
                    # KEPT though: 78 interception plays across 2004-2025 name two
                    # distinct defenders in the text ("intercepted by #25 K.Bretz
                    # ... broken up by #2 M.Taylor") -- a tip-drill pick, where the
                    # breakup credit is real. Nulling it would destroy that.
                    coalesce_exprs.append(
                        pl.when(is_int)
                        .then(pl.col("pass_breakup_player_name"))
                        .otherwise(
                            pl.coalesce(
                                pl.col(pd_part).str.strip_chars(),
                                pl.col("pass_breakup_player_name"),
                            ),
                        )
                        .alias("pass_breakup_player_name"),
                    )

            if coalesce_exprs:
                play_df = play_df.with_columns(coalesce_exprs)

            # Drop all _part helper columns so they don't leak into downstream schema
            part_cols_to_drop = [f"{c}_part" for c in available_part_cols]
            play_df = play_df.drop([c for c in part_cols_to_drop if c in play_df.columns])

            logging.debug(
                f"{self.gameId}: __join_participants applied {len(coalesce_exprs)} name coalesces "
                f"from {parts.height} participant rows",
            )
            return play_df

        except Exception as exc:
            logging.debug(f"{self.gameId}: __join_participants fallback -- {exc}")
            return original_play_df

    #: (name_col, id_col, team_col) for every extractable player type. ``team_col``
    #: is the play column holding that player's team id, used for team-aware roster
    #: matching (offense pos_team / defense def_pos_team / special-teams kicking,
    #: return, recovery columns from __add_attribution_cols).
    _PLAYER_ID_TEAM_MAP = [
        ("rusher_player_name", "rusher_player_id", "pos_team"),
        ("passer_player_name", "passer_player_id", "pos_team"),
        ("receiver_player_name", "receiver_player_id", "pos_team"),
        ("fumble_player_name", "fumble_player_id", "pos_team"),
        ("sack_player_name", "sack_player_id", "def_pos_team"),
        ("sack_player_name2", "sack_player_id2", "def_pos_team"),
        ("interception_player_name", "interception_player_id", "def_pos_team"),
        ("pass_breakup_player_name", "pass_breakup_player_id", "def_pos_team"),
        ("fumble_forced_player_name", "fumble_forced_player_id", "def_pos_team"),
        ("fumble_recovered_player_name", "fumble_recovered_player_id", "fumble_recovery_team"),
        ("fg_kicker_player_name", "fg_kicker_player_id", "kicking_team"),
        ("punter_player_name", "punter_player_id", "kicking_team"),
        ("kickoff_player_name", "kickoff_player_id", "kicking_team"),
        ("kickoff_return_player_name", "kickoff_return_player_id", "return_team"),
        ("punt_return_player_name", "punt_return_player_id", "punt_return_team"),
        ("fg_block_player_name", "fg_block_player_id", "return_team"),
        ("punt_block_player_name", "punt_block_player_id", "return_team"),
        ("fg_return_player_name", "fg_return_player_id", "return_team"),
        ("punt_block_return_player_name", "punt_block_return_player_id", "return_team"),
    ]

    @staticmethod
    def __roster_records(roster):
        """Normalize a roster (list / ``{"data": [...]}`` / DataFrame) into a list
        of ``(normalized_full_name, athlete_id:int, team_id:str)`` tuples."""
        if roster is None:
            return []
        if hasattr(roster, "to_dicts"):  # polars DataFrame
            rows = roster.to_dicts()
        elif hasattr(roster, "to_dict"):  # pandas DataFrame
            rows = roster.to_dict("records")
        elif isinstance(roster, dict):
            rows = roster.get("data") or []
        else:
            rows = roster
        out = []
        for a in rows:
            if not isinstance(a, dict):
                continue
            aid = a.get("athlete_id") if a.get("athlete_id") is not None else a.get("id")
            name = a.get("full_name") or a.get("athlete_display_name") or a.get("displayName")
            tid = a.get("team_id") if a.get("team_id") is not None else a.get("teamId")
            nn = _norm_player_name(name)
            if aid is None or not nn:
                continue
            try:
                out.append((nn, int(aid), str(tid)))
            except (TypeError, ValueError):
                continue
        return out

    def __boxscore_records(self):
        """Secondary id source: ESPN's per-player box score, same tuple shape as
        ``__roster_records``.

        ``game_rosters`` is the primary source, but ESPN 404s its roster resource
        for a non-trivial share of games -- 376 of 898 in 2018, 142 of 706 in
        2020 -- and on those games the roster join has nothing to match against.
        The box score ships the SAME ESPN athlete-id namespace (measured: 0 id
        conflicts on the roster/box overlap across 2004-2025) and already lives
        in the summary payload, so this costs no extra request.

        Coverage effect, measured on the committed cfbfastR-cfb-raw tree:
        pre-2014 the box is a strict subset of the roster (~27 names vs ~60) and
        adds nothing; 2014+ it adds +0.4 to +6.4pp normally and +32.4pp on 2018,
        where it recovers almost the entire empty-roster hole.
        """
        box = (getattr(self, "json", None) or {}).get("boxscore") or {}
        out = []
        for r in _parse_espn_player_box(box):
            aid = r.get("athlete_id")
            nn = _norm_player_name(r.get("athlete"))
            if aid is None or not nn:
                continue
            try:
                out.append((nn, int(aid), str(r.get("team_id"))))
            except (TypeError, ValueError):
                continue
        return out

    def __attach_player_ids(self, play_df):
        """Null garbage names, then attach a roster-resolved ``{type}_player_id``
        for each extracted ``{type}_player_name``.

        Games without a structured ``participants[]`` array (pre-2014) carry names
        parsed from the play text but no ids. This matches each name against the
        game roster (``self.game_roster`` if supplied, else fetched once on the
        live path) to recover the ESPN ``athlete_id``. Matching is **team-aware**
        -- each player type maps to the team that fielded it -- so identical names
        on opposing rosters don't collide; a globally-unique name is the fallback.
        Ids already populated (participants, 2014+) are preserved. No roster ->
        only the garbage-name cleanup is applied.
        """
        from collections import defaultdict

        # 1) clean captured names, then null obvious artifacts (always runs)
        present_names = [nc for nc, _, _ in self._PLAYER_ID_TEAM_MAP if nc in play_df.columns]
        if present_names:
            # 1a) shed a trailing "(TEAM)". This one is unconditional: a trailing
            #     parenthesised team code is never part of a person's name, so
            #     stripping it can't destroy information.
            #
            #     The narrative-tail strip is NOT done here -- it is applied in
            #     step 4, and only when the roster confirms the trimmed form.
            #     Blanket-stripping it would turn a real surname into a wrong one
            #     ("John Middle" -> "John"), which the surname fallback could then
            #     resolve to the wrong athlete.
            play_df = play_df.with_columns(
                [
                    pl.col(nc).str.replace(_PLAYER_NAME_TEAM_SUFFIX.pattern, "").str.strip_chars().alias(nc)
                    for nc in present_names
                ],
            )
            play_df = play_df.with_columns(
                [
                    pl.when(
                        pl.col(nc).str.contains(_PLAYER_NAME_GARBAGE.pattern) | (pl.col(nc).str.len_chars() == 0),
                    )
                    .then(None)
                    .otherwise(pl.col(nc))
                    .alias(nc)
                    for nc in present_names
                ],
            )

        # 2) resolve a roster (passed in, or live fetch when allowed)
        roster = self.game_roster
        if roster is None and getattr(self, "join_participants", True):
            try:
                from sportsdataverse.cfb.cfb_game_rosters import espn_cfb_game_rosters

                roster = espn_cfb_game_rosters(self.gameId)
            except Exception as exc:  # pragma: no cover - network dependent
                logging.debug(f"{self.gameId}: __attach_player_ids roster fetch failed -- {exc}")
                roster = None
        # Roster first, box score second. Both feed the same set-valued lookups
        # below, so order only affects which source seeds a name -- and since the
        # two agree on ids, a name present in both stays unambiguous. A name that
        # genuinely maps to two different ids becomes ambiguous and is dropped,
        # which is the intended conservative behavior.
        records = self.__roster_records(roster) + self.__boxscore_records()
        if not records:
            return play_df

        # 3) team-aware + global-unique lookups (ambiguous names are dropped)
        by_name, by_name_team = defaultdict(set), defaultdict(set)
        for nn, aid, tid in records:
            by_name[nn].add(aid)
            by_name_team[(nn, tid)].add(aid)
        team_lu = {k: next(iter(v)) for k, v in by_name_team.items() if len(v) == 1}
        global_lu = {k: next(iter(v)) for k, v in by_name.items() if len(v) == 1}

        # Fallback tiers for pre-2014 games, where the name comes from play text
        # rather than participants[] and may not render as the roster's full
        # name ("A. Smith" / bare surname). Both are TEAM-SCOPED and require the
        # key to be unique within that team -- a global last-name fallback
        # collides far too often to be safe.
        by_initial, by_last = defaultdict(set), defaultdict(set)
        for nn, aid, tid in records:
            parts = nn.split()
            if len(parts) >= 2:
                by_initial[(f"{parts[0][0]} {parts[-1]}", tid)].add(aid)
                by_last[(parts[-1], tid)].add(aid)
        initial_lu = {k: next(iter(v)) for k, v in by_initial.items() if len(v) == 1}
        last_lu = {k: next(iter(v)) for k, v in by_last.items() if len(v) == 1}

        def _match(name, team_id, *, allow_fallback=True):
            """Exact tiers always; the fuzzy tiers only when ``allow_fallback``.

            The fuzzy tiers key on a bare surname, so they must NOT be run on a
            name that still carries a narrative tail -- the tail word is itself a
            plausible surname. See ``_resolve``.
            """
            nn = _norm_player_name(name)
            if not nn:
                return None
            tid = str(team_id)
            aid = team_lu.get((nn, tid))
            if aid is not None:
                return aid
            aid = global_lu.get(nn)
            if aid is not None:
                return aid
            if not allow_fallback:
                return None
            parts = nn.split()
            if len(parts) >= 2:
                aid = initial_lu.get((f"{parts[0][0]} {parts[-1]}", tid))
                if aid is not None:
                    return aid
            return last_lu.get((parts[-1], tid))

        def _resolve(name, team_id):
            """Resolve a captured name to (display_name, athlete_id).

            The narrative-tail strip is applied HERE rather than blanket-applied
            during cleanup, and only when it actually earns a match. A name is
            rewritten only when the roster/box confirms the trimmed form, so a
            legitimate surname that happens to collide with a stopword
            ("John Middle", "Alex Screen") survives intact instead of being
            truncated and then mis-resolved by the surname fallback.
            """
            if not name:
                return {"name": None, "id": None}
            trimmed = _PLAYER_NAME_TAIL.sub("", str(name)).strip()
            has_tail = trimmed != name

            # A name that still carries a narrative tail is only trustworthy for
            # an EXACT match. The fuzzy tiers key on a bare surname, and a tail
            # word is itself a plausible surname: with roster entries
            # "Russell Wilson" and "Alex Screen", running the surname tier on
            # "Russell Wilson screen" resolves it to ALEX SCREEN. Withhold the
            # fuzzy tiers until the tail is gone.
            aid = _match(name, team_id, allow_fallback=not has_tail)
            if aid is not None:
                return {"name": name, "id": aid}
            if has_tail and trimmed:
                aid = _match(trimmed, team_id)
                if aid is not None:
                    return {"name": trimmed, "id": aid}
            # Neither form matched: keep the name as captured. Without a roster
            # hit there is no evidence the tail is narrative bleed rather than
            # part of the name, so trimming here would be a guess.
            return {"name": name, "id": None}

        # 4) attach {type}_player_id (fill nulls only; preserve participant ids)
        _RESOLVED = pl.Struct([pl.Field("name", pl.Utf8), pl.Field("id", pl.Int64)])
        exprs = []
        for name_col, id_col, team_col in self._PLAYER_ID_TEAM_MAP:
            if name_col not in play_df.columns or team_col not in play_df.columns:
                continue
            resolved = (
                pl.struct([pl.col(name_col).alias("_n"), pl.col(team_col).cast(pl.Utf8).alias("_t")])
                .map_elements(lambda s: _resolve(s["_n"], s["_t"]), return_dtype=_RESOLVED)
                .alias(f"__res_{name_col}")
            )
            play_df = play_df.with_columns(resolved)
            exprs.append(pl.col(f"__res_{name_col}").struct.field("name").alias(name_col))
            roster_id = pl.col(f"__res_{name_col}").struct.field("id")
            if id_col in play_df.columns:
                exprs.append(
                    pl.when(pl.col(id_col).is_null()).then(roster_id).otherwise(pl.col(id_col)).alias(id_col),
                )
            else:
                exprs.append(roster_id.alias(id_col))
        if exprs:
            play_df = play_df.with_columns(exprs).drop(
                [c for c in play_df.columns if c.startswith("__res_")],
            )
        return play_df

    # cfb4th decision-surface columns appended to 4th-down plays when
    # ``run_processing_pipeline(fourth_down_probs=True)``.
    _FOURTH_DOWN_DECISION_COLS = [
        "go_wp",
        "first_down_prob",
        "wp_succeed",
        "wp_fail",
        "fg_make_prob",
        "make_fg_wp",
        "miss_fg_wp",
        "fg_wp",
        "punt_wp",
        "go_boost",
        "go_wp_diff",
        "fg_wp_diff",
        "punt_wp_diff",
        "fourth_down_recommendation",
    ]

    def __add_fourth_down_probs(self, play_df):
        """Append the cfb4th 4th-down decision columns to 4th-down plays.

        Runs the decision surface (:func:`sportsdataverse.cfb.cfb_fourth_down.get_4th_down_probs`
        — go / field-goal / punt WP + the max-WP recommendation) on the 4th-down
        subset of the already-enriched frame, then left-joins the decision columns
        back onto every play (non-4th-down rows are null). Imported lazily because
        ``cfb_fourth_down`` imports the EP/WP boosters from this module.
        """
        from sportsdataverse.cfb.cfb_fourth_down import get_4th_down_probs

        cols = self._FOURTH_DOWN_DECISION_COLS
        str_cols = {"fourth_down_recommendation"}
        if "start.down" not in play_df.columns:
            return play_df
        fourth = play_df.filter(pl.col("start.down") == 4)
        if fourth.height == 0:
            # stable schema: emit the decision columns as nulls
            return play_df.with_columns(
                [pl.lit(None).cast(pl.Utf8 if c in str_cols else pl.Float64).alias(c) for c in cols]
            )
        scored = get_4th_down_probs(fourth)  # pandas; preserves input cols incl. ``id``
        keep = ["id"] + [c for c in cols if c in scored.columns]
        dec = pl.from_pandas(scored[keep]).with_columns(pl.col("id").cast(play_df.schema["id"]))
        return play_df.join(dec, on="id", how="left")

    def __add_two_pt_probs(self, play_df):
        """Append the cfb4th two-point (XP vs go-for-2) decision columns to PAT rows.

        Runs :func:`sportsdataverse.cfb.cfb_two_point.get_2pt_probs` on the
        point-after / two-point attempt rows (``pointAfterAttempt.text`` present, or
        ``extra_point_result`` / ``two_point_conv_result`` non-null) and left-joins the
        decision columns back; every other row is null. Idempotent (returns unchanged
        if the columns already exist). Lazy import -- ``cfb_two_point`` imports the
        EP/WP boosters from this module.
        """
        if "two_pt_recommendation" in play_df.columns:
            return play_df
        from sportsdataverse.cfb.cfb_two_point import get_2pt_probs

        decision_cols = ["two_pt_wp", "xp_wp", "prob_2pt", "two_pt_recommendation", "two_pt_wp_diff"]
        pat_mask = pl.lit(False)
        if "pointAfterAttempt.text" in play_df.columns:
            pat_mask = pat_mask | (
                pl.col("pointAfterAttempt.text").is_not_null()
                & (pl.col("pointAfterAttempt.text").cast(pl.Utf8).str.strip_chars() != "")
            )
        for c in ("extra_point_result", "two_point_conv_result"):
            if c in play_df.columns:
                pat_mask = pat_mask | pl.col(c).is_not_null()
        # Pre-2014 games carry no pointAfterAttempt / extra_point_result columns and
        # instead represent the PAT as a SEPARATE play row ("Extra Point Good",
        # "Two-Point Conversion Good", ...). Detect those by play type so the
        # decision surface covers older games too. (On those separate rows
        # pos_score_diff_start is already the post-TD score and pass_td/rush_td are
        # False, so the +6 post-TD adjustment below correctly does not apply.)
        for tcol in ("type.text", "play_type", "type"):
            if tcol in play_df.columns:
                pat_mask = pat_mask | pl.col(tcol).cast(pl.Utf8).str.contains(r"(?i)extra point|two.?point")
                break
        plays = play_df.with_row_index("__twopt_row_idx")
        pat = plays.filter(pat_mask)
        if pat.height == 0:
            for c in decision_cols:
                dtype = pl.Utf8 if c == "two_pt_recommendation" else pl.Float64
                plays = plays.with_columns(pl.lit(None, dtype=dtype).alias(c))
            return plays.drop("__twopt_row_idx")
        # The PAT shares the touchdown's play row, so pos_score_diff_start is the
        # PRE-TD diff; add the 6-pt touchdown so get_2pt_probs scores the XP-vs-2pt
        # decision at the POST-TD score. Only offensive (posteam) touchdowns gain +6;
        # other PAT rows (e.g. defensive-TD tries, where the posteam is not the
        # scoring team) are left as-is rather than mis-credited.
        if {"pass_td", "rush_td", "offense_score_play"}.issubset(set(pat.columns)):
            # pass_td also fires for pick-sixes (its `pass & td_play` branch), so AND with
            # offense_score_play to keep only TDs scored BY the posteam -- a defensive
            # return TD must not push the posteam's score frame +6. (td_play alone is
            # unreliable here: "Passing Touchdown" rows carry td_play == False.)
            off_td = (
                pl.col("pass_td").cast(pl.Boolean).fill_null(False)
                | pl.col("rush_td").cast(pl.Boolean).fill_null(False)
            ) & pl.col("offense_score_play").cast(pl.Boolean).fill_null(False)
            pat = pat.with_columns(
                pl.when(off_td)
                .then(pl.col("pos_score_diff_start") + 6)
                .otherwise(pl.col("pos_score_diff_start"))
                .alias("pos_score_diff_start")
            )
        scored = get_2pt_probs(pat)  # pandas
        scored_pl = pl.from_pandas(scored[["__twopt_row_idx", *decision_cols]]).with_columns(
            pl.col("__twopt_row_idx").cast(pl.UInt32)
        )
        return plays.join(scored_pl, on="__twopt_row_idx", how="left").drop("__twopt_row_idx")

    def run_processing_pipeline(self, fourth_down_probs: bool = True, two_pt_probs: bool = True):
        """Run the full play-by-play processing pipeline.

        Applies every scoring/feature step in order: down detection, play type
        flags, rush/pass flags, team score variables, new play types, penalty
        setup, play category flags, yardage cols, player cols, after cols,
        spread time, EPA, WPA, drive data, and QBR. Also produces an advanced
        box score and stores it under ``advBoxScore`` on the returned dict.

        Idempotent -- subsequent calls return the cached ``self.json``.

        Args:
            fourth_down_probs: when True (default), run the cfb4th decision surface
                (:func:`sportsdataverse.cfb.cfb_fourth_down.get_4th_down_probs`) on the
                enriched frame and append the go/field-goal/punt WP columns plus the
                ``fourth_down_recommendation`` to 4th-down plays (null elsewhere). Pass
                False to skip it (e.g. to avoid loading the fourth-down model).
            two_pt_probs: when True (default), run the cfb4th two-point decision surface
                (:func:`sportsdataverse.cfb.cfb_two_point.get_2pt_probs`) and append
                ``two_pt_wp`` / ``xp_wp`` / ``prob_2pt`` / ``two_pt_recommendation`` /
                ``two_pt_wp_diff`` to point-after / two-point rows (null elsewhere).

        Returns:
            dict: The fully-processed game payload. If the constructor was
            given ``return_keys``, only those keys are returned.

        Example:
            Quick start::

                from sportsdataverse.cfb import CFBPlayProcess
                game = CFBPlayProcess(gameId=401628334)
                game.espn_cfb_pbp()
                processed = game.run_processing_pipeline()
                print(processed["advBoxScore"].keys())

            Pipeline next step (return only selected keys)::

                game = CFBPlayProcess(gameId=401628334, return_keys=["plays", "advBoxScore"])
                game.espn_cfb_pbp()
                trimmed = game.run_processing_pipeline()

            See Also:
                * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB PBP
        """
        if self.ran_pipeline == False:
            pbp_txt = self.__helper_cfb_pbp_drives(self.json)
            self.plays_json = pbp_txt["plays"]

            pbp_json = {
                "gameId": int(self.gameId),
                "plays": self.plays_json.to_dicts(),
                "season": pbp_txt["season"],
                "week": pbp_txt["header"]["week"],
                "gameInfo": pbp_txt["gameInfo"],
                "teamInfo": pbp_txt["header"]["competitions"][0],
                "playByPlaySource": pbp_txt.get("header").get("competitions")[0].get("playByPlaySource"),
                "drives": pbp_txt["drives"],
                "boxscore": pbp_txt["boxscore"],
                "header": pbp_txt["header"],
                "standings": pbp_txt["standings"],
                "leaders": np.array(pbp_txt["leaders"]).tolist(),
                "timeouts": pbp_txt["timeouts"],
                "homeTeamSpread": np.array(pbp_txt["homeTeamSpread"]).tolist(),
                "gameSpread": pbp_txt["gameSpread"],
                "gameSpreadAvailable": pbp_txt["gameSpreadAvailable"],
                "overUnder": pbp_txt["overUnder"],
                "pickcenter": np.array(pbp_txt["pickcenter"]).tolist(),
                "scoringPlays": np.array(pbp_txt["scoringPlays"]).tolist(),
                "winprobability": np.array(pbp_txt["winprobability"]).tolist(),
                "broadcasts": np.array(pbp_txt["broadcasts"]).tolist(),
                "videos": np.array(pbp_txt["videos"]).tolist(),
            }
            self.json = pbp_json
            self.plays_json = pbp_txt["plays"]

            confirmed_corrupt = self.corrupt_pbp_check()

            if confirmed_corrupt:
                return self.json if self.return_keys is None else {k: self.json.get(f"{k}") for k in self.return_keys}

            if (pbp_json.get("header").get("competitions")[0].get("playByPlaySource") != "none") and (
                len(pbp_txt["drives"]) > 0
            ):
                self.plays_json = (
                    self.plays_json.pipe(self.__add_downs_data)
                    .pipe(self.__add_play_type_flags)
                    .pipe(self.__add_rush_pass_flags)
                    .pipe(self.__add_team_score_variables)
                    .pipe(self.__add_new_play_types)
                    .pipe(self.__setup_penalty_data)
                    .pipe(self.__add_play_category_flags)
                    .pipe(self.__add_yardage_cols)
                    .pipe(self.__add_air_yards_cols)
                    .pipe(self.__add_player_cols)
                    .pipe(self.__add_xp_suffix_cols)
                    .pipe(self.__add_attribution_cols)
                    .pipe(self.__refine_play_types_post_attribution)
                    .pipe(self.__after_cols)
                    .pipe(self.__add_series_data)
                    .pipe(self.__add_spread_time)
                    .pipe(self.__process_epa)
                    .pipe(self.__process_wpa)
                    .pipe(self.__process_cpoe)
                    .pipe(self.__process_xpass)
                    .pipe(self.__add_drive_data)
                    .pipe(self.__process_qbr)
                )
                self.plays_json = self.plays_json.pipe(self.__join_participants)
                self.plays_json = self.plays_json.pipe(self.__attach_player_ids)
                if fourth_down_probs:
                    self.plays_json = self.__add_fourth_down_probs(self.plays_json)
                if two_pt_probs:
                    self.plays_json = self.__add_two_pt_probs(self.plays_json)
                self.ran_pipeline = True
                advBoxScore = self.plays_json.pipe(self.create_box_score)
                self.plays_json = self.plays_json.to_dicts()
                pbp_json = {
                    "gameId": int(self.gameId),
                    "plays": self.plays_json,
                    "season": pbp_txt["season"],
                    "week": pbp_txt["header"]["week"],
                    "gameInfo": pbp_txt["gameInfo"],
                    "teamInfo": pbp_txt["header"]["competitions"][0],
                    "playByPlaySource": pbp_txt["playByPlaySource"],
                    "drives": pbp_txt["drives"],
                    "boxscore": pbp_txt["boxscore"],
                    "advBoxScore": advBoxScore,
                    "header": pbp_txt["header"],
                    "standings": pbp_txt["standings"],
                    "leaders": np.array(pbp_txt["leaders"]).tolist(),
                    "timeouts": pbp_txt["timeouts"],
                    "homeTeamSpread": np.array(pbp_txt["homeTeamSpread"]).tolist(),
                    "gameSpread": pbp_txt["gameSpread"],
                    "gameSpreadAvailable": pbp_txt["gameSpreadAvailable"],
                    "overUnder": pbp_txt["overUnder"],
                    "pickcenter": np.array(pbp_txt["pickcenter"]).tolist(),
                    "scoringPlays": np.array(pbp_txt["scoringPlays"]).tolist(),
                    "winprobability": np.array(pbp_txt["winprobability"]).tolist(),
                    "broadcasts": np.array(pbp_txt["broadcasts"]).tolist(),
                    "videos": np.array(pbp_txt["videos"]).tolist(),
                }
                self.json = pbp_json
            self.ran_pipeline = True
            return self.json if self.return_keys is None else {k: self.json.get(f"{k}") for k in self.return_keys}

    def add_fourth_down_probs(self):
        """Add the cfb4th 4th-down decision surface to the processed plays.

        Runs :meth:`run_processing_pipeline` first if it hasn't already, then
        computes the go / punt / field-goal win-probability options plus the
        max-WP ``fourth_down_recommendation`` (and per-option ``*_wp_diff`` and
        ``go_boost``) on every 4th-down row via
        :func:`sportsdataverse.cfb.cfb_fourth_down.get_4th_down_probs`. The new
        columns are written back onto ``self.plays_json`` (and ``self.json``'s
        ``plays``); non-4th-down rows carry nulls for the decision columns.

        Field-goal columns (``fg_make_prob`` / ``make_fg_wp`` / ``miss_fg_wp`` /
        ``fg_wp``) are null when the cfb4th FG model isn't bundled
        (``cfb_fourth_down.FG_MODEL_AVAILABLE`` is False) -- the go + punt surface
        and the recommendation over the available options are still computed.

        Returns:
            polars.DataFrame: ``self.plays_json`` as a frame with the decision
            columns appended (also persisted back onto the instance).

        Example:
            Quick start::

                from sportsdataverse.cfb import CFBPlayProcess
                game = CFBPlayProcess(gameId=401628334)
                game.espn_cfb_pbp()
                game.run_processing_pipeline()
                fourth = game.add_fourth_down_probs()
                print(fourth.filter(pl.col("start.down") == 4)
                            .select(["go_wp", "punt_wp", "fg_wp", "fourth_down_recommendation"])
                            .head())

            See Also:
                * `cfb4th <https://github.com/sportsdataverse/cfb4th>`_ -- R 4th-down decision model
        """
        from sportsdataverse.cfb.cfb_fourth_down import get_4th_down_probs

        if self.ran_pipeline == False:
            self.run_processing_pipeline()

        plays = pl.DataFrame(self.plays_json, infer_schema_length=None)
        decision_cols = self._FOURTH_DOWN_DECISION_COLS
        # run_processing_pipeline() appends these by default, so the documented
        # run_processing_pipeline(); add_fourth_down_probs() flow starts with them
        # already present. Drop any existing copies first so the re-score is a clean
        # overwrite instead of producing suffixed duplicate columns on the join.
        existing = [c for c in decision_cols if c in plays.columns]
        if existing:
            plays = plays.drop(existing)
        plays = plays.with_row_index("__fourth_row_idx")
        fourth = plays.filter(pl.col("start.down") == 4)

        if fourth.height == 0:
            for c in decision_cols:
                dtype = pl.Utf8 if c == "fourth_down_recommendation" else pl.Float64
                plays = plays.with_columns(pl.lit(None, dtype=dtype).alias(c))
            plays = plays.drop("__fourth_row_idx")
            self.plays_json = plays.to_dicts()
            self.json["plays"] = self.plays_json
            return plays

        scored = get_4th_down_probs(fourth)  # pandas
        keep = ["__fourth_row_idx"] + [c for c in decision_cols if c in scored.columns]
        scored_pl = pl.from_pandas(scored[keep]).with_columns(pl.col("__fourth_row_idx").cast(pl.UInt32))

        plays = plays.join(scored_pl, on="__fourth_row_idx", how="left").drop("__fourth_row_idx")
        self.plays_json = plays.to_dicts()
        self.json["plays"] = self.plays_json
        return plays

    def add_2pt_probs(self):
        """Add the cfb4th two-point-conversion decision surface to the processed plays.

        Runs :meth:`run_processing_pipeline` first if it hasn't already, then
        computes the extra-point vs go-for-2 win-probability options on every
        **point-after / two-point conversion** row via
        :func:`sportsdataverse.cfb.cfb_two_point.get_2pt_probs`. A row is treated
        as a PAT / two-point attempt when ``pointAfterAttempt.text`` is present
        (or the derived ``extra_point_result`` / ``two_point_conv_result`` is
        non-null). The new columns -- ``two_pt_wp``, ``xp_wp``, ``prob_2pt``,
        ``two_pt_recommendation`` (``"go_for_2"`` / ``"kick_xp"``) and
        ``two_pt_wp_diff`` (``two_pt_wp - xp_wp``, positive => go for 2) -- are
        written back onto ``self.plays_json`` (and ``self.json``'s ``plays``);
        every other row carries nulls.

        Returns:
            polars.DataFrame: ``self.plays_json`` as a frame with the decision
            columns appended (also persisted back onto the instance).

        Example:
            Quick start::

                from sportsdataverse.cfb import CFBPlayProcess
                game = CFBPlayProcess(gameId=401628334)
                game.espn_cfb_pbp()
                game.run_processing_pipeline()
                out = game.add_2pt_probs()
                print(out.filter(pl.col("two_pt_recommendation").is_not_null())
                         .select(["two_pt_wp", "xp_wp", "two_pt_recommendation"])
                         .head())

            See Also:
                * `cfb4th <https://github.com/sportsdataverse/cfb4th>`_ -- R 4th-down / 2pt decision model
        """
        if self.ran_pipeline == False:
            # run_processing_pipeline applies the two-point surface by default, so a
            # plain run already carries the columns; recompute here for explicit calls.
            self.run_processing_pipeline()

        plays = self.__add_two_pt_probs(pl.DataFrame(self.plays_json, infer_schema_length=None))
        self.plays_json = plays.to_dicts()
        self.json["plays"] = self.plays_json
        return plays

    def run_cleaning_pipeline(self):
        """Run the lighter cleaning pipeline (no EPA/WPA/QBR/box-score).

        Same per-play feature engineering as :meth:`run_processing_pipeline`
        through ``__add_spread_time``, but stops short of the modeling steps.
        Use this when you only need cleaned plays and don't need expected
        points or win probability columns.

        Returns:
            dict: Cleaned game payload (no ``advBoxScore`` key).

        Example:
            Quick start::

                from sportsdataverse.cfb import CFBPlayProcess
                game = CFBPlayProcess(gameId=401628334)
                game.espn_cfb_pbp()
                cleaned = game.run_cleaning_pipeline()
                print(len(cleaned["plays"]))

            See Also:
                * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB PBP
        """
        if self.ran_cleaning_pipeline == False:
            pbp_txt = self.__helper_cfb_pbp_drives(self.json)
            self.plays_json = pbp_txt["plays"]

            pbp_json = {
                "gameId": int(self.gameId),
                "plays": self.plays_json.to_dicts(),
                "season": pbp_txt["season"],
                "week": pbp_txt["header"]["week"],
                "gameInfo": pbp_txt["gameInfo"],
                "teamInfo": pbp_txt["header"]["competitions"][0],
                "playByPlaySource": pbp_txt.get("header").get("competitions")[0].get("playByPlaySource"),
                "drives": pbp_txt["drives"],
                "boxscore": pbp_txt["boxscore"],
                "header": pbp_txt["header"],
                "standings": pbp_txt["standings"],
                "leaders": np.array(pbp_txt["leaders"]).tolist(),
                "timeouts": pbp_txt["timeouts"],
                "homeTeamSpread": np.array(pbp_txt["homeTeamSpread"]).tolist(),
                "gameSpread": pbp_txt["gameSpread"],
                "gameSpreadAvailable": pbp_txt["gameSpreadAvailable"],
                "overUnder": pbp_txt["overUnder"],
                "pickcenter": np.array(pbp_txt["pickcenter"]).tolist(),
                "scoringPlays": np.array(pbp_txt["scoringPlays"]).tolist(),
                "winprobability": np.array(pbp_txt["winprobability"]).tolist(),
                "broadcasts": np.array(pbp_txt["broadcasts"]).tolist(),
                "videos": np.array(pbp_txt["videos"]).tolist(),
            }
            self.json = pbp_json
            self.plays_json = pbp_txt["plays"]

            confirmed_corrupt = self.corrupt_pbp_check()

            if confirmed_corrupt:
                return self.json if self.return_keys is None else {k: self.json.get(f"{k}") for k in self.return_keys}

            if (
                pbp_json.get("header").get("competitions")[0].get("playByPlaySource") != "none"
                and len(pbp_txt["drives"]) > 0
            ):
                self.plays_json = (
                    self.plays_json.pipe(self.__add_downs_data)
                    .pipe(self.__add_play_type_flags)
                    .pipe(self.__add_rush_pass_flags)
                    .pipe(self.__add_team_score_variables)
                    .pipe(self.__add_new_play_types)
                    .pipe(self.__setup_penalty_data)
                    .pipe(self.__add_play_category_flags)
                    .pipe(self.__add_yardage_cols)
                    .pipe(self.__add_air_yards_cols)
                    .pipe(self.__add_player_cols)
                    .pipe(self.__add_xp_suffix_cols)
                    .pipe(self.__after_cols)
                    .pipe(self.__add_series_data)
                    .pipe(self.__add_spread_time)
                )
                self.plays_json = self.plays_json.to_dicts()
                pbp_json = {
                    "gameId": int(self.gameId),
                    "plays": self.plays_json,
                    "season": pbp_txt["season"],
                    "week": pbp_txt["header"]["week"],
                    "gameInfo": pbp_txt["gameInfo"],
                    "teamInfo": pbp_txt["header"]["competitions"][0],
                    "playByPlaySource": pbp_txt["playByPlaySource"],
                    "drives": pbp_txt["drives"],
                    "boxscore": pbp_txt["boxscore"],
                    "header": pbp_txt["header"],
                    "standings": pbp_txt["standings"],
                    "leaders": np.array(pbp_txt["leaders"]).tolist(),
                    "timeouts": pbp_txt["timeouts"],
                    "homeTeamSpread": np.array(pbp_txt["homeTeamSpread"]).tolist(),
                    "gameSpread": pbp_txt["gameSpread"],
                    "gameSpreadAvailable": pbp_txt["gameSpreadAvailable"],
                    "overUnder": pbp_txt["overUnder"],
                    "pickcenter": np.array(pbp_txt["pickcenter"]).tolist(),
                    "scoringPlays": np.array(pbp_txt["scoringPlays"]).tolist(),
                    "winprobability": np.array(pbp_txt["winprobability"]).tolist(),
                    "broadcasts": np.array(pbp_txt["broadcasts"]).tolist(),
                    "videos": np.array(pbp_txt["videos"]).tolist(),
                }
                self.json = pbp_json
            self.ran_cleaning_pipeline = True
            return self.json

    def corrupt_pbp_check(self):
        """Heuristic check for corrupt or incomplete play-by-play.

        Flags games with zero plays, fewer than 50 plays for a completed game,
        or more than 500 plays for a completed game -- all of which historically
        indicate ESPN delivered a malformed PBP payload that should not be
        processed downstream.

        Returns:
            bool: True if PBP looks corrupt and the processing pipeline should
            be skipped, False otherwise.

        Example:
            Quick start::

                from sportsdataverse.cfb import CFBPlayProcess
                game = CFBPlayProcess(gameId=401628334)
                game.espn_cfb_pbp()
                if not game.corrupt_pbp_check():
                    game.run_processing_pipeline()
        """
        if len(self.json["plays"]) == 0:
            logging.debug(
                f"{self.gameId}: appear to be too no plays available ({len(self.json['plays'])}). run_processing_pipeline did not run",
            )
            return True
        if (len(self.json["plays"]) < 50) and (
            self.json.get("header").get("competitions")[0].get("status").get("type").get("completed") == True
        ):
            logging.debug(
                f"{self.gameId}: appear to be too few plays ({len(self.json['plays'])}) for a completed game. run_processing_pipeline did not run",
            )
            return True
        if (len(self.json["plays"]) > 500) and (
            self.json.get("header").get("competitions")[0].get("status").get("type").get("completed") == True
        ):
            logging.debug(
                f"{self.gameId}: appear to be too many plays ({len(self.json['plays'])}) for a completed game. run_processing_pipeline did not run",
            )
            return True
        return False

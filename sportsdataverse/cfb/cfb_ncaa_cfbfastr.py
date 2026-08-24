"""Map stats.ncaa.org MFB pbp (``parse_cfb_ncaa_pbp`` output) to cfbfastR-named columns.

Takes the 49-column NCAA structural frame and emits as many of the ~330
cfbfastR ``cfb_pbp`` columns as a raw-text parser can honestly produce -- ids,
pos/def teams, period/clock, down/distance/yards_to_goal, running scores,
participant names (cfbfastR "First Last" naming), play-type labels, and the
flag family. Model outputs (EPA/WP/...) and ESPN-participant columns are out
of scope by design.

Stateful derivations (period from quarter markers, running scores, play
numbering) run in one ordered pass; window/lag columns are polars ops on top.

Input: the frame from :func:`sportsdataverse.cfb.cfb_ncaa_pbp.parse_cfb_ncaa_pbp`.
Optional ``drives`` / ``linescore`` frames (from
:mod:`sportsdataverse.cfb.cfb_ncaa_box`) refine period + home/away when the
full bundle is available; ``ot_drives`` takes a
:func:`sportsdataverse.cfb.cfb_ncaa_box.parse_cfb_ncaa_drives` frame (overtime
rows are selected internally via ``period > 4``).

**Provenance.** Graduated from the ``ncaa-mfb-football-raw`` producer's
``python/mfb_cfbfastr.py`` (behavior-preserving port; the four heuristics --
FCS defense-labelled-drive majority vote, away/home checkpoint-slot vote,
linescore-arbitrated score snapping, OT synthesis -- are unchanged).
"""

from __future__ import annotations

import math
import re
from typing import TYPE_CHECKING, Any, Optional, Union

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "CFBFASTR_SCHEMA",
    "to_cfbfastr",
]

_TRAILING_CLOCK_RE = re.compile(r"clock (\d{1,2}:\d{2})")
_QTR_MARKER_RE = re.compile(r"start of (\d)(?:st|nd|rd|th) quarter", re.I)
_INT_BY_RE = re.compile(r"intercepted by ([A-Z][\w'.\- ]*,[\w'.\- ]+?)(?: at| return| for|\.|,|$)")
_RET_YDS_RE = re.compile(r"return (\d+) yards", re.I)
# XP/FG result: "good" appears in both cases and "NO GOOD"/"no good" must not match.
_KICK_GOOD_RE = re.compile(r"(?<!no )good", re.I)

#: play rows that are game furniture, not plays (dropped from the cfbfastR frame
#: after they've fed the stateful pass).
_MARKER_TYPES = {"drive_start", "coin_toss"}

#: end_how -> approximate cfbfastR play_type label for synthesized OT rows.
_OT_END_HOW_LABEL = {
    "TD": "Touchdown",
    "FG": "Field Goal Good",
    "FGA": "Field Goal Missed",
    "PUNT": "Punt",
    "INT": "Pass Interception Return",
    "FUMB": "Fumble Recovery (Opponent)",
    "DOWNS": "Turnover on Downs",
    "HALF": "End of Game",
    "END": "End of Game",
}

#: every emitted column, in order. The stateful pass builds the leading
#: columns; the trailing window/lag columns (from ``lag_pos_team`` on) are
#: polars ops appended after frame construction. Dtypes were frozen from the
#: mapper's output on the committed real-game fixture corpus.
CFBFASTR_SCHEMA: "dict[str, pl.DataType]" = {
    # ids / context
    "game_id": pl.Int64,
    "id_play": pl.Int64,
    "drive_id": pl.Int64,
    "game_play_number": pl.Int64,
    "half_play_number": pl.Int64,
    "drive_play_number": pl.Int64,
    "drive_number": pl.Int64,
    "season": pl.Int64,
    "year": pl.Int64,
    "week": pl.Int64,
    "period": pl.Int64,
    "half": pl.Int64,
    "clock.minutes": pl.Int64,
    "clock.seconds": pl.Int64,
    "TimeSecsRem": pl.Int64,
    "Under_two": pl.Boolean,
    # teams
    "pos_team": pl.Utf8,
    "def_pos_team": pl.Utf8,
    "offense_play": pl.Utf8,
    "defense_play": pl.Utf8,
    "home": pl.Utf8,
    "away": pl.Utf8,
    # score (after the play, cfbfastR convention for offense/defense_score)
    "pos_team_score": pl.Int64,
    "def_pos_team_score": pl.Int64,
    "offense_score": pl.Int64,
    "defense_score": pl.Int64,
    "pos_score_diff": pl.Int64,
    "score_pts": pl.Int64,
    "scoring_play": pl.Boolean,
    "scoring": pl.Boolean,
    # situation
    "down": pl.Int64,
    "distance": pl.Int64,
    "yard_line": pl.Utf8,
    "yards_to_goal": pl.Int64,
    "yards_to_goal_end": pl.Int64,
    "Goal_To_Go": pl.Boolean,
    "log_ydstogo": pl.Float64,
    "yards_gained": pl.Int64,
    # typing
    "play_type": pl.Utf8,
    "orig_play_type": pl.Utf8,
    "play_text": pl.Utf8,
    "rush": pl.Boolean,
    "rush_td": pl.Boolean,
    "pass": pl.Boolean,
    "pass_td": pl.Boolean,
    "pass_attempt": pl.Boolean,
    "completion": pl.Boolean,
    "target": pl.Boolean,
    "sack": pl.Boolean,
    "sack_vec": pl.Boolean,
    "int": pl.Boolean,
    "int_td": pl.Boolean,
    "turnover_vec": pl.Boolean,
    "downs_turnover": pl.Boolean,
    "touchdown": pl.Boolean,
    "td_play": pl.Boolean,
    "safety": pl.Boolean,
    "fumble_vec": pl.Boolean,
    "punt": pl.Boolean,
    "punt_play": pl.Boolean,
    "kickoff_play": pl.Boolean,
    "kick_play": pl.Boolean,
    "fg_inds": pl.Boolean,
    "fg_made": pl.Boolean,
    "punt_blocked": pl.Boolean,
    "punt_fair_catch": pl.Boolean,
    "firstD_by_yards": pl.Boolean,
    "firstD_by_penalty": pl.Boolean,
    # penalties
    "penalty_flag": pl.Boolean,
    "penalty_no_play": pl.Boolean,
    "penalty_declined": pl.Boolean,
    "penalty_offset": pl.Boolean,
    "penalty_text": pl.Utf8,
    "yds_penalty": pl.Int64,
    # participants (cfbfastR "First Last")
    "rusher_player_name": pl.Utf8,
    "passer_player_name": pl.Utf8,
    "receiver_player_name": pl.Utf8,
    "interception_player_name": pl.Utf8,
    "punter_player_name": pl.Utf8,
    "punt_returner_player_name": pl.Utf8,
    "fg_kicker_player_name": pl.Utf8,
    "kickoff_player_name": pl.Utf8,
    "kickoff_returner_player_name": pl.Utf8,
    "yds_rushed": pl.Int64,
    "yds_receiving": pl.Int64,
    "yds_sacked": pl.Int64,
    "yds_punted": pl.Int64,
    "yds_punt_return": pl.Int64,
    "yds_kickoff": pl.Int64,
    "yds_kickoff_return": pl.Int64,
    "yds_int_return": pl.Int64,
    "yds_fg": pl.Int64,
    "drive_result": pl.Utf8,
    "drive_scoring": pl.Boolean,
    "ot_synthesized": pl.Boolean,
    # window/lag bookkeeping (appended after the stateful pass)
    "lag_pos_team": pl.Utf8,
    "lead_pos_team": pl.Utf8,
    "lag_play_type": pl.Utf8,
    "lead_play_type": pl.Utf8,
    "lag_play_text": pl.Utf8,
    "lead_play_text": pl.Utf8,
    "change_of_pos_team": pl.Boolean,
    "play_after_turnover": pl.Boolean,
    "n_plays_in_game": pl.UInt32,
}

#: columns appended by the window/lag pass (not part of the row dicts).
_WINDOW_COLS = (
    "lag_pos_team",
    "lead_pos_team",
    "lag_play_type",
    "lead_play_type",
    "lag_play_text",
    "lead_play_text",
    "change_of_pos_team",
    "play_after_turnover",
    "n_plays_in_game",
)

_ROW_SCHEMA: "dict[str, pl.DataType]" = {k: t for k, t in CFBFASTR_SCHEMA.items() if k not in _WINDOW_COLS}


def _norm_team(name: "str | None") -> str:
    """Normalize a team label for cross-surface matching: the linescore and the
    drive titles can disagree on variant spellings ('Saint Anselm' vs
    'St. Anselm')."""
    s = (name or "").lower().strip()
    s = re.sub(r"\bsaint\b", "st.", s)
    return re.sub(r"[^a-z0-9&]+", " ", s).strip()


def _first_last(name: "str | None") -> "str | None":
    """NCAA 'Last[ Suffix],First' -> cfbfastR 'First Last[ Suffix]'."""
    if not name or "," not in name:
        return name
    last, first = name.split(",", 1)
    return f"{first.strip()} {last.strip()}"


def _clock_secs(clock: "str | None") -> "int | None":
    if not clock or ":" not in clock:
        return None
    mm, ss = clock.split(":", 1)
    try:
        return int(mm) * 60 + int(ss)
    except ValueError:
        return None


def _own_side(df: pl.DataFrame) -> "dict[str, str]":
    """Infer each offense's own yard-line side code (e.g. Merrimack -> 'MC').

    Drives overwhelmingly start in the offense's own territory, so of the two
    possible (team name -> side code) assignments, pick the one under which
    more first-plays-of-drive sit on the offense's own side.
    """
    teams = [t for t in df.get_column("offense").unique().to_list() if t]
    sides = [s for s in df.get_column("yard_line_side").unique().to_list() if s]
    if len(teams) != 2 or len(sides) != 2:
        return {}
    firsts = (
        df.filter(pl.col("yard_line_side").is_not_null())
        .group_by("drive_number", maintain_order=True)
        .first()
        .select("offense", "yard_line_side")
        .to_dicts()
    )
    a = {teams[0]: sides[0], teams[1]: sides[1]}
    b = {teams[0]: sides[1], teams[1]: sides[0]}
    score_a = sum(1 for r in firsts if a.get(r["offense"]) == r["yard_line_side"])
    score_b = sum(1 for r in firsts if b.get(r["offense"]) == r["yard_line_side"])
    return a if score_a >= score_b else b


def _play_type_label(r: "dict[str, Any]") -> str:
    """Map the NCAA structural play_type to the cfbfastR play_type vocabulary."""
    pt, td = r["play_type"], bool(r["is_touchdown"])
    if pt == "rush" or pt == "kneel":
        return "Rushing Touchdown" if td else "Rush"
    if pt == "pass":
        if r["turnover_type"] == "interception":
            return "Interception Return Touchdown" if td else "Pass Interception Return"
        if r["pass_complete"]:
            return "Passing Touchdown" if td else "Pass Reception"
        return "Pass Incompletion"
    if pt == "sack":
        return "Sack"
    if pt == "punt":
        return "Blocked Punt" if "blocked" in (r["play_text"] or "").lower() else "Punt"
    if pt == "kickoff":
        return "Kickoff Return Touchdown" if td else "Kickoff"
    if pt == "field_goal":
        return "Field Goal Good" if r["fg_made"] else "Field Goal Missed"
    if pt == "extra_point":
        return "Extra Point Good" if _KICK_GOOD_RE.search(r["play_text"] or "") else "Extra Point Missed"
    if pt == "two_point":
        return "Two Point Pass" if r["passer"] else "Two Point Rush"
    if pt == "penalty":
        return "Penalty"
    if pt == "timeout":
        return "Timeout"
    if pt == "period_marker":
        return "End Period"
    return str(pt)


def to_cfbfastr(
    pbp: pl.DataFrame,
    *,
    season: "Optional[int]" = None,
    week: "Optional[int]" = None,
    drives: "Optional[pl.DataFrame]" = None,
    linescore: "Optional[pl.DataFrame]" = None,
    drive_titles: "Optional[pl.DataFrame]" = None,
    ot_drives: "Optional[pl.DataFrame]" = None,
    scoring_summary: "Optional[pl.DataFrame]" = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """cfbfastR-named play frame from the NCAA structural pbp frame.

    Args:
        pbp: Output of :func:`sportsdataverse.cfb.cfb_ncaa_pbp.parse_cfb_ncaa_pbp`
            (one game).
        season: Season year (2025 = fall-2025), written to ``season``/``year``.
        week: Optional week number (from the schedule master).
        drives: Optional :func:`sportsdataverse.cfb.cfb_ncaa_box.parse_cfb_ncaa_drives`
            frame -- refines ``period`` per drive when quarter markers are
            missing from the pbp page.
        linescore: Optional
            :func:`sportsdataverse.cfb.cfb_ncaa_box.parse_cfb_ncaa_linescore`
            frame -- provides ``home``/``away`` team names and the official
            per-team finals.
        drive_titles: Optional
            :func:`sportsdataverse.cfb.cfb_ncaa_pbp.parse_cfb_ncaa_drive_titles`
            frame -- authoritative per-drive team labels (fixes graduated-parser
            team truncation) and running-score checkpoints the play-level score
            snaps to at each drive boundary (self-heals OT scoring rules +
            missed events).
        ot_drives: Optional
            :func:`sportsdataverse.cfb.cfb_ncaa_box.parse_cfb_ncaa_drives`
            frame for the OT-synthesis pass -- overtime rows (``period > 4``)
            are selected internally, so the full drives frame can be passed
            as-is.
        scoring_summary: Optional
            :func:`sportsdataverse.cfb.cfb_ncaa_box.parse_cfb_ncaa_scoring_summary`
            frame -- running-score checkpoints for the synthesized OT rows and
            the final-drive snap.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of ``polars``.

    Returns:
        A ``polars.DataFrame`` (or ``pandas.DataFrame`` when
        ``return_as_pandas``) with one row per play (markers/furniture dropped)
        and the columns of :data:`CFBFASTR_SCHEMA`. Empty input returns a
        **zero-row frame carrying the documented schema**.

    Example:
        Quick start::

            from sportsdataverse.cfb import parse_cfb_ncaa_pbp, to_cfbfastr
            pbp = parse_cfb_ncaa_pbp(open("play_by_play_5362431.html").read(), contest_id=5362431)
            df = to_cfbfastr(pbp, season=2024)
            print(df.shape)

        Final score from the running-score columns::

            df.select("pos_team", "pos_team_score", "def_pos_team", "def_pos_team_score").row(-1)

        See Also:
            * `cfbfastR`_ -- the ESPN-sourced college-football ``cfb_pbp``
              frame (R) whose column vocabulary this mapper targets

        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    if pbp.height == 0:
        empty = pl.DataFrame(schema=CFBFASTR_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty
    title_team: "dict[int, str]" = {}
    checkpoint: "dict[int, tuple[int, int]]" = {}  # drive -> (score_away, score_home)
    title_result: "dict[int, str]" = {}
    if drive_titles is not None and drive_titles.height:
        for t in drive_titles.to_dicts():
            dn = t["drive_number"]
            if t["team"]:
                title_team[dn] = t["team"]
            if t["result"]:
                title_result[dn] = t["result"]
            if t["score_away"] is not None and t["score_home"] is not None:
                checkpoint[dn] = (t["score_away"], t["score_home"])
    # Some (FCS) pages label every drive h5 with the DEFENSE. The per-drive
    # "X drive start at MM:SS" markers name the true offense, so majority-vote
    # the markers against the titles (prefix-matched -- markers use name
    # variants like "Southern (La.)" for title "Southern U.") and flip the
    # whole page's title assignment when the majority disagree.
    if title_team and len(set(title_team.values())) == 2:
        two = sorted(set(title_team.values()))

        def _match(marker: str) -> "Optional[str]":
            scores = {t: 0 for t in two}
            for t in two:
                n = 0
                for a, b in zip(marker.lower(), t.lower()):
                    if a != b:
                        break
                    n += 1
                scores[t] = n
            best = max(two, key=lambda t: scores[t])
            other = next(t for t in two if t != best)
            return best if scores[best] >= 4 and scores[best] > scores[other] else None

        agree = disagree = 0
        for r in pbp.filter(pl.col("play_type") == "drive_start").to_dicts():
            m = re.match(r"^(.+?) drive start at", r["play_text"] or "")
            marker_team = _match(m.group(1)) if m else None
            titled = title_team.get(r["drive_number"])
            if marker_team and titled in two:
                agree += marker_team == titled
                disagree += marker_team != titled
        if disagree > agree:
            flip = {two[0]: two[1], two[1]: two[0]}
            title_team = {dn: flip[t] for dn, t in title_team.items()}
    if title_team:
        # authoritative per-drive team labels fix truncated offense values
        pbp = pbp.with_columns(
            pl.col("drive_number")
            .replace_strict(title_team, default=None, return_dtype=pl.Utf8)
            .fill_null(pl.col("offense"))
            .alias("offense")
        )
    own_side = _own_side(pbp)
    teams = (
        sorted(set(title_team.values()))
        if len(set(title_team.values())) == 2
        else [t for t in pbp.get_column("offense").unique().to_list() if t]
    )
    home = away = None
    if linescore is not None and linescore.height:
        ls = linescore.group_by("team", "home_away").len()
        for r in ls.to_dicts():
            if r["home_away"] == "home":
                home = r["team"]
            elif r["home_away"] == "away":
                away = r["team"]
    # Checkpoint slot -> team mapping. The title score pair is USUALLY
    # "away - home", but some (mostly FCS) pages emit it "home - away", so
    # trusting the linescore's away/home for SNAPPING swaps the whole game.
    # MAJORITY VOTE over every scoring drive whose checkpoint moved exactly one
    # slot (the mover is almost always the drive's own team; defensive TDs are
    # rare) decides which team owns the first slot. The linescore still
    # supplies the emitted home/away columns.
    snap_first, snap_second = away, home
    if checkpoint and len(teams) == 2:
        tally = dict.fromkeys(teams, 0)
        prev = (0, 0)
        for dn in sorted(checkpoint):
            ca, ch = checkpoint[dn]
            team = title_team.get(dn)
            if team in teams and title_result.get(dn) in ("TD", "FG"):
                if ca > prev[0] and ch == prev[1]:
                    tally[team] += 1
                elif ch > prev[1] and ca == prev[0]:
                    tally[team] -= 1
            prev = (ca, ch)
        if any(tally.values()):
            snap_first = max(teams, key=lambda t: tally[t])
            snap_second = next(t for t in teams if t != snap_first)
    if away not in teams or home not in teams:
        away, home = snap_first, snap_second
    drive_period: "dict[int, int]" = {}
    if drives is not None and drives.height:
        drive_period = {
            r["drive_number"]: r["quarter"]
            for r in drives.select("drive_number", "quarter").to_dicts()
            if r["drive_number"] is not None and r["quarter"] is not None
        }

    game_id = pbp.get_column("contest_id")[0]
    gid = int(game_id) if game_id and str(game_id).isdigit() else None

    period = 1
    last_td_team: "Optional[str]" = None
    score: "dict[str, int]" = {t: 0 for t in teams}
    game_play_number = 0
    half_play_number = 0
    prev_half = 1
    prev_drive: "Optional[int]" = None
    rows: "list[dict[str, Any]]" = []
    drive_play_counter: "dict[int, int]" = {}

    # the scoring summary is a SUPERSET of the drive-title checkpoints (some
    # pages score plays the final drive title never checkpoints -- e.g. a late
    # TD after the last titled drive), so the game's last drive snaps to its
    # final regulation running-score instead.
    last_reg_summary: "Optional[tuple[int, int]]" = None
    if scoring_summary is not None and scoring_summary.height:
        reg = scoring_summary.filter(
            (pl.col("period") <= 4) & pl.col("score_away").is_not_null() & pl.col("score_home").is_not_null()
        )
        if reg.height:
            last_reg_summary = (reg["score_away"][-1], reg["score_home"][-1])
    last_drive_number = max(checkpoint) if checkpoint else None
    # official per-team finals from the linescore (name-normalized onto the
    # title team labels) -- the arbiter when the titles and the scoring
    # summary disagree at game end (source self-inconsistencies exist:
    # Mercyhurst 5366186 titles say 55, the official book says 48).
    official_final: "dict[str, int]" = {}
    if linescore is not None and linescore.height and len(teams) == 2:
        for r in linescore.group_by("team").agg(pl.col("final").max()).to_dicts():
            if r["final"] is None or not r["team"]:
                continue
            match = next((t for t in teams if _norm_team(t) == _norm_team(r["team"])), None)
            if match:
                official_final[match] = r["final"]
        if len(official_final) != 2:
            official_final = {}

    def _final_matches(cand: "tuple[int, int]") -> bool:
        return (
            bool(official_final)
            and (
                official_final.get(snap_first or ""),
                official_final.get(snap_second or ""),
            )
            == cand
        )

    def _snap(drive: "Optional[int]") -> None:
        """Snap the running score to the checkpoint of a finished drive."""
        if snap_first not in score or snap_second not in score:
            return
        if drive == last_drive_number and last_reg_summary is not None:
            cp = checkpoint.get(drive) if drive is not None else None
            # candidates: title checkpoint vs summary final. The official
            # linescore arbitrates; without its vote, take the further-along
            # one (the summary trails on OT pages).
            if cp is not None and _final_matches(cp):
                score[snap_first], score[snap_second] = cp
                return
            if _final_matches(last_reg_summary) or sum(last_reg_summary) >= sum(cp or (0, 0)):
                score[snap_first], score[snap_second] = last_reg_summary
                return
        if drive in checkpoint:
            score[snap_first], score[snap_second] = checkpoint[drive]

    all_rows = pbp.to_dicts()
    # the checkpoint is the score AFTER a drive, so the drive's LAST play must
    # emit exactly it (event-sourcing can't see OT-shootout scoring rules).
    last_play_of_drive: "dict[int, int]" = {
        r["drive_number"]: i for i, r in enumerate(all_rows) if r["drive_number"] is not None
    }

    for i, r in enumerate(all_rows):
        text = r["play_text"] or ""
        qm = _QTR_MARKER_RE.search(text.lower())
        if qm:
            period = int(qm.group(1))
        if r["drive_number"] in drive_period:
            period = drive_period[r["drive_number"]]
        half = 1 if period <= 2 else 2
        if half != prev_half:
            half_play_number = 0
            prev_half = half

        if r["drive_number"] != prev_drive:
            _snap(prev_drive)
            prev_drive = r["drive_number"]

        offense = title_team.get(r["drive_number"]) or r["offense"]
        defense = next((t for t in teams if t != offense), None) if offense else None

        # running score -- award points to the right side of the ball
        pts_off = pts_def = 0
        if r["play_type"] not in (
            "timeout",
            "period_marker",
            "drive_start",
            "coin_toss",
        ):
            if r["is_touchdown"]:
                # a fumble-return TD has turnover_type set WITHOUT is_turnover
                to_defense = r["turnover_type"] in ("interception", "fumble")
                if to_defense:
                    pts_def += 6
                    last_td_team = defense
                else:
                    pts_off += 6
                    last_td_team = offense
            if r["play_type"] == "field_goal" and r["fg_made"]:
                pts_off += 3
            # XP/2pt belong to whoever scored the preceding TD (a defensive TD's
            # try is kicked by the drive's DEFENSE, so drive offense is wrong).
            if r["play_type"] == "extra_point" and _KICK_GOOD_RE.search(text):
                if (last_td_team or offense) == defense:
                    pts_def += 1
                else:
                    pts_off += 1
            if r["play_type"] == "two_point" and "successful" in text.lower():
                if (last_td_team or offense) == defense:
                    pts_def += 2
                else:
                    pts_off += 2
            if r["is_safety"]:
                pts_def += 2
        if offense and pts_off:
            score[offense] = score.get(offense, 0) + pts_off
        if defense and pts_def:
            score[defense] = score.get(defense, 0) + pts_def
        if last_play_of_drive.get(r["drive_number"]) == i:
            _snap(r["drive_number"])

        if r["play_type"] in _MARKER_TYPES:
            continue
        game_play_number += 1
        half_play_number += 1
        dn = r["drive_number"]
        drive_play_counter[dn] = drive_play_counter.get(dn, 0) + 1

        clock = r["clock"]
        if not clock:
            cm = _TRAILING_CLOCK_RE.search(text)
            clock = cm.group(1) if cm else None
        secs = _clock_secs(clock)
        time_secs_rem = (4 - period) * 900 + secs if secs is not None and period <= 4 else None

        side, num = r["yard_line_side"], r["yard_line_number"]
        ytg = None
        if side is not None and num is not None and offense in own_side:
            ytg = 100 - num if own_side[offense] == side else num
        end_ytg = None
        eyl = r["end_yard_line"] or ""
        em = re.match(r"([A-Za-z&]{1,4})(\d+)$", eyl)
        if em and offense in own_side:
            end_ytg = 100 - int(em.group(2)) if own_side[offense] == em.group(1) else int(em.group(2))

        pt = r["play_type"]
        is_rush = pt in ("rush", "kneel")
        is_pass_att = pt == "pass"
        is_sack = pt == "sack"
        completion = bool(r["pass_complete"]) if is_pass_att else False
        interception = r["turnover_type"] == "interception"
        scoring_play = bool(pts_off or pts_def)
        int_m = _INT_BY_RE.search(text)
        ret_m = _RET_YDS_RE.search(text)

        pos_score = score.get(offense, 0) if offense else None
        def_score = score.get(defense, 0) if defense else None
        rows.append(
            {
                # ids / context
                "game_id": gid,
                "id_play": gid * 10_000 + game_play_number if gid else None,
                "drive_id": gid * 100 + dn if gid and dn else None,
                "game_play_number": game_play_number,
                "half_play_number": half_play_number,
                "drive_play_number": drive_play_counter[dn],
                "drive_number": dn,
                "season": season,
                "year": season,
                "week": week,
                "period": period,
                "half": half,
                "clock.minutes": secs // 60 if secs is not None else None,
                "clock.seconds": secs % 60 if secs is not None else None,
                "TimeSecsRem": time_secs_rem,
                "Under_two": time_secs_rem is not None and time_secs_rem <= 120,
                # teams
                "pos_team": offense,
                "def_pos_team": defense,
                "offense_play": offense,
                "defense_play": defense,
                "home": home,
                "away": away,
                # score (after the play, cfbfastR convention for offense/defense_score)
                "pos_team_score": pos_score,
                "def_pos_team_score": def_score,
                "offense_score": pos_score,
                "defense_score": def_score,
                "pos_score_diff": pos_score - def_score if pos_score is not None and def_score is not None else None,
                "score_pts": pts_off - pts_def,
                "scoring_play": scoring_play,
                "scoring": scoring_play,
                # situation
                "down": r["down"],
                "distance": r["distance"],
                "yard_line": r["yard_line"],
                "yards_to_goal": ytg,
                "yards_to_goal_end": end_ytg,
                "Goal_To_Go": (r["distance"] is not None and ytg is not None and ytg <= r["distance"]),
                "log_ydstogo": math.log(r["distance"]) if r["distance"] else None,
                "yards_gained": r["yards_gained"],
                # typing
                "play_type": _play_type_label(r),
                "orig_play_type": pt,
                "play_text": r["play_text"],
                "rush": is_rush,
                "rush_td": is_rush and bool(r["is_touchdown"]),
                "pass": is_pass_att or is_sack,
                "pass_td": is_pass_att and completion and bool(r["is_touchdown"]),
                "pass_attempt": is_pass_att,
                "completion": completion,
                "target": is_pass_att and r["receiver"] is not None,
                "sack": is_sack,
                "sack_vec": is_sack,
                "int": interception,
                "int_td": interception and bool(r["is_touchdown"]),
                "turnover_vec": bool(r["is_turnover"]) or r["turnover_type"] == "fumble",
                "downs_turnover": r["turnover_type"] == "downs",
                "touchdown": bool(r["is_touchdown"]),
                "td_play": bool(r["is_touchdown"]),
                "safety": bool(r["is_safety"]),
                "fumble_vec": bool(r["is_fumble"]),
                "punt": pt == "punt",
                "punt_play": pt == "punt",
                "kickoff_play": pt == "kickoff",
                "kick_play": pt == "field_goal" or pt == "extra_point",
                "fg_inds": pt == "field_goal",
                "fg_made": r["fg_made"],
                "punt_blocked": pt == "punt" and "blocked" in text.lower(),
                "punt_fair_catch": pt == "punt" and bool(r["fair_catch"]),
                "firstD_by_yards": bool(r["is_first_down"]) and not bool(r["penalty_flag"]),
                "firstD_by_penalty": bool(r["is_first_down"]) and bool(r["penalty_flag"]),
                # penalties
                "penalty_flag": bool(r["penalty_flag"]),
                "penalty_no_play": bool(r["no_play"]),
                "penalty_declined": "declined" in text.lower(),
                "penalty_offset": "off-setting" in text.lower() or "offsetting" in text.lower(),
                "penalty_text": r["penalty_type"],
                "yds_penalty": r["penalty_yards"],
                # participants (cfbfastR "First Last")
                "rusher_player_name": _first_last(r["rusher"]) if is_rush else None,
                "passer_player_name": _first_last(r["passer"]) if (is_pass_att or is_sack) else None,
                "receiver_player_name": _first_last(r["receiver"]),
                "interception_player_name": _first_last(int_m.group(1)) if int_m and interception else None,
                "punter_player_name": _first_last(r["punter"]),
                "punt_returner_player_name": _first_last(r["returner"]) if pt == "punt" else None,
                "fg_kicker_player_name": _first_last(r["kicker"]) if pt in ("field_goal", "extra_point") else None,
                "kickoff_player_name": _first_last(r["kicker"]) if pt == "kickoff" else None,
                "kickoff_returner_player_name": _first_last(r["returner"]) if pt == "kickoff" else None,
                "yds_rushed": r["yards_gained"] if is_rush else None,
                "yds_receiving": r["yards_gained"] if completion else None,
                "yds_sacked": -r["yards_gained"] if is_sack and r["yards_gained"] is not None else None,
                "yds_punted": r["punt_yards"],
                "yds_punt_return": r["return_yards"] if pt == "punt" else None,
                "yds_kickoff": r["kick_yards"],
                "yds_kickoff_return": r["return_yards"] if pt == "kickoff" else None,
                "yds_int_return": int(ret_m.group(1)) if ret_m and interception else None,
                "yds_fg": r["fg_distance"],
                "drive_result": r["drive_result"],
                "drive_scoring": r["drive_scored"],
                "ot_synthesized": False,
            }
        )

    # --- OT synthesis: stats.ncaa.org pbp pages omit OT drives. Rebuild them
    # (one row per drive) from the drives tab, with scores walked through the
    # scoring-summary running-score checkpoints. Rows are flagged
    # ot_synthesized=True; play_text is the summary's real description when it
    # ships one, else an honest synthesized descriptor.
    max_pbp_drive = max((r["drive_number"] for r in rows if r["drive_number"]), default=0)
    # official_final (built above, name-normalized) also serves as the mop-up
    # for OT tails no surface checkpoints (2-pt shootouts, walk-off return TDs).
    # Page-wise: does the pbp already reach the final (its titles include OT,
    # like some FBS pages do)? Then synthesize nothing. Drive numbers can NOT
    # be compared across the pbp and drives tabs (they misalign by one on some
    # pages), so the decision is score-based and synthesized drives renumber
    # from max_pbp_drive + 1.
    pbp_reaches_final = bool(
        checkpoint and official_final and sum(checkpoint[max(checkpoint)]) >= sum(official_final.values())
    )
    # the graduated parse_cfb_ncaa_drives frame carries the whole game; only
    # its overtime rows (period 5+) feed the synthesis.
    ot_rows = ot_drives.filter(pl.col("period") > 4) if ot_drives is not None and ot_drives.height else None
    if rows and not pbp_reaches_final and ot_rows is not None and ot_rows.height:
        ot_checkpoints = (
            scoring_summary.filter(pl.col("period") > 4).to_dicts()
            if scoring_summary is not None and scoring_summary.height
            else []
        )
        template = dict.fromkeys(rows[-1].keys())
        n_synth = 0
        for od in sorted(ot_rows.to_dicts(), key=lambda d: d["drive_number"]):
            n_synth += 1
            od = dict(od)
            od["drive_number"] = max_pbp_drive + n_synth
            offense = od["team"]
            defense = next((t for t in teams if t != offense), None)
            scoring = od["end_how"] in ("TD", "FG", "SAF")
            summary_text = None
            if scoring and ot_checkpoints:
                cp = ot_checkpoints.pop(0)
                summary_text = cp["play_text"]
                if snap_first in score and snap_second in score and cp["score_away"] is not None:
                    # scoring-summary slots follow the same per-page order
                    score[snap_first], score[snap_second] = (
                        cp["score_away"],
                        cp["score_home"],
                    )
            elif scoring and offense in score:
                score[offense] += 3 if od["end_how"] == "FG" else 6
            game_play_number += 1
            half_play_number += 1
            pos_s = score.get(offense) if offense else None
            def_s = score.get(defense) if defense else None
            row = dict(template)
            row.update(
                {
                    "game_id": gid,
                    "id_play": gid * 10_000 + game_play_number if gid else None,
                    "drive_id": gid * 100 + od["drive_number"] if gid else None,
                    "game_play_number": game_play_number,
                    "half_play_number": half_play_number,
                    "drive_play_number": 1,
                    "drive_number": od["drive_number"],
                    "season": season,
                    "year": season,
                    "week": week,
                    "period": od["period"],
                    "half": 3,
                    "pos_team": offense,
                    "def_pos_team": defense,
                    "offense_play": offense,
                    "defense_play": defense,
                    "home": home,
                    "away": away,
                    "pos_team_score": pos_s,
                    "def_pos_team_score": def_s,
                    "offense_score": pos_s,
                    "defense_score": def_s,
                    "pos_score_diff": pos_s - def_s if pos_s is not None and def_s is not None else None,
                    "scoring_play": scoring,
                    "scoring": scoring,
                    "yard_line": od["start_yard_line"],
                    "play_type": _OT_END_HOW_LABEL.get(od["end_how"], od["end_how"]),
                    "orig_play_type": "ot_drive",
                    "play_text": summary_text
                    or (
                        f"{offense} OT drive ({od['end_how']}): "
                        f"{od['n_plays']} plays, {od['yards']} yards, "
                        f"{od['start_yard_line']} to {od['end_yard_line']}"
                    ),
                    "touchdown": od["end_how"] == "TD",
                    "td_play": od["end_how"] == "TD",
                    "fg_inds": od["end_how"] in ("FG", "FGA"),
                    "fg_made": od["end_how"] == "FG" if od["end_how"] in ("FG", "FGA") else None,
                    "punt": od["end_how"] == "PUNT",
                    "punt_play": od["end_how"] == "PUNT",
                    "int": od["end_how"] == "INT",
                    "turnover_vec": od["end_how"] in ("INT", "FUMB", "DOWNS"),
                    "downs_turnover": od["end_how"] == "DOWNS",
                    "drive_result": od["end_how"],
                    "drive_scoring": scoring,
                    "ot_synthesized": True,
                }
            )
            rows.append(row)

        # mop-up: OT tails no surface checkpoints (3OT+ two-point shootouts,
        # walk-off return TDs) leave the last synthesized row short of the
        # official final -- reconcile it against the linescore per-team finals.
        if (
            rows
            and rows[-1].get("ot_synthesized")
            and official_final
            and rows[-1]["pos_team"] in official_final
            and rows[-1]["def_pos_team"] in official_final
        ):
            last = rows[-1]
            want_pos = official_final[last["pos_team"]]
            want_def = official_final[last["def_pos_team"]]
            if (last["pos_team_score"], last["def_pos_team_score"]) != (
                want_pos,
                want_def,
            ):
                last["pos_team_score"], last["def_pos_team_score"] = want_pos, want_def
                last["offense_score"], last["defense_score"] = want_pos, want_def
                last["pos_score_diff"] = want_pos - want_def
                last["scoring_play"] = last["scoring"] = True
                score[last["pos_team"]] = want_pos
                score[last["def_pos_team"]] = want_def

    df = pl.DataFrame(rows, schema=_ROW_SCHEMA)
    # window/lag bookkeeping over the ordered game
    df = df.with_columns(
        pl.col("pos_team").shift(1).alias("lag_pos_team"),
        pl.col("pos_team").shift(-1).alias("lead_pos_team"),
        pl.col("play_type").shift(1).alias("lag_play_type"),
        pl.col("play_type").shift(-1).alias("lead_play_type"),
        pl.col("play_text").shift(1).alias("lag_play_text"),
        pl.col("play_text").shift(-1).alias("lead_play_text"),
        (pl.col("pos_team") != pl.col("pos_team").shift(1)).alias("change_of_pos_team"),
        (pl.col("turnover_vec").shift(1) == True).alias("play_after_turnover"),  # noqa: E712
        pl.len().alias("n_plays_in_game"),
    )
    return df.to_pandas() if return_as_pandas else df

"""Credited-team attribution, penalty enforcement and play-type refinement shared by
the CFB and NFL play-by-play processors.

The logic is league-agnostic (it reads ``pos_team`` / ``def_pos_team``, the
play-type flags and the ESPN text); only the three text parsers that read a
team token out of the play text differ per league, so they are injected:

* ``parse_recovery_abbrevs(text) -> list[str]`` -- ordered upper-case team
  abbreviations that recovered the ball ("recovered by TEAM");
* ``parse_penalty_team_side(row) -> "home" | "away" | None`` -- the penalized
  side from the play text;
* ``parse_penalty_spot(row) -> "<home|away|mid>:<yardline>" | None`` -- the
  enforcement spot.

``cfb_pbp`` passes its school-name-aware parsers, ``nfl_pbp`` the
``"PENALTY on CLV-S.Williams"`` / ``"RECOVERED by ARZ-W.Johnson"`` grammar.
"""

from __future__ import annotations

import re

import polars as pl

__all__ = [
    "add_attribution_cols",
    "add_penalty_enforcement_cols",
    "refine_play_types_post_attribution",
]

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
        # ESPN's NFL foul labels (``penalty.type.text``) that are defensive by rule
        "Illegal Contact",
        "Defensive Too Many Men on Field",
        "Defensive Delay of Game",
        "Leverage",
        "Leaping",
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
        # ESPN's NFL foul labels that are offensive by rule
        "Offensive Too Many Men on Field",
        "Illegal Touch Pass",
        "Illegal Forward Pass",
        "Ineligible Downfield Pass",
        "Offensive Offside",
        "Illegal Touch Kick",
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


#: A field-position token in the 2025+ vendor play text: a school abbreviation of
#: up to two words in any case ("SDSU", "Sac St", "NC ST", "Mizzou", "Wake F",
#: "BC."), an optional dot/space, then the 1-2 digit yardline. The abbreviation is
#: optional so a bare "50" (midfield) still yields the yardline.


def add_penalty_enforcement_cols(play_df: pl.DataFrame) -> pl.DataFrame:
    return (
        play_df.with_columns(
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
    )


def add_attribution_cols(
    play_df: pl.DataFrame,
    *,
    parse_recovery_abbrevs,
    parse_penalty_team_side,
    parse_penalty_spot,
    trust_text_side: bool = False,
    max_penalty_yards: int | None = None,
) -> pl.DataFrame:
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
                parse_recovery_abbrevs,
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
            ).map_elements(parse_penalty_team_side, return_dtype=pl.Utf8),
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
            ).map_elements(parse_penalty_spot, return_dtype=pl.Utf8),
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
            | (pl.col("text").str.contains(r"(?i)punt") & pl.col("text").str.contains(r"(?i)return|muff|fair catch")),
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
                & ((pl.col("sp") == True) | (pl.col("_is_punt_return") == True) | (pl.col("_is_kick_return") == True)),
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
            #      parse_penalty_team_side) -- resolves 98.6% of 2025 rows
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
                > (
                    # college: no foul is longer than 15 yards (25 pre-2014 with the old
                    # spot-foul PI), so anything larger is a mis-parse. The NFL's
                    # defensive pass interference is a spot foul (34, 61 yards happen),
                    # so the NFL processor lifts the cap.
                    pl.lit(max_penalty_yards)
                    if max_penalty_yards is not None
                    else pl.when(pl.col("season") >= 2014).then(pl.lit(25)).otherwise(pl.lit(50))
                ),
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
            # A feed that names the penalized team explicitly ("PENALTY on
            # CLV-S.Williams") needs no second witness: the text-resolved side
            # stands alone when neither the replay nor the foul label speaks.
            .when(
                pl.lit(trust_text_side)
                .and_(pl.col("_pen_L3").is_not_null())
                .and_(pl.col("_pen_L1").is_null())
                .and_(pl.col("_pen_L2").is_null()),
            )
            .then(pl.col("_pen_L3"))
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


def refine_play_types_post_attribution(play_df: pl.DataFrame, *, normalplay, end_change_vec) -> pl.DataFrame:
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

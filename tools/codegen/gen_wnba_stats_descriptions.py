"""Descriptions for the stats.wnba.com loader return tables.

These are the league's own result-set columns, so the vocabulary is the standard
stats API one. Two things are NOT assumed and are derived from published data:

  * column SHAPE (count vs rate vs rank), profiled from the 2024 asset;
  * RANK DIRECTION, which is *not* uniform. Verified empirically: pts_rank 1 is
    the highest points total, but tov_rank 1 is the MOST turnovers (i.e. worst),
    while def_rating_rank 1 and pf_rank 1 are the lowest values. Writing
    "1 = best" everywhere would have been wrong for the negative stats, so each
    rank column states the direction measured for that specific column.

Anything unmatched is reported and left blank rather than invented.
"""

from __future__ import annotations

import pathlib
import re
import sys

sys.path.insert(0, ".")

BASE: dict[str, str] = {
    "gp": "games played",
    "w": "games won",
    "l": "games lost",
    "w_pct": "win percentage",
    "min": "minutes played",
    "pts": "total points scored",
    "ast": "assists credited",
    "reb": "total rebounds collected",
    "oreb": "offensive rebounds collected",
    "dreb": "defensive rebounds collected",
    "stl": "steals recorded",
    "blk": "total shots blocked",
    "blka": "blocked attempts -- the player's own shots that were blocked",
    "tov": "turnovers committed",
    "pf": "personal fouls committed",
    "pfd": "personal fouls drawn",
    "fgm": "field goals made",
    "fga": "field goals attempted",
    "fg_pct": "field-goal percentage",
    "fg3m": "three-point field goals made",
    "fg3a": "three-point field goals attempted",
    "fg3_pct": "three-point percentage",
    "ftm": "free throws made",
    "fta": "free throws attempted",
    "ft_pct": "free-throw percentage",
    "plus_minus": "plus-minus point differential",
    "dd2": "double-doubles",
    "td3": "triple-doubles",
    "nba_fantasy_pts": "fantasy points",
    "fantasy_pts": "fantasy points",
    # --- advanced ---
    "off_rating": "offensive rating -- points produced per 100 possessions",
    "def_rating": "defensive rating -- points allowed per 100 possessions",
    "net_rating": "net rating -- offensive minus defensive rating",
    "ast_pct": "assist percentage -- share of teammate field goals assisted while on court",
    "ast_to": "assist-to-turnover ratio",
    "ast_ratio": "assist ratio -- assists per 100 possessions used",
    "oreb_pct": "offensive rebound percentage",
    "dreb_pct": "defensive rebound percentage",
    "reb_pct": "total rebound percentage",
    "tm_tov_pct": "team turnover percentage -- turnovers per 100 possessions",
    "efg_pct": "effective field-goal percentage, weighting three-pointers 1.5x",
    "ts_pct": "true shooting percentage, accounting for threes and free throws",
    "usg_pct": "usage percentage -- share of team possessions used while on court",
    "pace": "pace -- possessions per 48 minutes",
    "pie": "player impact estimate -- share of game events contributed",
    "poss": "possessions used",
    "def_ws": "defensive win shares",
    "def_ws_raw": "defensive win shares before rounding",
}

STANDINGS: dict[str, str] = {
    "conference_games_back": "Games behind the conference leader.",
    "division_games_back": "Games behind the division leader.",
    "league_games_back": "Games behind the league leader.",
    "division_rank": "Rank within the division.",
    "league_rank": "Rank within the league.",
    "division_record": "Record against divisional opponents.",
    "current_streak": "Current winning or losing streak, signed.",
    "current_home_streak": "Current streak in home games.",
    "current_road_streak": "Current streak in road games.",
    "long_win_streak": "Longest winning streak of the season.",
    "long_loss_streak": "Longest losing streak of the season.",
    "long_home_streak": "Longest home winning streak of the season.",
    "long_road_streak": "Longest road winning streak of the season.",
    "last10home": "Record over the team's last 10 home games.",
    "last10road": "Record over the team's last 10 road games.",
    "diff_total_points": "Season point differential -- points scored minus points allowed.",
    "ahead_at_half": "Record in games the team led at halftime.",
    "behind_at_half": "Record in games the team trailed at halftime.",
    "ahead_at_third": "Record in games the team led after the third period.",
    "behind_at_third": "Record in games the team trailed after the third period.",
    "fewer_turnovers": "Record in games the team committed fewer turnovers than its opponent.",
    "lead_in_fgpct": "Record in games the team shot a higher field-goal percentage than its opponent.",
    "lead_in_reb": "Record in games the team out-rebounded its opponent.",
    "opp_over500": "Record against opponents with a winning record.",
    "opp_score100pts": "Record in games the opponent reached 100 points.",
    "opp_score_80_plus": "Record in games the opponent scored 80 or more.",
    "opp_score_below_80": "Record in games the opponent scored fewer than 80.",
    "score_100pts": "Record in games the team reached 100 points.",
    "score_80_plus": "Record in games the team scored 80 or more.",
    "score_below_80": "Record in games the team scored fewer than 80.",
    "clinched_conference_title": "Flag for having clinched the conference title.",
    "clinched_division_title": "Flag for having clinched the division title.",
    "clinched_playoff_birth": "Flag for having clinched a playoff berth.",
    "clinched_play_in": "Flag for having clinched a play-in berth.",
    "clinched_post_season": "Flag for having clinched a postseason berth.",
    "eliminated_conference": "Flag for elimination from conference contention.",
    "eliminated_division": "Flag for elimination from division contention.",
}
MONTHS = {
    "jan": "January",
    "feb": "February",
    "mar": "March",
    "apr": "April",
    "may": "May",
    "jun": "June",
    "jul": "July",
    "aug": "August",
    "sep": "September",
    "oct": "October",
    "nov": "November",
    "dec": "December",
}

MISC: dict[str, str] = {
    "measure_type": "Which stats.wnba.com measure set the row came from (e.g. Base, Advanced).",
    "how_acquired": "How the team acquired the player (draft, trade, free agency).",
    "season_2": "Season label in the league's second display form.",
    "team_id_lookup": "Team id used to join the row back to the team tables.",
    "stat_description": "Human-readable description of the statistic the row reports.",
    "act_type": "Action-type code for the play-by-play event.",
    "msg_type": "Message-type code for the play-by-play event.",
    "number_event": "Sequence number of the event within the game.",
    "off_slug_team": "Slug of the team on offense for the event.",
    "slug_team": "Slug of the team credited with the event.",
    "team_home": "Slug of the home team.",
    "team_away": "Slug of the away team.",
    "secs_passed_game": "Seconds elapsed in the game at the event.",
    "shot_pts": "Points scored on the shot, if the event was a made field goal.",
    "total_starters_away": "Number of the away team's starters on the floor for the event.",
    "total_starters_home": "Number of the home team's starters on the floor for the event.",
}


def _phrase(stat: str) -> str | None:
    """Resolve a (possibly modified) stat token to a noun phrase."""
    opp = stat.startswith("opp_")
    if opp:
        stat = stat[4:]
    est = stat.startswith("e_")
    if est:
        stat = stat[2:]
    pg = stat.endswith("_pg")
    if pg:
        stat = stat[:-3]
    base = BASE.get(stat)
    if base is None:
        return None
    out = base
    if pg:
        out += " per game"
    if est:
        out = f"estimated {out}"
    if opp:
        out = f"opponent {out}"
    return out


def build(direction: dict[str, str]):
    def describe(col: str) -> str | None:
        if col in MISC:
            return MISC[col]
        if col in STANDINGS:
            return STANDINGS[col]
        if col in MONTHS:
            return f"Team's win-loss record in {MONTHS[col]}."
        m = re.fullmatch(r"(.+)_rank", col)
        if m:
            p = _phrase(m.group(1))
            if p:
                d = direction.get(col)
                tail = f" Rank 1 is the {d} value." if d else ""
                return f"League rank for {p}.{tail}"
            return None
        p = _phrase(col)
        return f"{p[0].upper()}{p[1:]}." if p else None

    return describe


def _measure_directions(frames) -> dict[str, str]:
    """Per-rank-column direction, measured rather than assumed.

    stats.wnba.com does not rank every statistic the same way -- pts_rank 1 is the
    highest total while pf_rank 1 is the lowest -- so the direction is read off the
    data by correlating each value column with its rank.
    """
    import polars as pl

    out: dict[str, str] = {}
    for d in frames:
        for rk in [c for c in d.columns if c.endswith("_rank")]:
            val = rk[: -len("_rank")]
            if val not in d.columns:
                continue
            s = d.select([val, rk]).drop_nulls()
            if s.height < 20:
                continue
            try:
                corr = pl.DataFrame(s).select(pl.corr(val, rk)).item()
            except Exception:
                continue
            if corr is None:
                continue
            out[rk] = "highest" if corr < 0 else "lowest"
    return out


def main() -> None:
    import yaml

    import sportsdataverse.wnba as w

    # ONLY loaders declared in releases.yaml render a column/description table.
    # The four big wnba_stats season/standings/lineups loaders are hand-written and
    # document a PROSE Returns paragraph on the league additional page, so authoring
    # descriptions for them would be dead config -- excluded deliberately.
    targets = [
        "load_wnba_stats_pbp",
        "load_wnba_stats_rosters",
        "load_wnba_stats_player_game_logs",
        "load_wnba_team_season_stats",
    ]
    frames = []
    for fn in ("load_wnba_stats_player_season_stats", "load_wnba_stats_team_season_stats"):
        try:
            frames.append(getattr(w, fn)([2024]))
        except Exception as exc:  # noqa: BLE001
            print(f"   (direction probe skipped for {fn}: {type(exc).__name__})")
    direction = _measure_directions(frames)
    print(f"measured rank direction for {len(direction)} columns")
    describe = build(direction)

    schemas = yaml.safe_load(pathlib.Path("tools/codegen/schemas/loader_schemas.yaml").read_text(encoding="utf-8"))
    out, unparsed = {}, []
    for t in targets:
        got = {}
        for c in schemas.get(t, []):
            d = describe(c["name"])
            if d:
                got[c["name"]] = d
            else:
                unparsed.append(f"{t}.{c['name']}")
        if got:
            out[t] = got
    print(f"composed {sum(len(v) for v in out.values())}; unparsed {len(unparsed)}")
    seen = sorted({u.split(".", 1)[1] for u in unparsed})
    for u in seen[:45]:
        print(f"   {u}")
    with open("_wnba_descs.yaml", "w", encoding="utf-8") as fh:
        yaml.safe_dump(
            {k: dict(sorted(v.items())) for k, v in out.items()}, fh, sort_keys=True, allow_unicode=True, width=120
        )
    print("wrote _wnba_descs.yaml")


if __name__ == "__main__":
    main()

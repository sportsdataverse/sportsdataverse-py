---
title: Changelog
---

# Changelog

The newest releases. Changes merged since 0.1.4 are under [Unreleased](/changelog-unreleased); older releases are in the [archive](/changelog-archive).

## 0.1.4 Release: September 1, 2026

### Fixed — CFB EP/WP inputs: mirrored end yardlines, the wrong `wp_after` perspective, and a flipped WP (#408, #411, #413)

ESPN's `end.yardsToEndzone` arrives mirrored on a class of plays (a `-1` missing
marker, kick returns resolved against the wrong possession team, an all-zero end
state); it reached the EP model as-is, so `EPA` for those rows measured the
wrong field position. The repairs now run before the model: the mirrored end
yardline is corrected, a return spot is resolved against the **end** possession
team, `EP_between` no longer folds across a score, a penalty with no effect has
no penalty EPA, and `yds_sacked` is right for 2004-07 text. `wp_after` was
computed from the wrong possession perspective on **24,332** home/away rows,
and it flipped when the next play kept the same possession (**867** rows).
One live 2026 game exposed three more: a Timeout row lent its phantom EP to the
next play's lag, an all-zero end state now reads as 99, and the last row of a
game in progress no longer renders 0.0% (filled as the complement of the
end-state WP). Plays ESPN inserts late (2014+ feed) are moved back to where
their sequence says they belong before any lag runs.

### Added — the penalty's own side, net yardage and EPA, plus first-down provenance (#408)

The penalty parsers are era-aware (2004-13 / 2014-24 / the 2025 vendor
template). New play-by-play columns: `penalty_side`, `penalty_yards_net`
(signed, only where two of three signals agree), `EPA_penalty_direct` and
`EP_penalty_cf` (the counterfactual EP had the penalty not occurred),
`penalty_enforcement`, `penalty_spot_side` / `penalty_spot_yardline` /
`penalty_spot_yardsToEndzone` / `penalty_cf_yardsToEndzone`, `penalty_team_id`,
`penalty_count`, `penalty_declined_count`, `penalty_all_declined`,
`penalty_negated_play`; first-down provenance `first_down_earned`,
`first_down_yards`, `first_down_penalty`, `firstD_by_yards`, `firstD_by_penalty`,
`firstD_by_poss`, `firstD_by_kickoff`, `new_series`; and the extra-point trio
`xp_attempt`, `xp_made`, `xp_kicker_player_name`. `espn_cfb_pbp` grows from
476 to 501 columns.

### Added — air yards, aDOT and YAC in the passer and receiver box scores (#414)

`create_box_score` emits `AirYds`, `aDOT`, `CompAirYds`, `YAC` and `AirYdsPct`
on the `pass` and `receiver` sections, aggregated from the play-level
`air_yards` / `yards_after_catch`. Null (not zero) for a passer or receiver
whose plays carry no catch spot, which is every season before 2025: ESPN's
play text only started stating the catch point (`caught at SAC18`, `thrown to
LIN30`) with the vendor template that rolled out in 2025 week 9.

### Fixed — air yards sided by the game's own text abbreviations (#418)

That vendor text spots the catch with each school's **own** abbreviation --
`UHM`, `UH`, `USC` for South Carolina, `GSU`, `GSO`, `OSU` for Oregon State,
`Sac St`, `BC.` -- which is frequently not ESPN's `homeTeamAbbrev` /
`awayTeamAbbrev` (`HAW`, `HOU`, `SC`, `GAST`, `GASO`, `ORST`). The derivation
only matched ESPN's abbreviation, so a mismatched team lost every one of its
plays: in 2025's new-template games 25.6% of spot-phrase pass plays (6,131 of
23,957) came out null, one whole side of the field in 103 of 411 games.
The side is now learned from the game itself -- every `... to the ABC nn` end
spot (the last one on a multi-spot play) is compared with ESPN's numeric
`end.yardsToEndzone` and votes for `ABC` being the possessing or defending
team; the per-abbreviation majority wins (>= 2 votes, >= 60%), ESPN's
abbreviation is the fallback, a spot at the 50 needs none, and one regex covers
every observed token shape (`UA 10`, `BC.41`, `Sac St10`, `NC ST19`). In the
published 2025 assets in-game coverage rose from 72.4% to 89.8% of pass plays
(100% of spot-phrase plays; 2026: 91.8%). cfbfastR carries the same logic
(cfbfastR#146).

### Fixed — returning production had no roster since #399 (#417, #419)

`load_cfb_rosters` was repointed at the ESPN `espn_cfb_rosters` release in
0.1.1 (#399); `cfb_returning_production._roster_keys` still keyed on the CFBD
roster's `team` name, so every call raised `ColumnNotFoundError`. It now takes
`team_id` directly from the ESPN roster -- and, because that roster is built
from **game** rosters (week 1 of 2026: 4 teams, 476 rows, every player looked
departed), unions it with the CFBD preseason roster resolved through
`team_info`. 2026 returning production: 236 team rows, offense / defense
0.452 / 0.463 (was 0.006).

### Fixed — codegen let caller params reach ESPN, and un-truncated the endpoints that were silently short (#409)

### Changed — CI dispatches the Game on Paper deploy on every push to `main`; docs site social metadata completed (#410, #412)

### Data

Every cfbfastR-cfb-raw final (2004-2026) was rebuilt on this code and every
`espn_cfb_*` season republished to sportsdataverse-data on 2026-09-01; the
finals' `processing_version` now carries the sdv-py commit (`0.1.3+9efee9f1.3`)
so a lock bump can no longer leave a stale final looking current.

## 0.1.3 Release: August 28, 2026

### Fixed — formation tags reached the box score as player names (#407)

Game 401896383 listed **`No Huddle-Shotgun #1 C.Parker`** in the passing box
score as a third quarterback, alongside that same player's real line.

ESPN prefixes play text with the formation:

```text
No Huddle-Shotgun #1 C.Parker pass complete short right to #9 J.Triplett...
```

The player-name captures are windowed -- `(.{0,30} )pass` (with a trailing space) -- and read the raw
`text`. `No Huddle-Shotgun #1 C.Parker` is 29 characters, so it fits inside the
window and is swallowed whole; a longer prefix is captured *truncated*, starting
mid-token (`dle-Shotgun #5 R.Marshall`). A `cleaned_text` column already stripped
exactly these tokens for the verb-anchored parsers, but the name captures never
used it.

This surfaces only through the regex **fallback**: where ESPN supplies a
participant, the join overwrites the name outright. It bites where ESPN does
not, and that is not rare -- the participants feed for this game carried a null
passer on **96 of its 210 plays**, while resolving the receiver on the very play
that broke.

The cleanup runs at the single point where all 19 `*_player_name` columns are
finalized, so extractors that bypass `_extract_player_name` -- `receiver_player`
from `to (.+)`, the Passing Touchdown passer from `pass from(.+)` -- are covered
too. The formation pattern consumes through the *last* formation token, which
handles a partial leading fragment as well as an intact prefix; no real surname
contains "huddle" or "shotgun".

Known residual: the fallback yields the abbreviated `C.Parker` where the
participants path yields `Carson Parker`, so such a play still forms its own
box-score row. Merging them needs first-initial+surname resolution against the
game roster with a uniqueness guard (`_norm_player_name` strips punctuation, so
`cparker` cannot match `carson parker` today) and is deliberately left out of a
parsing fix.

## 0.1.2 Release: August 27, 2026

### Fixed — passers vanished from the CFB advanced box score (#405)

`create_box_score` returned an empty `advBoxScore["pass"]` for **every** game
whenever `join_participants=True`, while `rush` and `receiver` filled in
normally. There cannot be receptions without passes, so the play data was fine
and the aggregation was not. Game on Paper renders that section directly, so
every box score on the site showed no passers.

`athlete_name` is derived during QBR feature setup, which runs *before* the
participants join rewrites `passer_player_name` / `rusher_player_name` with
cleaned names. It therefore kept the raw participant text while the passer list
built later held the cleaned name -- all 119 rows of the sample game
disagreed:

| `athlete_name` (stale) | `passer_player_name` (cleaned) |
| --- | --- |
| `No Huddle-Shotgun #2 E.Buehler` | `Eddie Buehler` |
| `dle-Shotgun #5 R.Marshall` | `Rashawn Marshall` |

`athlete_name.is_in(qbs_list)` then matched nothing, the QBR frame came back
empty, and the **inner** join onto it deleted every passer.

Two changes, one for the cause and one for the blast radius:

- `athlete_name` is recomputed inside `create_box_score` from the current
  passer/rusher columns, so it cannot go stale behind a later rewrite however
  the pipeline order evolves.
- The QBR join is now a **left** join. QBR is an enrichment; an inner join lets
  any failure to score it delete the whole passing box score rather than leaving
  one column null. That fragility is what turned a single stale column into an
  empty table on every game.

Only reproduces with `join_participants=True`. The offline fixtures use `False`,
which is why the suite stayed green -- the added regression test is a live test
asserting that passers exist wherever receivers do, and that no raw participant
text (a `#`) leaks into a name.

Verified across three games: 401866532 0 -> 2 passers, 401867894 0 -> 3, and
401752921 0 -> 2 (Sayin 26/19/233/3TD, QBR 88.5). Rush and receiver counts are
unchanged.

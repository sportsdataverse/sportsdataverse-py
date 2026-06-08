---
title: NHL — NHL Records API
sidebar_label: NHL Records API
sidebar_position: 13
---
# NHL — NHL Records API

`sportsdataverse.nhl` — 44 endpoints.

## `nhl_records_awards`

List all NHL award / trophy records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/award-details`

**Valid URL:** [https://records.nhl.com/site/api/award-details](https://records.nhl.com/site/api/award-details)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

List all NHL award / trophy records.

### Example

```python
nhl_records_awards()
```

_Last validated n/a._

## `nhl_records_awards_by_franchise`

List award records for a single franchise.

**Endpoint URL:** `GET https://records.nhl.com/site/api/award-details/{franchise_id}`

**Valid URL:** [https://records.nhl.com/site/api/award-details/1](https://records.nhl.com/site/api/award-details/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns

List award records for a single franchise.

### Example

```python
nhl_records_awards_by_franchise(franchise_id=1)
```

_Last validated n/a._

## `nhl_records_awards_trophy_season`

Retrieve the trophy winner for a specific season.

**Endpoint URL:** `GET https://records.nhl.com/site/api/award-details/trophy/{trophy_id}/season/{season_id}`

**Valid URL:** [https://records.nhl.com/site/api/award-details/trophy/1/season/X](https://records.nhl.com/site/api/award-details/trophy/1/season/X)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `trophy_id` | `trophy_id` |  | `Y` |  | trophy_id path parameter. |
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |

### Returns

Retrieve the trophy winner for a specific season.

### Example

```python
nhl_records_awards_trophy_season(trophy_id=1, season_id='X')
```

_Last validated n/a._

## `nhl_records_coaches`

List NHL head coaches.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach`

**Valid URL:** [https://records.nhl.com/site/api/coach](https://records.nhl.com/site/api/coach)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `bio` | character |  |
| `birth_city` | character |  |
| `birth_country3code` | character |  |
| `birth_date` | character |  |
| `birth_state_province_code` | character |  |
| `brief_description` | character |  |
| `date_of_death` | character |  |
| `deceased` | logical |  |
| `description` | character |  |
| `featured_image` | character |  |
| `first_name` | character |  |
| `full_name` | character |  |
| `history` | character |  |
| `hockey_hof_link` | character |  |
| `in_hockey_hof` | logical |  |
| `in_iihf_hockey_hof` | logical |  |
| `in_us_hockey_hof` | logical |  |
| `instagram` | character |  |
| `is_active` | logical |  |
| `last_name` | character |  |
| `nationality_code` | character |  |
| `player_id` | double |  |
| `stanley_cup` | double |  |
| `team_id` | character |  |
| `top100_player_link` | character |  |
| `twitter` | character |  |

### Example

```python
nhl_records_coaches()
```

_Last validated n/a._

## `nhl_records_coach`

Retrieve one coach by their numeric ID.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach/{coach_id}`

**Valid URL:** [https://records.nhl.com/site/api/coach/X](https://records.nhl.com/site/api/coach/X)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns

Retrieve one coach by their numeric ID.

### Example

```python
nhl_records_coach(coach_id='X')
```

_Last validated n/a._

## `nhl_records_coach_career`

Coach career-records (regular season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach-career-records/{coach_id}`

**Valid URL:** [https://records.nhl.com/site/api/coach-career-records](https://records.nhl.com/site/api/coach-career-records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  |  | `Y` | coach_id path parameter. |

### Returns

Coach career-records (regular season).

### Example

```python
nhl_records_coach_career()
```

_Last validated n/a._

## `nhl_records_coach_career_with_playoffs`

Coach career records inclusive of regular season + playoffs.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach-career-records-regular-plus-playoffs`

**Valid URL:** [https://records.nhl.com/site/api/coach-career-records-regular-plus-playoffs](https://records.nhl.com/site/api/coach-career-records-regular-plus-playoffs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Coach career records inclusive of regular season + playoffs.

### Example

```python
nhl_records_coach_career_with_playoffs()
```

_Last validated n/a._

## `nhl_records_coach_franchise`

Coach records scoped to individual franchise stints.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach-franchise-records/{coach_id}`

**Valid URL:** [https://records.nhl.com/site/api/coach-franchise-records](https://records.nhl.com/site/api/coach-franchise-records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  |  | `Y` | coach_id path parameter. |

### Returns

Coach records scoped to individual franchise stints.

### Example

```python
nhl_records_coach_franchise()
```

_Last validated n/a._

## `nhl_records_coach_stanley_cup`

Coach Stanley Cup Final win streak and consecutive-cup records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach-stanley-cup-streak`

**Valid URL:** [https://records.nhl.com/site/api/coach-stanley-cup-streak](https://records.nhl.com/site/api/coach-stanley-cup-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Coach Stanley Cup Final win streak and consecutive-cup records.

### Example

```python
nhl_records_coach_stanley_cup()
```

_Last validated n/a._

## `nhl_records_franchises`

List all NHL franchises (historical and active).

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise`

**Valid URL:** [https://records.nhl.com/site/api/franchise](https://records.nhl.com/site/api/franchise)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `first_season_id` | integer |  |
| `full_name` | character |  |
| `last_season_id` | double |  |
| `most_recent_team_id` | integer |  |
| `team_abbrev` | character |  |
| `team_common_name` | character |  |
| `team_place_name` | character |  |

### Example

```python
nhl_records_franchises()
```

_Last validated n/a._

## `nhl_records_franchise_detail`

Franchise detail records (extended metadata per franchise).

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-detail`

**Valid URL:** [https://records.nhl.com/site/api/franchise-detail](https://records.nhl.com/site/api/franchise-detail)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Franchise detail records (extended metadata per franchise).

### Example

```python
nhl_records_franchise_detail()
```

_Last validated n/a._

## `nhl_records_franchise_team_totals`

All-time team totals per franchise (regular season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-team-totals`

**Valid URL:** [https://records.nhl.com/site/api/franchise-team-totals](https://records.nhl.com/site/api/franchise-team-totals)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `active_franchise` | integer |  |
| `active_team` | logical |  |
| `cups` | integer |  |
| `first_season_id` | integer |  |
| `franchise_id` | integer |  |
| `game_type_id` | integer |  |
| `game_win_pctg` | double |  |
| `games_played` | double |  |
| `goals_against` | double |  |
| `goals_for` | double |  |
| `home_losses` | double |  |
| `home_overtime_losses` | double |  |
| `home_ties` | double |  |
| `home_wins` | double |  |
| `last_season_id` | double |  |
| `losses` | double |  |
| `overtime_losses` | double |  |
| `penalty_minutes` | double |  |
| `playoff_seasons` | double |  |
| `point_pctg` | double |  |
| `points` | double |  |
| `road_losses` | double |  |
| `road_overtime_losses` | double |  |
| `road_ties` | double |  |
| `road_wins` | double |  |
| `series_losses` | integer |  |
| `series_played` | double |  |
| `series_win_pctg` | double |  |
| `series_wins` | integer |  |
| `shootout_losses` | double |  |
| `shootout_wins` | double |  |
| `shutouts` | double |  |
| `team_id` | integer |  |
| `team_name` | character |  |
| `ties` | double |  |
| `tri_code` | character |  |
| `wins` | double |  |

### Example

```python
nhl_records_franchise_team_totals()
```

_Last validated n/a._

## `nhl_records_franchise_season_results`

Season-by-season results for each franchise.

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-season-results`

**Valid URL:** [https://records.nhl.com/site/api/franchise-season-results](https://records.nhl.com/site/api/franchise-season-results)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Season-by-season results for each franchise.

### Example

```python
nhl_records_franchise_season_results()
```

_Last validated n/a._

## `nhl_records_franchise_playoff_appearances`

Franchise playoff appearance counts and streak information.

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-playoff-appearances`

**Valid URL:** [https://records.nhl.com/site/api/franchise-playoff-appearances](https://records.nhl.com/site/api/franchise-playoff-appearances)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Franchise playoff appearance counts and streak information.

### Example

```python
nhl_records_franchise_playoff_appearances()
```

_Last validated n/a._

## `nhl_records_franchise_totals`

League-wide franchise totals (all-time aggregate per franchise).

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-totals`

**Valid URL:** [https://records.nhl.com/site/api/franchise-totals](https://records.nhl.com/site/api/franchise-totals)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

League-wide franchise totals (all-time aggregate per franchise).

### Example

```python
nhl_records_franchise_totals()
```

_Last validated n/a._

## `nhl_records_all_time_record_vs_franchise`

All-time head-to-head records between every franchise pairing.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-time-record-vs-franchise`

**Valid URL:** [https://records.nhl.com/site/api/all-time-record-vs-franchise](https://records.nhl.com/site/api/all-time-record-vs-franchise)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

All-time head-to-head records between every franchise pairing.

### Example

```python
nhl_records_all_time_record_vs_franchise()
```

_Last validated n/a._

## `nhl_records_skater_career_stats`

Skater career statistics (all-time, regular season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/skater-career-statistics`

**Valid URL:** [https://records.nhl.com/site/api/skater-career-statistics](https://records.nhl.com/site/api/skater-career-statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Skater career statistics (all-time, regular season).

### Example

```python
nhl_records_skater_career_stats()
```

_Last validated n/a._

## `nhl_records_skater_career_leaders`

All-time skater career leaderboards.

**Endpoint URL:** `GET https://records.nhl.com/site/api/skater-career-leaders`

**Valid URL:** [https://records.nhl.com/site/api/skater-career-leaders](https://records.nhl.com/site/api/skater-career-leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

All-time skater career leaderboards.

### Example

```python
nhl_records_skater_career_leaders()
```

_Last validated n/a._

## `nhl_records_consecutive_100pt_seasons`

Skaters with the most consecutive 100-point seasons.

**Endpoint URL:** `GET https://records.nhl.com/site/api/consecutive-100-point-seasons`

**Valid URL:** [https://records.nhl.com/site/api/consecutive-100-point-seasons](https://records.nhl.com/site/api/consecutive-100-point-seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Skaters with the most consecutive 100-point seasons.

### Example

```python
nhl_records_consecutive_100pt_seasons()
```

_Last validated n/a._

## `nhl_records_goalie_career_stats`

Goaltender career statistics (regular season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-career-stats`

**Valid URL:** [https://records.nhl.com/site/api/goalie-career-stats](https://records.nhl.com/site/api/goalie-career-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Goaltender career statistics (regular season).

### Example

```python
nhl_records_goalie_career_stats()
```

_Last validated n/a._

## `nhl_records_goalie_career_stats_with_playoffs`

Goaltender career stats inclusive of regular season and playoffs.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie_career_stats_incl_playoffs`

**Valid URL:** [https://records.nhl.com/site/api/goalie_career_stats_incl_playoffs](https://records.nhl.com/site/api/goalie_career_stats_incl_playoffs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Goaltender career stats inclusive of regular season and playoffs.

### Example

```python
nhl_records_goalie_career_stats_with_playoffs()
```

_Last validated n/a._

## `nhl_records_goalie_season_stats`

Goaltender single-season statistics.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-season-stats`

**Valid URL:** [https://records.nhl.com/site/api/goalie-season-stats](https://records.nhl.com/site/api/goalie-season-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Goaltender single-season statistics.

### Example

```python
nhl_records_goalie_season_stats()
```

_Last validated n/a._

## `nhl_records_goalie_win_streak`

Goaltenders with the longest consecutive-win streaks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-win-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-win-streak](https://records.nhl.com/site/api/goalie-win-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Goaltenders with the longest consecutive-win streaks.

### Example

```python
nhl_records_goalie_win_streak()
```

_Last validated n/a._

## `nhl_records_goalie_shutout_streak`

Goaltenders with the longest consecutive-shutout streaks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-shutout-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-shutout-streak](https://records.nhl.com/site/api/goalie-shutout-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Goaltenders with the longest consecutive-shutout streaks.

### Example

```python
nhl_records_goalie_shutout_streak()
```

_Last validated n/a._

## `nhl_records_goalie_win_plateaus`

Goaltenders who reached each win plateau (100, 200, 300 …).

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-win-plateaus`

**Valid URL:** [https://records.nhl.com/site/api/goalie-win-plateaus](https://records.nhl.com/site/api/goalie-win-plateaus)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Goaltenders who reached each win plateau (100, 200, 300 …).

### Example

```python
nhl_records_goalie_win_plateaus()
```

_Last validated n/a._

## `nhl_records_goalie_playoff_streak`

Goaltender consecutive playoff-win streaks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-playoff-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-playoff-streak](https://records.nhl.com/site/api/goalie-playoff-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Goaltender consecutive playoff-win streaks.

### Example

```python
nhl_records_goalie_playoff_streak()
```

_Last validated n/a._

## `nhl_records_goalie_undefeated_streak`

Goaltender longest undefeated streaks (wins + ties).

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-undefeated-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-undefeated-streak](https://records.nhl.com/site/api/goalie-undefeated-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Goaltender longest undefeated streaks (wins + ties).

### Example

```python
nhl_records_goalie_undefeated_streak()
```

_Last validated n/a._

## `nhl_records_draft`

Retrieve NHL Entry Draft picks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/draft/{draft_id}`

**Valid URL:** [https://records.nhl.com/site/api/draft](https://records.nhl.com/site/api/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `draft_id` | `draft_id` |  |  | `Y` | draft_id path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `age_in_days` | character |  |
| `age_in_days_for_year` | character |  |
| `age_in_years` | character |  |
| `amateur_club_name` | character |  |
| `amateur_league` | character |  |
| `birth_date` | character |  |
| `birth_place` | character |  |
| `country_code` | character |  |
| `cs_player_id` | character |  |
| `draft_date` | character |  |
| `draft_master_id` | integer |  |
| `draft_year` | integer |  |
| `drafted_by_team_id` | character |  |
| `first_name` | character |  |
| `height` | character |  |
| `last_name` | character |  |
| `notes` | character |  |
| `overall_pick_number` | integer |  |
| `pick_in_round` | integer |  |
| `player_id` | character |  |
| `player_name` | character |  |
| `position` | character |  |
| `removed_outright` | character |  |
| `removed_outright_why` | character |  |
| `round_number` | integer |  |
| `shoots_catches` | character |  |
| `supplemental_draft` | character |  |
| `team_pick_history` | character |  |
| `tri_code` | character |  |
| `weight` | character |  |

### Example

```python
nhl_records_draft()
```

_Last validated n/a._

## `nhl_records_draft_by_team`

All draft picks made by a single team.

**Endpoint URL:** `GET https://records.nhl.com/site/api/draft/byTeam/{team_id}`

**Valid URL:** [https://records.nhl.com/site/api/draft/byTeam/10](https://records.nhl.com/site/api/draft/byTeam/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

All draft picks made by a single team.

### Example

```python
nhl_records_draft_by_team(team_id=10)
```

_Last validated n/a._

## `nhl_records_draft_prospect`

Draft prospect records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/draft-prospect/{prospect_id}`

**Valid URL:** [https://records.nhl.com/site/api/draft-prospect](https://records.nhl.com/site/api/draft-prospect)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `prospect_id` | `prospect_id` |  |  | `Y` | prospect_id path parameter. |

### Returns

Draft prospect records.

### Example

```python
nhl_records_draft_prospect()
```

_Last validated n/a._

## `nhl_records_draft_lottery_odds`

Draft lottery odds (current year or filtered by season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/draft-lottery-odds`

**Valid URL:** [https://records.nhl.com/site/api/draft-lottery-odds](https://records.nhl.com/site/api/draft-lottery-odds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Draft lottery odds (current year or filtered by season).

### Example

```python
nhl_records_draft_lottery_odds()
```

_Last validated n/a._

## `nhl_records_expansion_draft_picks`

Expansion draft picks (e.g. Vegas 2017, Seattle 2021).

**Endpoint URL:** `GET https://records.nhl.com/site/api/expansion-draft-picks`

**Valid URL:** [https://records.nhl.com/site/api/expansion-draft-picks](https://records.nhl.com/site/api/expansion-draft-picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Expansion draft picks (e.g. Vegas 2017, Seattle 2021).

### Example

```python
nhl_records_expansion_draft_picks()
```

_Last validated n/a._

## `nhl_records_allstar_skater_career`

All-Star Game career statistics for skaters.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-skater-career-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-skater-career-stats](https://records.nhl.com/site/api/all-star-skater-career-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

All-Star Game career statistics for skaters.

### Example

```python
nhl_records_allstar_skater_career()
```

_Last validated n/a._

## `nhl_records_allstar_goalie_career`

All-Star Game career statistics for goaltenders.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-goaltender-career-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-goaltender-career-stats](https://records.nhl.com/site/api/all-star-goaltender-career-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

All-Star Game career statistics for goaltenders.

### Example

```python
nhl_records_allstar_goalie_career()
```

_Last validated n/a._

## `nhl_records_allstar_coach_career`

All-Star Game career records for coaches.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-coach-career-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-coach-career-stats](https://records.nhl.com/site/api/all-star-coach-career-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

All-Star Game career records for coaches.

### Example

```python
nhl_records_allstar_coach_career()
```

_Last validated n/a._

## `nhl_records_allstar_skater_game`

All-Star Game single-game scoring records for skaters.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-skater-game-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-skater-game-stats](https://records.nhl.com/site/api/all-star-skater-game-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

All-Star Game single-game scoring records for skaters.

### Example

```python
nhl_records_allstar_skater_game()
```

_Last validated n/a._

## `nhl_records_allstar_goalie_game`

All-Star Game single-game stats for goaltenders.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-goaltender-game-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-goaltender-game-stats](https://records.nhl.com/site/api/all-star-goaltender-game-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

All-Star Game single-game stats for goaltenders.

### Example

```python
nhl_records_allstar_goalie_game()
```

_Last validated n/a._

## `nhl_records_attendance`

NHL arena attendance records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/attendance`

**Valid URL:** [https://records.nhl.com/site/api/attendance](https://records.nhl.com/site/api/attendance)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `playoff_attendance` | double |  |
| `regular_attendance` | double |  |
| `season_id` | integer |  |
| `total_attendance` | double |  |

### Example

```python
nhl_records_attendance()
```

_Last validated n/a._

## `nhl_records_hof_players`

Hockey Hall of Fame player inductees.

**Endpoint URL:** `GET https://records.nhl.com/site/api/hof/players`

**Valid URL:** [https://records.nhl.com/site/api/hof/players](https://records.nhl.com/site/api/hof/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Hockey Hall of Fame player inductees.

### Example

```python
nhl_records_hof_players()
```

_Last validated n/a._

## `nhl_records_hof_players_by_office`

Hall of Fame players for a specific induction office/category.

**Endpoint URL:** `GET https://records.nhl.com/site/api/hof/players/{office_id}`

**Valid URL:** [https://records.nhl.com/site/api/hof/players/X](https://records.nhl.com/site/api/hof/players/X)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `office_id` | `office_id` |  | `Y` |  | office_id path parameter. |

### Returns

Hall of Fame players for a specific induction office/category.

### Example

```python
nhl_records_hof_players_by_office(office_id='X')
```

_Last validated n/a._

## `nhl_records_gm_career`

General Manager career records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/general-manager/{gm_id}`

**Valid URL:** [https://records.nhl.com/site/api/general-manager](https://records.nhl.com/site/api/general-manager)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gm_id` | `gm_id` |  |  | `Y` | gm_id path parameter. |

### Returns

General Manager career records.

### Example

```python
nhl_records_gm_career()
```

_Last validated n/a._

## `nhl_records_gm_franchise`

General Manager records scoped to franchise stints.

**Endpoint URL:** `GET https://records.nhl.com/site/api/general-manager-franchise-records`

**Valid URL:** [https://records.nhl.com/site/api/general-manager-franchise-records](https://records.nhl.com/site/api/general-manager-franchise-records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

General Manager records scoped to franchise stints.

### Example

```python
nhl_records_gm_franchise()
```

_Last validated n/a._

## `nhl_records_home_team_record`

League-wide home-team win/loss record by season.

**Endpoint URL:** `GET https://records.nhl.com/site/api/home-team-record`

**Valid URL:** [https://records.nhl.com/site/api/home-team-record](https://records.nhl.com/site/api/home-team-record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

League-wide home-team win/loss record by season.

### Example

```python
nhl_records_home_team_record()
```

_Last validated n/a._

## `nhl_records_away_team_record`

League-wide away-team win/loss record by season.

**Endpoint URL:** `GET https://records.nhl.com/site/api/away-team-record`

**Valid URL:** [https://records.nhl.com/site/api/away-team-record](https://records.nhl.com/site/api/away-team-record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

League-wide away-team win/loss record by season.

### Example

```python
nhl_records_away_team_record()
```

_Last validated n/a._

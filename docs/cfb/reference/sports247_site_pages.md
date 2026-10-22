# CFB — 247Sports Site Pages (247sports.com)

> CFB — 247Sports Site Pages (247sports.com) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.cfb` — 35 endpoints.

## Coach

| Function | Summary |
|---|---|
| [sports247_site_pages_coach_alma_mater](sports247_site_pages/coach.md#sports247_site_pages_coach_alma_mater) | Coach alma-mater Institution. |
| [sports247_site_pages_coach_hometown](sports247_site_pages/coach.md#sports247_site_pages_coach_hometown) | Coach hometown Location. |
| [sports247_site_pages_coach_ranking](sports247_site_pages/coach.md#sports247_site_pages_coach_ranking) | Single CoachRanking row. |
| [sports247_site_pages_coach_rankings](sports247_site_pages/coach.md#sports247_site_pages_coach_rankings) | Coach's recruiting-ranking history (one row per Ranking snapshot). |

## Player

| Function | Summary |
|---|---|
| [sports247_site_pages_player](sports247_site_pages/player.md#sports247_site_pages_player) | Player detail (identity + primary-sport rating/ranks). |
| [sports247_site_pages_player_current_institution](sports247_site_pages/player.md#sports247_site_pages_player_current_institution) | Player's current PlayerInstitution (committed/enrolled school). |
| [sports247_site_pages_player_high_school](sports247_site_pages/player.md#sports247_site_pages_player_high_school) | Player's high-school PlayerInstitution row. |
| [sports247_site_pages_player_institution](sports247_site_pages/player.md#sports247_site_pages_player_institution) | Player-at-institution association detail. |
| [sports247_site_pages_player_institution_evaluation](sports247_site_pages/player.md#sports247_site_pages_player_institution_evaluation) | Scout evaluation of a player-institution fit. |
| [sports247_site_pages_player_primary_sport](sports247_site_pages/player.md#sports247_site_pages_player_primary_sport) | Player's primary PlayerSport (rating/class/positions). |
| [sports247_site_pages_player_search](sports247_site_pages/player.md#sports247_site_pages_player_search) | Player name search. |
| [sports247_site_pages_playersport](sports247_site_pages/player.md#sports247_site_pages_playersport) | PlayerSport detail (note lowercase route segment). |

## Recruitment

| Function | Summary |
|---|---|
| [sports247_site_pages_recruitment_final_choice](sports247_site_pages/recruitment.md#sports247_site_pages_recruitment_final_choice) | Final-choice PlayerSport/commit for a recruitment. |
| [sports247_site_pages_recruitment_institution](sports247_site_pages/recruitment.md#sports247_site_pages_recruitment_institution) | Committed institution for a recruitment. |
| [sports247_site_pages_recruitment_interests](sports247_site_pages/recruitment.md#sports247_site_pages_recruitment_interests) | All institutions the recruit has interest links with. |
| [sports247_site_pages_recruitment_offers](sports247_site_pages/recruitment.md#sports247_site_pages_recruitment_offers) | Institutions that have offered the recruit. |
| [sports247_site_pages_recruitment_player_sport](sports247_site_pages/recruitment.md#sports247_site_pages_recruitment_player_sport) | PlayerSport underlying a recruitment. |

## Season

| Function | Summary |
|---|---|
| [sports247_site_pages_season_current_expert_predictions](sports247_site_pages/season.md#sports247_site_pages_season_current_expert_predictions) | Current expert 'crystal ball' predictions for a season. |
| [sports247_site_pages_season_recruit_interest_events](sports247_site_pages/season.md#sports247_site_pages_season_recruit_interest_events) | Recruit-interest timeline events for a season (offers/visits/commits). |
| [sports247_site_pages_season_recruit_interests](sports247_site_pages/season.md#sports247_site_pages_season_recruit_interests) | All recruit interests for a season (paginated). |
| [sports247_site_pages_season_recruits](sports247_site_pages/season.md#sports247_site_pages_season_recruits) | Recruit class rankings for a season (rich per-recruit rows with inlined Player). |
| [sports247_site_pages_season_roster_embed](sports247_site_pages/season.md#sports247_site_pages_season_roster_embed) | Signed-class roster embed (PlayerSport rows). Accuracy can lag. |

## Other

| Function | Summary |
|---|---|
| [sports247_site_pages_coach](sports247_site_pages/other.md#sports247_site_pages_coach) | Coach identity detail. |
| [sports247_site_pages_event](sports247_site_pages/other.md#sports247_site_pages_event) | Recruiting event detail (camp/combine/regional). |
| [sports247_site_pages_institution](sports247_site_pages/other.md#sports247_site_pages_institution) | Institution (school/team) detail. |
| [sports247_site_pages_institution_list](sports247_site_pages/other.md#sports247_site_pages_institution_list) | Institution directory (paginated list). |
| [sports247_site_pages_institution_location](sports247_site_pages/other.md#sports247_site_pages_institution_location) | Institution location (city/state/coords/tax). |
| [sports247_site_pages_institution_timeline_events](sports247_site_pages/other.md#sports247_site_pages_institution_timeline_events) | Institution recruiting timeline (site-authored event blurbs). |
| [sports247_site_pages_league_draft_picks](sports247_site_pages/other.md#sports247_site_pages_league_draft_picks) | Pro-draft picks embed for a league/year/round. |
| [sports247_site_pages_league_institutions](sports247_site_pages/other.md#sports247_site_pages_league_institutions) | Institutions belonging to a league. |
| [sports247_site_pages_page_feeds](sports247_site_pages/other.md#sports247_site_pages_page_feeds) | News/headline feed items for a site Page. |
| [sports247_site_pages_playersport_institution](sports247_site_pages/other.md#sports247_site_pages_playersport_institution) | PlayerInstitution linked to a PlayerSport. |
| [sports247_site_pages_playersport_rank_history](sports247_site_pages/other.md#sports247_site_pages_playersport_rank_history) | Ranking history for a PlayerSport (one row per Ranking snapshot). |
| [sports247_site_pages_position_rankings](sports247_site_pages/other.md#sports247_site_pages_position_rankings) | Player-sport rankings for a position. |
| [sports247_site_pages_recruit_interest](sports247_site_pages/other.md#sports247_site_pages_recruit_interest) | Single recruit-interest (school<->recruit link) detail. |

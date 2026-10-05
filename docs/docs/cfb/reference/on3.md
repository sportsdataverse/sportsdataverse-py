---
title: CFB — On3 Recruit Database (api.on3.com)
sidebar_label: On3 Recruit Database (api.on3.com)
description: "CFB — On3 Recruit Database (api.on3.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# CFB — On3 Recruit Database (api.on3.com)

`sportsdataverse.cfb` — 78 endpoints.

## Draft

| Function | Summary |
|---|---|
| [on3_draft_organization_rank](on3/draft.md#on3_draft_organization_rank) | GET /rdb/v1/draft-organization-rank |
| [on3_draft_pick_organization_rank](on3/draft.md#on3_draft_pick_organization_rank) | GET /rdb/v1/draft-pick-organization-rank |
| [on3_drafts](on3/draft.md#on3_drafts) | GET /rdb/v1/drafts |
| [on3_drafts_by_stars](on3/draft.md#on3_drafts_by_stars) | GET /rdb/v1/drafts-by-stars |
| [on3_drafts_by_stars_summary](on3/draft.md#on3_drafts_by_stars_summary) | GET /rdb/v1/drafts-by-stars-summary |
| [on3_drafts_players](on3/draft.md#on3_drafts_players) | GET /rdb/v1/drafts/{orgKey}/players |

## Organizations

| Function | Summary |
|---|---|
| [on3_organizations_draft_class_by_state](on3/organizations.md#on3_organizations_draft_class_by_state) | GET /rdb/v1/organizations/{organizationKey}/draft-class-by-state |
| [on3_organizations_draft_class_by_year](on3/organizations.md#on3_organizations_draft_class_by_year) | GET /rdb/v1/organizations/{organizationKey}/draft-class-by-year |
| [on3_organizations_draft_count_by_stars](on3/organizations.md#on3_organizations_draft_count_by_stars) | GET /rdb/v1/organizations/{organizationKey}/draft-count-by-stars |
| [on3_organizations_draft_count_by_year](on3/organizations.md#on3_organizations_draft_count_by_year) | GET /rdb/v1/organizations/{organizationKey}/draft-count-by-year |
| [on3_organizations_draft_ranking_summary](on3/organizations.md#on3_organizations_draft_ranking_summary) | GET /rdb/v1/organizations/{organizationKey}/draft-ranking-summary |
| [on3_organizations_drafted_players](on3/organizations.md#on3_organizations_drafted_players) | GET /rdb/v1/organizations/{organizationKey}/drafted-players |
| [on3_organizations_drafts_by_stars_summary](on3/organizations.md#on3_organizations_drafts_by_stars_summary) | GET /rdb/v1/organizations/{organizationKey}/drafts-by-stars-summary |
| [on3_organizations_roster](on3/organizations.md#on3_organizations_roster) | GET /rdb/v1/organizations/{organizationKey}/roster |
| [on3_organizations_roster_header](on3/organizations.md#on3_organizations_roster_header) | GET /rdb/v1/organizations/{organizationKey}/roster-header |

## People

| Function | Summary |
|---|---|
| [on3_people_combine_measurements](on3/people.md#on3_people_combine_measurements) | GET /rdb/v1/people/{personKey}/combine-measurements |
| [on3_people_latest_valuation](on3/people.md#on3_people_latest_valuation) | GET /rdb/v1/people/{personKey}/latest-valuation |
| [on3_people_measurements](on3/people.md#on3_people_measurements) | GET /rdb/v1/people/{personKey}/measurements |
| [on3_people_measurements_averages](on3/people.md#on3_people_measurements_averages) | GET /rdb/v1/people/{personKey}/measurements/averages |
| [on3_people_person_connections](on3/people.md#on3_people_person_connections) | GET /rdb/v1/people/{personKey}/person-connections |
| [on3_people_social](on3/people.md#on3_people_social) | GET /rdb/v1/people/{personKey}/social |
| [on3_people_social_post_summary](on3/people.md#on3_people_social_post_summary) | GET /rdb/v1/people/{personKey}/social-post-summary |
| [on3_people_track_and_field_measurements](on3/people.md#on3_people_track_and_field_measurements) | GET /rdb/v1/people/{personKey}/track-and-field-measurements |
| [on3_people_valuation_growth](on3/people.md#on3_people_valuation_growth) | GET /rdb/v1/people/{personKey}/valuation-growth |

## Player

| Function | Summary |
|---|---|
| [on3_player_all_rankings](on3/player.md#on3_player_all_rankings) | GET /rdb/v1/player/{personKey}/all-rankings |
| [on3_player_database_updates](on3/player.md#on3_player_database_updates) | GET /rdb/v1/player/{personKey}/database-updates |
| [on3_player_images](on3/player.md#on3_player_images) | GET /rdb/v1/player/{personKey}/images |
| [on3_player_organizations](on3/player.md#on3_player_organizations) | GET /rdb/v1/player/{personKey}/organizations |
| [on3_player_organizations_org_key](on3/player.md#on3_player_organizations_org_key) | GET /rdb/v1/player/{playerKey}/organizations/{orgKey} |
| [on3_player_person_rankings](on3/player.md#on3_player_person_rankings) | GET /rdb/v1/player/{personKey}/rankings |
| [on3_player_profile](on3/player.md#on3_player_profile) | GET /rdb/v1/player/{personKey}/profile |
| [on3_player_team_targets](on3/player.md#on3_player_team_targets) | GET /rdb/v1/player/{playerKey}/team-targets |
| [on3_player_verified](on3/player.md#on3_player_verified) | GET /rdb/v1/player/verified |
| [on3_player_videos](on3/player.md#on3_player_videos) | GET /rdb/v1/player/{personKey}/videos |
| [on3_player_visit_center](on3/player.md#on3_player_visit_center) | GET /rdb/v1/player/{playerKey}/visit-center |
| [on3_players_industry_comparision](on3/player.md#on3_players_industry_comparision) | GET /rdb/v1/players/industry-comparision |
| [on3_players_industry_comparision_list](on3/player.md#on3_players_industry_comparision_list) | GET /rdb/v1/players/industry-comparision-list |

## Recruitment

| Function | Summary |
|---|---|
| [on3_recruitment_primary_recruitment_evaluation](on3/recruitment.md#on3_recruitment_primary_recruitment_evaluation) | GET /rdb/v1/recruitment/{recruitmentKey}/primary-recruitment-evaluation |
| [on3_recruitment_recruitment_evaluations](on3/recruitment.md#on3_recruitment_recruitment_evaluations) | GET /rdb/v1/recruitment/{recruitmentKey}/recruitment-evaluations |
| [on3_recruitments_latest_rpm_picks](on3/recruitment.md#on3_recruitments_latest_rpm_picks) | Latest RPM (prediction) picks feed — paged {list,pagination} |
| [on3_recruitments_profile](on3/recruitment.md#on3_recruitments_profile) | GET /rdb/v1/recruitments/{recKey}/profile |
| [on3_recruitments_rpm_picks](on3/recruitment.md#on3_recruitments_rpm_picks) | GET /rdb/v1/recruitments/{recKey}/rpm-picks |
| [on3_recruitments_rpm_summary](on3/recruitment.md#on3_recruitments_rpm_summary) | GET /rdb/v1/recruitments/{recKey}/rpm-summary |

## Team

| Function | Summary |
|---|---|
| [on3_team_ranking](on3/team.md#on3_team_ranking) | GET /rdb/v1/team-ranking |
| [on3_team_ranking_bluechips_team_rankings](on3/team.md#on3_team_ranking_bluechips_team_rankings) | GET /rdb/v1/team-ranking/{sport}-{year}/bluechips-team-rankings |
| [on3_team_ranking_consensus_team_rankings](on3/team.md#on3_team_ranking_consensus_team_rankings) | GET /rdb/v1/team-ranking/{sport}-{year}/consensus-team-rankings |
| [on3_team_ranking_organizations_summary](on3/team.md#on3_team_ranking_organizations_summary) | GET /rdb/v1/team-ranking/organizations/{orgKey}/summary |
| [on3_team_ranking_team_rankings](on3/team.md#on3_team_ranking_team_rankings) | GET /rdb/v1/team-ranking/{sport}-{year}/team-rankings |

## Other

| Function | Summary |
|---|---|
| [on3_coaches_history](on3/other.md#on3_coaches_history) | GET /rdb/v1/coaches/{personKey}/history |
| [on3_coaches_profile](on3/other.md#on3_coaches_profile) | GET /rdb/v1/coaches/{personKey}/profile |
| [on3_collective_groups](on3/other.md#on3_collective_groups) | GET /rdb/v1/collective-groups |
| [on3_collective_groups_deals](on3/other.md#on3_collective_groups_deals) | GET /rdb/v1/collective-groups/{key}/deals |
| [on3_collective_groups_key](on3/other.md#on3_collective_groups_key) | GET /rdb/v1/collective-groups/{key} |
| [on3_commits_latest](on3/other.md#on3_commits_latest) | GET /rdb/v1/commits/latest |
| [on3_commits_organizations_latest_commits](on3/other.md#on3_commits_organizations_latest_commits) | GET /rdb/v1/commits/organizations/{orgKey}/latest-commits |
| [on3_commits_organizations_org_key](on3/other.md#on3_commits_organizations_org_key) | GET /rdb/v1/commits/organizations/{orgKey} |
| [on3_filters_conferences](on3/other.md#on3_filters_conferences) | GET /rdb/v1/filters/conferences |
| [on3_filters_draft_rounds](on3/other.md#on3_filters_draft_rounds) | GET /rdb/v1/filters/draft-rounds |
| [on3_filters_positions](on3/other.md#on3_filters_positions) | GET /rdb/v1/filters/positions |
| [on3_filters_sports](on3/other.md#on3_filters_sports) | GET /rdb/v1/filters/sports |
| [on3_filters_status](on3/other.md#on3_filters_status) | GET /rdb/v1/filters/status |
| [on3_filters_teams](on3/other.md#on3_filters_teams) | GET /rdb/v1/filters/teams |
| [on3_filters_years](on3/other.md#on3_filters_years) | GET /rdb/v1/filters/years |
| [on3_nil_100](on3/other.md#on3_nil_100) | GET /rdb/v1/nil-100 |
| [on3_nil_100_v2](on3/other.md#on3_nil_100_v2) | GET /rdb/v2/nil-100 |
| [on3_nil_compliances_state](on3/other.md#on3_nil_compliances_state) | GET /rdb/v1/nil-compliances/state |
| [on3_nil_rankings](on3/other.md#on3_nil_rankings) | GET /rdb/v1/nil-rankings |
| [on3_person_connections_connection_key](on3/other.md#on3_person_connections_connection_key) | GET /rdb/v1/person-connections/{connectionKey} |
| [on3_person_primary_recruitment_evaluation](on3/other.md#on3_person_primary_recruitment_evaluation) | GET /rdb/v1/person/{personKey}/primary-recruitment-evaluation |
| [on3_person_recruitment_evaluations](on3/other.md#on3_person_recruitment_evaluations) | GET /rdb/v1/person/{personKey}/recruitment-evaluations |
| [on3_person_sport_profile_recruit](on3/other.md#on3_person_sport_profile_recruit) | GET /rdb/v1/person-sport/{psKey}/profile-recruit |
| [on3_person_sport_rankings](on3/other.md#on3_person_sport_rankings) | GET /rdb/v1/person-sport-rankings |
| [on3_predictions_user_key](on3/other.md#on3_predictions_user_key) | Expert prediction accuracy + feed (see PredictionAccuracies) |
| [on3_quotes](on3/other.md#on3_quotes) | GET /rdb/v1/quotes |
| [on3_quotes_key](on3/other.md#on3_quotes_key) | GET /rdb/v1/quotes/{key} |
| [on3_transfers_best_available](on3/other.md#on3_transfers_best_available) | GET /rdb/v1/transfers/best-available |
| [on3_transfers_latest](on3/other.md#on3_transfers_latest) | GET /rdb/v1/transfers/latest |
| [on3_videos_video_key](on3/other.md#on3_videos_video_key) | GET /rdb/v1/videos/{videoKey} |

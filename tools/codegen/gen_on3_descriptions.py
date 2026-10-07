"""Compose returns-table descriptions for the On3 Recruit Database (RDB) tables.

``parse_on3_rdb`` json_normalize-flattens every nested object, so one RDB column name is a
CHAIN of entity segments followed by a leaf field: ``person_high_school_default_asset_height``
is person -> high school -> default asset -> height. The 2,400-odd columns are therefore a
few hundred leaves repeated under a few dozen owners, and a description is composed from the
two -- the leaf template says what the field is, the chain says whose it is. Every leaf
template and owner phrase below was read off the vendored captures in ``tests/fixtures/on3``
(``on3_player_rankings`` / ``on3_team_rankings`` carry the hand-authored originals these
phrases follow). A leaf with no template is left blank, never guessed.

Run:  uv run python tools/codegen/gen_on3_descriptions.py > <out.yaml>
Then: uv run python tools/codegen/merge_column_descriptions.py <out.yaml>
"""

from __future__ import annotations

import glob
import os
import re
import sys

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCHEMA_DIR = os.path.join(ROOT, "tools", "codegen", "schemas", "native", "on3")

# Entity segments, longest first so ``committed_organization`` wins over ``organization``.
# Value: (noun phrase, kind) -- kind picks the asset wording ("logo" vs "headshot").
_ENTITIES: dict[str, tuple[str, str]] = {
    "pick_expert_profile_picture_response": ("expert's profile-picture asset", "asset"),
    "committed_status_committed_asset_res": ("committed-to program's logo asset", "asset"),
    "committed_status_committed_asset": ("committed-to program", "org"),
    "committed_status": ("commitment status", "status"),
    "committed_organization": ("committed-to program", "org"),
    "committed_article": ("commitment article", "article"),
    "entered_article": ("portal-entry article", "article"),
    "exited_article": ("portal-exit article", "article"),
    "transfer_industry_comparison": ("transfer industry comparison", "json"),
    "industry_comparison": ("industry comparison", "json"),
    "organization_history": ("organization history", "json"),
    "default_sport": ("primary sport", "sport"),
    "default_asset": ("default asset", "asset"),
    "social_asset": ("social-media asset", "asset"),
    "profile_picture": ("profile picture", "asset"),
    "pick_expert": ("pick's expert", "expert"),
    "head_coach": ("head coach", "person"),
    "high_school": ("high school", "org"),
    "hometown_state": ("home-town state", "state"),
    "current_state": ("current state", "state"),
    "primary_position": ("primary position", "position"),
    "transferred_from_organization": ("program transferred from", "org"),
    "transferred_asset": ("program transferred to", "org"),
    "committed_asset": ("committed-to program", "org"),
    "commit_status": ("commitment status", "status"),
    "interest_status": ("interest status", "status"),
    "roster_rating": ("On3 roster rating", "rating"),
    "transfer_rating": ("On3 transfer-portal rating", "rating"),
    "college_team": ("college program", "org"),
    "last_team": ("previous program", "org"),
    "organization": ("program", "org"),
    "nil_value": ("NIL valuation", "valuation"),
    "valuation": ("NIL valuation", "valuation"),
    "rating": ("On3 rating", "rating"),
    "ranking": ("ranking", "rating"),
    "consensus": ("industry-consensus", "rating"),
    "status": ("recruiting status", "status"),
    "person": ("person", "person"),
    "player": ("player", "person"),
    "pick": ("RPM pick", "pick"),
    "asset_url": ("logo link", "assetlink"),
    "asset": ("logo asset", "asset"),
    "res": ("logo asset", "asset"),
    "sport": ("sport", "sport"),
    "position": ("position", "position"),
    "hometown": ("home town", "hometown"),
}
_ENTITY_RE = re.compile("^(" + "|".join(sorted(_ENTITIES, key=len, reverse=True)) + ")_")

# Leaf templates per entity kind; ``{o}`` is the owner phrase (e.g. "the recruit's high school").
_ASSET = {
    "key": "On3 asset key of {o}.",
    "url": "Full CDN URL of {o}.",
    "domain": "CDN domain serving {o}.",
    "domain_override": "CDN domain override for {o} (usually null).",
    "source": "CDN-relative source path of {o}.",
    "source_override": "Source-path override for {o} (usually null).",
    "title": "Editorial title attached to {o}.",
    "description": "Editorial description attached to {o} (usually null).",
    "caption": "Editorial caption attached to {o} (usually null).",
    "category": "Editorial category label of {o} (usually null).",
    "alt_text": "Accessibility alt text of {o} (usually null).",
    "height": "Pixel height of {o}.",
    "width": "Pixel width of {o}.",
    "asset_type": "On3 asset-type discriminator of {o} (e.g. Image).",
    "file_system": "Storage file-system flag of {o}.",
    "path": "Storage path of {o}.",
    "type": "Media type field of {o} (file extension, e.g. png).",
    "thumbnail": "Thumbnail variant of {o} (video assets; usually null).",
    "duration": "Duration of {o} when it is a video (usually null or 0).",
    "mime_type": "MIME type of {o}.",
}
_ORG = {
    "key": "On3 numeric key of {o}.",
    "name": "Short name of {o}.",
    "full_name": "Full name of {o} (e.g. 'Alabama Crimson Tide').",
    "known_as": "Common short name of {o}, when On3 lists one.",
    "mascot": "Mascot of {o}.",
    "abbreviation": "Abbreviation of {o}.",
    "slug": "URL slug of {o} on On3.",
    "url_slug": "URL slug variant of {o}'s page, with the key appended.",
    "primary_color": "Primary hex color of {o}.",
    "org_type": "Organization type label of {o} (e.g. HighSchool, College).",
    "org_type_enum": "Organization type enum of {o} (same vocabulary as org_type).",
    "division": "Division or classification of {o} (e.g. NCAA-FB).",
    "site_keys": "JSON-encoded On3 site keys covering {o} (usually null).",
    "asset_url": "Convenience CDN URL of {o}'s logo.",
    "default_asset": "Nested default-asset object of {o} (null when On3 ships none).",
    "res_key": "On3 asset key of {o}'s logo asset.",
    "type": "Organization type label of {o} (e.g. HighSchool, College).",
    "url": "CDN URL of {o}'s logo.",
    "asset": "Nested logo asset object of {o} (stringified or null).",
    "level": "Level of {o} (e.g. HighSchool, College, Professional).",
    "type_enum": "Organization type enum of {o} (same vocabulary as org_type).",
}
_PERSON = {
    "key": "On3 numeric key of {o}.",
    "recruitment_key": "On3 key of {o}'s active recruitment record.",
    "name": "Display name of {o}.",
    "first_name": "First name of {o}.",
    "last_name": "Last name of {o}.",
    "full_name": "Full name of {o}.",
    "known_as_name": "Preferred name of {o}, when it differs from the given name.",
    "slug": "URL slug of {o}'s On3 profile.",
    "home_town_name": "Home town of {o} as On3 lists it (e.g. 'Belleville, MI').",
    "hometown_name": "Home town of {o} as On3 lists it.",
    "high_school_name": "High-school display name on {o}'s record.",
    "high_school": "High-school display name on {o}'s record.",
    "early_enrollee": "Whether {o} early-enrolled at college.",
    "early_signee": "Whether {o} signed during the early signing period.",
    "class_year": "High-school graduating class year of {o}.",
    "class_rank": "Academic class standing of {o} (e.g. Senior, RedShirt Senior).",
    "athlete_verified": "Whether {o}'s athlete profile is verified by On3.",
    "prospect_verified": "Whether {o}'s prospect measurables are verified by On3.",
    "height": "Height of {o}: a formatted string (e.g. '6-8') or inches, depending on the endpoint.",
    "formatted_height": "Human-formatted height of {o} (e.g. '6-3.5').",
    "weight": "Weight of {o} in pounds.",
    "age": "Age of {o} in years, when known.",
    "date_of_birth": "Date of birth of {o} (ISO timestamp string).",
    "jersey_number": "Jersey number of {o}, when listed.",
    "tier": "On3 profile tier of {o} (e.g. Star).",
    "division": "Division of {o}'s current organization (e.g. NCAA-FB, NCAA-BK).",
    "position_abbreviation": "Position abbreviation on {o}'s record.",
    "roster_rating": "On3 roster (transfer-portal-adjusted) rating of {o}, when published.",
    "rating": "On3 rating of {o} (0-100 scale), when published.",
    "predictions": "JSON-encoded On3 RPM (Recruiting Prediction Machine) entries for {o}.",
    "nil_status": "On3 NIL valuation status of {o} (e.g. Normal).",
    "nil_value": "On3 NIL valuation of {o} in US dollars.",
    "sport": "Nested sport object of {o} (null when On3 ships none).",
    "default_sport": "Nested primary-sport object of {o} (null when On3 ships none).",
    "tags": "JSON-encoded list of On3 profile tags on {o} (e.g. Influencer).",
    "default_asset_url": "Convenience CDN URL of {o}'s headshot.",
    "secondary_position": "Secondary position of {o}, when listed.",
    "org_season_count": "Seasons {o} has spent at the program.",
    "years_active": "Years {o} has been active in coaching.",
    "twitter_handle": "Twitter/X handle of {o}, when listed.",
    "instagram_profile": "Instagram profile of {o}, when listed.",
    "tik_tok_handle": "TikTok handle of {o}, when listed.",
    "espn_profile": "ESPN profile link of {o}, when listed.",
    "two_four_seven_profile": "247Sports profile link of {o}, when listed.",
    "rivals_profile": "Rivals profile link of {o}, when listed.",
    "can_manage_recruitment": "Whether {o} can self-manage the recruitment on On3.",
    "sport_key": "On3 key of {o}'s athlete-sport profile (person x sport).",
    "organization_level": "Level of {o}'s current organization (e.g. HighSchool, College, Professional).",
    "state_abbr": "State abbreviation of {o}'s home town.",
    "measurements": "JSON-encoded list of {o}'s measurement records (type, value, verification).",
    "hometown_key": "On3 numeric key of {o}'s home town.",
    "hometown_abbr": "Abbreviated home town of {o} (city, state).",
    "state_key": "On3 numeric key of {o}'s home state.",
    "state_name": "Name of {o}'s home state.",
}
_RATING = {
    "key": "On3 key of {o} record.",
    "rating": "Numeric value of {o} (0-100 scale).",
    "stars": "Star rating of {o} (2-5).",
    "national_rank": "National rank of {o}.",
    "position_rank": "Position rank of {o}.",
    "state_rank": "State rank of {o}.",
    "position_abbr": "Position abbreviation {o} was assigned at.",
    "state_abbr": "State abbreviation {o} was assigned in.",
    "five_star_plus": "Five-star-plus flag on {o}.",
    "nearly_five_star_plus": "Near-five-star-plus flag on {o}.",
    "year": "Ranking cycle year of {o}.",
    "type": "Type of {o} (e.g. Player, TransferPortal, Team).",
    "consensus_rating": "Industry-consensus numeric rating paired with {o} (0-100 scale).",
    "consensus_stars": "Industry-consensus star rating paired with {o} (2-5).",
    "consensus_national_rank": "Industry-consensus national rank paired with {o}.",
    "consensus_position_rank": "Industry-consensus position rank paired with {o}.",
    "consensus_state_rank": "Industry-consensus state rank paired with {o}.",
    "sport_key": "On3 numeric key of the sport {o} is in.",
    "sport_name": "Name of the sport {o} is in (e.g. Football).",
    "sport_abbr": "Abbreviation of the sport {o} is in.",
}
_STATUS = {
    "is_committed": "Whether the recruit is committed ({o}).",
    "is_signed": "Whether the recruit has signed ({o}).",
    "is_transfer": "Whether the recruit is a transfer ({o}).",
    "is_enrolled": "Whether the recruit is enrolled ({o}).",
    "commitment_date": "Date of the commitment ({o}; ISO timestamp string).",
    "type": "Type of {o} (e.g. Committed, Signed, Enrolled, None).",
    "date": "Date {o} took effect (ISO timestamp string).",
    "short_term_signee": "Short-term-signee flag of {o} (null when not applicable).",
    "transfer": "Transfer flag of {o} (null when not applicable).",
    "draft": "Draft flag of {o} (null when not applicable).",
    "expected_to_transfer": "Expected-to-transfer flag of {o} (null when not applicable).",
    "recruitment_year": "Recruiting-cycle year {o} belongs to.",
    "transfer_entered": "Date the player entered the transfer portal ({o}; null when never entered).",
    "transfer_withdrawn": "Date the player withdrew from the transfer portal ({o}; null when never withdrawn).",
    "transferred_asset": "Nested asset of the program transferred to ({o}; usually null).",
    "decommitted_asset": "Nested asset of the program decommitted from ({o}; usually null).",
    "committed_organization_key": "On3 key of the committed-to program ({o}).",
    "committed_organization_slug": "URL slug of the committed-to program ({o}).",
    "committed_organization_asset_url": "CDN URL of the committed-to program's logo ({o}).",
    "committed_organization_asset": "Nested logo asset of the committed-to program ({o}; stringified or null).",
    "committed_asset": "Nested asset of the committed-to program ({o}; usually null).",
    "committed_asset_res": "Nested logo asset of the committed-to program ({o}; usually null).",
    "transferred_asset_res": "Nested logo asset of the program transferred to ({o}; usually null).",
    "class_rank": "Academic class standing recorded on {o} (e.g. Senior).",
    "recruitment_key": "On3 key of the recruitment record {o} belongs to.",
    "withdrawn_transfer": "Whether the player withdrew from the transfer portal ({o}).",
    "withdrawn_transfer_date": "Date the player withdrew from the transfer portal ({o}; null when never withdrawn).",
    "highest_interest_level": "Highest interest level a program has logged for the recruit ({o}).",
    "interest_count": "Number of programs with logged interest in the recruit ({o}).",
    "sport_name": "Sport {o} applies to.",
}
_SPORT = {
    "key": "On3 numeric key of {o}.",
    "name": "Name of {o} (e.g. Football).",
    "slug": "URL slug of {o}.",
    "abbreviation": "Abbreviation of {o}.",
    "abbr": "Abbreviation of {o}.",
    "is_rankable": "Whether On3 ranks players in {o}.",
    "is_industry_rankable": "Whether industry-consensus rankings exist for {o}.",
    "is_scoutable": "Whether On3 scouting reports exist for {o}.",
}
_VALUATION = {
    "key": "On3 key of {o} record.",
    "nil_status": "Status of {o} (e.g. Normal).",
    "valuation": "{O} in US dollars.",
    "total_value": "{O} in US dollars.",
    "valuation_change": "Change in {o} since the previous update, in US dollars.",
    "change": "Change in {o} since the previous update, in US dollars.",
    "followers": "Social-media follower count feeding {o}.",
    "rank": "Overall rank of {o} across On3's NIL 100.",
    "group_rank": "Rank of {o} within its group (sport or position).",
    "group_name": "Name of the group {o} is ranked within (usually null).",
    "last_updated": "Unix timestamp (seconds) of the last update to {o}.",
    "whisper": "On3 whisper valuation (reported deal value) behind {o}, in US dollars.",
    "whisper_change": "Change in the whisper valuation behind {o}, in US dollars.",
    "social_valuations": "JSON-encoded per-platform social valuations behind {o}.",
    "tags": "JSON-encoded list of NIL tags on {o} (e.g. Influencer).",
    "roster_value": "Nested roster-value object of {o} (usually null).",
    "nil_value": "Nested NIL-value object of {o} (usually null).",
}
_PICK = {
    "key": "On3 key of {o}.",
    "organization": "Nested program object {o} predicts (stringified; usually null).",
    "year": "Recruiting class year {o} applies to (0 when unset).",
    "date_added": "Timestamp {o} was logged (ISO string).",
    "expert": "Nested expert object of {o} (stringified; usually null).",
    "confidence": "Confidence the expert attached to {o}, in percent.",
    "article_link": "On3 article link accompanying {o}, when any.",
    "premium": "Whether {o} is behind the On3+ paywall.",
    "correct": "Whether {o} proved correct ('True'/'False' string; null while open).",
    "days_correct": "Days {o} stood correct.",
    "flipped_from_organization": "Program the recruit flipped from when {o} was logged, when any.",
    "previous_confidence": "Confidence of the expert's previous pick on the recruit, when any.",
    "previous_date_added": "Timestamp of the expert's previous pick on the recruit (a 0001-01-01 sentinel when none).",
    "type": "Type of {o} (e.g. NewLeader, Flip).",
    "expert_accuracy": "Lifetime accuracy of the expert behind {o}, in percent.",
    "top_teams": "JSON-encoded list of the recruit's top teams at the time of {o}.",
}
_EXPERT = {
    "key": "On3 key of {o}.",
    "name": "Display name of {o}.",
    "nice_name": "URL slug of {o}.",
    "twitter_handle": "Twitter/X handle of {o}, when listed.",
    "instagram_handle": "Instagram handle of {o}, when listed.",
    "youtube_url": "YouTube URL of {o}, when listed.",
    "bio": "Biography text of {o}, when listed.",
    "job_title": "Job title of {o}, when listed.",
    "site_affiliation": "On3 site {o} writes for, when listed.",
    "profile_picture": "Profile picture of {o} (nested; usually null).",
    "accuracy": "Lifetime pick accuracy of {o}, in percent.",
}
_ARTICLE = {
    "key": "On3 article key of {o}.",
    "slug": "URL slug of {o}.",
    "full_url": "Site-relative URL of {o}.",
    "title": "Headline of {o}.",
    "is_premium": "Whether {o} is behind the On3+ paywall.",
    "date_published_gmt": "Publication timestamp of {o} (GMT, ISO string).",
}
_STATE = {
    "key": "On3 numeric key of {o}.",
    "name": "Name of {o}.",
    "abbreviation": "Two-letter abbreviation of {o}.",
    "country_key": "On3 numeric key of {o}'s country.",
}
_POSITION = {
    "key": "On3 numeric key of {o}.",
    "name": "Name of {o} (e.g. Quarterback).",
    "abbreviation": "Abbreviation of {o} (e.g. QB).",
    "abbr": "Abbreviation of {o} (e.g. QB).",
    "position_type": "Position type of {o} (e.g. Offense, Defense).",
}
_HOMETOWN = {
    "key": "On3 numeric key of {o}.",
    "name": "Name of {o} (city, state).",
    "abbr": "Abbreviated form of {o} (city, state).",
    "state": "State of {o}, when listed.",
}
_ASSETLINK = {
    "key": "On3 asset key behind {o}.",
    "url": "Full CDN URL of {o}.",
    "full_name": "Full name of the program {o} belongs to.",
    "slug": "URL slug of the program {o} belongs to.",
}
_LEAVES: dict[str, dict[str, str]] = {
    "asset": _ASSET,
    "org": _ORG,
    "person": _PERSON,
    "rating": _RATING,
    "status": _STATUS,
    "sport": _SPORT,
    "valuation": _VALUATION,
    "pick": _PICK,
    "expert": _EXPERT,
    "article": _ARTICLE,
    "state": _STATE,
    "position": _POSITION,
    "hometown": _HOMETOWN,
    "assetlink": _ASSETLINK,
    "json": {},
}

# Top-level (no owner) leaves, keyed by schema or ``*`` for every On3 table.
_TOP: dict[str, dict[str, str]] = {
    "*": {
        "key": "On3 numeric key of the row's record.",
        "name": "Display name of the row's record.",
        "slug": "URL slug of the row's record on On3.",
        "description": "Free-text description or biography shipped by On3.",
        "type": "Type label of the row (vocabulary depends on the endpoint).",
        "link": "Site-relative On3 link for the row, when any.",
        "index": "Zero-based position of the row in the On3 response list.",
        "rec_status": "Recruiting status of the player (e.g. Committed, Enrolled).",
        "eligibility": "Eligibility note On3 attaches to the transfer, when any.",
        "withdrawn_transfer": "Whether the player withdrew from the transfer portal.",
        "withdrawn_transfer_date": "Date the player withdrew from the transfer portal, when withdrawn.",
        "valuation": "Nested NIL valuation object (stringified; usually null).",
        "rating": "On3 rating of the player (0-100 scale), when published.",
        "nil_status": "On3 NIL valuation status (e.g. Normal).",
        "nil_value": "On3 NIL valuation in US dollars.",
        "ranking": "Nested ranking object (stringified; usually null).",
        "ranking_key": "On3 key of the ranking cycle the row belongs to.",
        "ranking_year": "Ranking cycle year.",
        "ranking_type": "Ranking type (e.g. Player, TransferPortal, Team).",
        "sport": "Sport slug or name of the row.",
        "sport_key": "On3 numeric key of the sport.",
        "sport_name": "Name of the sport (e.g. Football).",
        "stars": "Star rating (2-5).",
        "strength": "Strength score On3 attaches to the ranking entry.",
        "overall_rank": "Overall national rank in the ranking.",
        "position_rank": "Rank at the player's position in the ranking.",
        "state_rank": "Rank within the player's state in the ranking.",
        "position_abbr": "Position abbreviation the ranking entry was assigned at.",
        "state_abbr": "State abbreviation the ranking entry was assigned in.",
        "five_star_plus": "Flag for On3's five-star-plus designation.",
        "nearly_five_star_plus": "Flag for On3's near-five-star-plus designation.",
        "consensus_rating": "Industry-consensus numeric rating (0-100 scale).",
        "consensus_stars": "Industry-consensus star rating (2-5).",
        "consensus_overall_rank": "Industry-consensus overall national rank.",
        "consensus_position_rank": "Industry-consensus position rank.",
        "consensus_state_rank": "Industry-consensus state rank.",
        "year": "Recruiting class year of the row.",
        "class_year": "High-school graduating class year.",
        "high_school_name": "High-school display name (top-level field).",
        "home_town_name": "Home town as On3 lists it.",
        "hometown_name": "Home town as On3 lists it.",
        "hometown_state": "Home-town state, when listed.",
        "current_state": "State the player currently plays in, when listed.",
        "position_abbreviation": "Position abbreviation on the record.",
        "primary_position": "Nested primary-position object (stringified).",
        "height": "Height as a formatted string (e.g. '6-3.5').",
        "weight": "Weight in pounds.",
        "age": "Age in years, when known.",
        "jersey_number": "Jersey number, when listed.",
        "early_enrollee": "Whether the recruit early-enrolled at college.",
        "early_signee": "Whether the recruit signed during the early signing period.",
        "athlete_verified": "Whether the athlete profile is verified by On3.",
        "prospect_verified": "Whether the prospect measurables are verified by On3.",
        "predictions": "JSON-encoded On3 RPM (Recruiting Prediction Machine) entries.",
        "default_asset_url": "Convenience CDN URL of the record's headshot or logo.",
        "organization_type": "Legal form of the collective (e.g. LLC, 501(c)(3)).",
        "status": "Roster status label On3 attaches to the player (usually null).",
        "rpm": "Nested RPM (Recruiting Prediction Machine) summary object (stringified).",
        "industry_comparison": "JSON-encoded list of the player's rankings across industry services.",
        "transfer_industry_comparison": "JSON-encoded list of the player's transfer-portal rankings across industry services.",
        "organization_history": "JSON-encoded list of the programs the player has been on, newest first.",
        "entered_article": "On3 article covering the player entering the transfer portal (stringified; usually null).",
        "exited_article": "On3 article covering the player exiting the transfer portal (stringified; usually null).",
        "person_sport_key": "On3 key of the athlete-sport profile (person x sport).",
        "pso_key": "On3 key of the person-sport-organization (roster membership) record.",
        "total": "Total drafted players counted in the row.",
        "round": "Draft round number.",
        "value": "Filter value as On3 lists it.",
        "handle": "Account handle on the platform.",
        "handshake": "Whether the account is linked (handshake-verified) on On3.",
        "social_type": "Social-media platform of the row (e.g. Instagram).",
        "followers": "Follower count on the platform.",
        "player_measurements": "JSON-encoded list of the player's measurement records (type, value, verification).",
        "sort": "Sort key On3 uses to order the measurement rows.",
        "measurement_record": "Nested measurement record object (stringified).",
        "text": "Text of the update as On3 displays it.",
        "replacement_text": "Alternate text for the update, when any.",
        "date_added": "Unix timestamp (seconds) the record was added.",
        "date_occurred": "Unix timestamp (seconds) the event occurred.",
        "date_updated": "Timestamp the record was last updated (ISO string).",
        "object_key": "On3 key of the object the update refers to.",
        "person_key": "On3 numeric key of the person.",
        "organization_key": "On3 numeric key of the program.",
        "recruitment_key": "On3 key of the recruitment record.",
        "class_year_recruitment_key": "On3 recruitment key for the recruiting-class cycle.",
        "oracle_key": "On3's internal oracle identifier for the record.",
        "category": "Category label On3 attaches to the row.",
        "body": "Body text of the quote.",
        "source_url": "Source URL of the video.",
        "title": "Title of the row's record.",
        "thumbnail": "Thumbnail image URL.",
        "date": "Date of the row (ISO string or Unix timestamp, by endpoint).",
        "date_unix": "Unix timestamp (seconds) of the row's date.",
        "is_featured": "Whether the video is featured on the profile.",
        "featured_order": "Display order among featured videos, when featured.",
        "category_key": "On3 key of the video category.",
        "category_value": "Video category label (e.g. Highlights).",
        "person_sport": "Sport slug the video belongs to.",
        "locked": "Whether the RPM summary is locked behind On3+.",
        "change_1": "Direction of the rating change since the previous update (e.g. Increase); the numeric suffix is a json_normalize de-duplication artefact.",
        "change_38": "Direction of the On3 rating change since the previous update (e.g. Increase); the numeric suffix is a json_normalize de-duplication artefact.",
        "consensus_change_41": "Direction of the consensus rating change since the previous update (e.g. Increase); the numeric suffix is a json_normalize de-duplication artefact.",
        "ranking_key_2": "Ranking key repeated from the nested ranking object (json_normalize de-duplication suffix).",
        "person_key_2": "Person key repeated from the nested person object (json_normalize de-duplication suffix).",
        "organization_key_2": "Organization key repeated from the nested organization object (json_normalize de-duplication suffix).",
        "default_asset_key_2": "Asset key repeated from the nested default-asset object (json_normalize de-duplication suffix).",
        "social_asset_key_2": "Asset key repeated from the nested social-asset object (json_normalize de-duplication suffix).",
        "talent_rank": "On3 talent-composite rank of the roster (national), as a string.",
        "prev_talent_rank": "Previous On3 talent-composite rank of the roster, as a string.",
        "conference_rank": "On3 talent-composite rank of the roster within its conference, as a string.",
        "prev_conference_rank": "Previous conference talent-composite rank, as a string.",
        "average_rating": "Average On3 rating across the roster, as a string.",
        "prev_average_rating": "Previous average On3 rating across the roster, as a string.",
        "average_nil_value": "Average On3 NIL valuation across the roster, in US dollars.",
        "total_nil_value": "Total On3 NIL valuation across the roster, in US dollars.",
        "blue_chip_percent": "Share of the state's drafted players who were blue-chip (4/5-star) recruits.",
        "population_percent": "State's share of the total drafted-player population.",
        "talent_ratio": "Blue-chip share divided by population share for the state.",
        "five_stars": "Count of five-star recruits.",
        "four_stars": "Count of four-star recruits.",
        "three_stars": "Count of three-star recruits.",
        "zero_stars": "Count of unrated (zero-star) recruits.",
        "ratings": "JSON-encoded list of the player's per-service ratings (On3, Rivals, 247Sports, ESPN).",
        "high_school_rating": "High-school rating annotation as shipped by On3 (mixed-type; stringified).",
        "nil_value_key": "On3 key of the player's NIL valuation record.",
        "measurement_key": "On3 key of the measurement type.",
        "measurement_name": "Measurement name (e.g. Height, 40-Yard Dash).",
        "current_person_measurement": "The player's current value for the measurement.",
        "current_measurement_verified": "Whether the current measurement is verified (string flag).",
        "top300_difference": "Difference between the player's value and the top-300 average for the measurement.",
        "top300_average": "Average value of the measurement across the top-300 recruits.",
        "combine_drafted_average": "Average value of the measurement across drafted combine participants.",
        "combine_drafted_difference": "Difference between the player's value and the drafted-combine average.",
        "launch_date": "Launch date of the collective (ISO string).",
        "twitter_handle": "Twitter/X handle, when listed.",
        "instagram_handle": "Instagram handle, when listed.",
        "tik_tok_handle": "TikTok handle, when listed.",
        "youtube_handle": "YouTube handle, when listed.",
        "linked_in_handle": "LinkedIn handle, when listed.",
        "website_name": "Website display name of the collective.",
        "website_url": "Website URL of the collective.",
        "mission_statement": "Mission statement of the collective.",
        "annual_goal_amount": "Annual fundraising goal of the collective, in US dollars.",
        "confirmed_raised_amount": "Confirmed amount raised by the collective, in US dollars.",
        "merged_into_group_key": "On3 key of the collective this one merged into, when merged.",
        "merged_into_group": "Name of the collective this one merged into, when merged.",
        "founders": "JSON-encoded list of the collective's founders.",
        "sports": "JSON-encoded list of the sports the record covers.",
        "default_asset_domain_override": "CDN domain override for the record's default asset (usually null).",
        "home_town": "Home town as On3 lists it (e.g. 'Pewaukee, WI').",
        "high_school": "High-school display name on the record.",
        "state_key": "On3 numeric key of the state.",
        "state_name": "Name of the state the row aggregates (e.g. Alabama).",
        "state_abbreviation": "Two-letter state abbreviation.",
        "state_country_key": "On3 numeric key of the state's country.",
        "source": "CDN-relative source path of the image asset.",
        "duration": "Duration of the asset when it is a video (0 for images).",
        "last_updated": "Unix timestamp (seconds) of the last update to the NIL valuation.",
        "whisper": "On3 whisper valuation (reported deal value) behind the NIL valuation, in US dollars.",
        "whisper_change": "Change in the whisper valuation since the previous update, in US dollars.",
        "group_rank": "Rank of the NIL valuation within its group (sport or position).",
        "group_name": "Name of the group the NIL valuation is ranked within (usually null).",
        "tags": "JSON-encoded list of NIL tags on the valuation (e.g. Influencer).",
        "roster_value": "Nested roster-value object of the NIL valuation (usually null).",
        "social_valuations": "JSON-encoded per-platform social valuations behind the NIL valuation.",
        "valuation_change": "Change in the NIL valuation since the previous update, in US dollars.",
    },
    "people_latest_valuation": {
        "rank": "Overall rank of the player's NIL valuation across On3's NIL 100.",
        "valuation": "On3 NIL valuation in US dollars.",
    },
    "people_valuation_growth": {"valuation": "On3 NIL valuation in US dollars at the row's date."},
    "player_person_rankings": {
        "consensus_change_41": "Direction of the consensus rating change since the previous update (e.g. Increase); the numeric suffix is a json_normalize de-duplication artefact.",
    },
    "collective_groups": {"organization_type": "Legal form of the collective (e.g. LLC, 501(c)(3))."},
}
_TOP["*"].update(
    {
        "high_school_key": "On3 numeric key of the high school.",
        "high_school_full_name": "Full name of the high school (with mascot).",
        "high_school_name_2": "Name field of the nested high-school object (json_normalize de-duplication of high_school_name).",
        "high_school_known_as": "Common short name of the high school, when On3 lists one.",
        "high_school_mascot": "High-school mascot.",
        "high_school_abbreviation": "High-school abbreviation.",
        "high_school_asset_url": "Convenience CDN URL of the high-school logo.",
        "high_school_slug": "URL slug of the high school on On3.",
        "high_school_url_slug": "URL slug variant of the high-school page, with the key appended.",
        "high_school_primary_color": "Primary hex color of the high school.",
        "high_school_org_type": "Organization type label of the school (e.g. HighSchool).",
        "high_school_org_type_enum": "Organization type enum of the school.",
        "high_school_division": "Division or classification of the high school, when listed.",
        "high_school_site_keys": "JSON-encoded On3 site keys covering the school (usually null).",
        "high_school_default_asset": "Nested default-asset object of the high school (null when On3 ships none).",
    }
)

_POSSESSIVE_FIX = (("person's", "person's"),)


def _owner_phrase(chain: list[str]) -> tuple[str, str]:
    """``[person, high_school, default_asset]`` -> ("the person's high school's logo asset", "asset")."""
    phrase = ""
    kind = ""
    for i, seg in enumerate(chain):
        noun, kind = _ENTITIES[seg]
        if kind == "asset" and seg in ("default_asset", "asset", "profile_picture"):
            prev_kind = _ENTITIES[chain[i - 1]][1] if i else ""
            noun = (
                "headshot asset" if prev_kind in ("person", "expert") else "logo asset" if prev_kind == "org" else noun
            )
        phrase = f"{phrase}'s {noun}" if phrase else f"the {noun}"
    return phrase, kind


def compose(schema: str, col: str) -> str:
    specific = _TOP.get(schema, {}).get(col)
    if specific:
        return specific
    chain: list[str] = []
    rest = col
    while True:
        kind = _ENTITIES[chain[-1]][1] if chain else ""
        if kind == "org" and rest.startswith("org_"):
            rest = rest[4:]  # recruitments_profile nests the school as ``high_school_org_*``
        if chain and (rest[:-2] if rest.endswith("_2") else rest) in _LEAVES.get(kind, {}):
            break  # ``rating_position_rank``: the leaf is the rating's, not a position's
        if not chain and rest in _TOP["*"]:
            break  # a top-level ``position_rank`` is not a position's rank
        m = _ENTITY_RE.match(rest)
        if not m:
            break
        chain.append(m.group(1))
        rest = rest[m.end() :]
    if not chain:
        return _TOP["*"].get(col, "")
    if not rest:  # the whole column is a nested object (``commit_status_transferred_asset_res``)
        owner = _owner_phrase(chain[:-1])[0] if len(chain) > 1 else "the row"
        return f"Nested {_ENTITIES[chain[-1]][0]} object of {owner} (stringified; usually null)."
    owner, kind = _owner_phrase(chain)
    leaf, dedup = (rest[:-2], True) if rest.endswith("_2") else (rest, False)
    tpl = _LEAVES.get(kind, {}).get(leaf)
    if not tpl:
        return ""
    text = tpl.replace("{o}", owner).replace("{O}", owner[0].upper() + owner[1:])
    return text[:-1] + " (json_normalize de-duplication suffix)." if dedup else text


def main() -> None:
    out: dict[str, dict[str, str]] = {}
    for f in sorted(glob.glob(os.path.join(SCHEMA_DIR, "*.yaml"))):
        d = yaml.safe_load(open(f, encoding="utf-8")) or {}
        schema = d.get("schema")
        block = {}
        for c in d.get("columns") or []:
            text = compose(schema, c["name"])
            if text:
                block[c["name"]] = text
        if block:
            out[schema] = block
    yaml.safe_dump(out, sys.stdout, sort_keys=True, allow_unicode=True, width=120)


if __name__ == "__main__":
    main()

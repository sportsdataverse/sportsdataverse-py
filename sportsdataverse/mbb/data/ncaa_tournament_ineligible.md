<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NCAA tournament ineligible programs (`ncaa_tournament_ineligible.csv`)](#ncaa-tournament-ineligible-programs-ncaa_tournament_ineligiblecsv)
  - [Reclassification rules](#reclassification-rules)
  - [Postseason bans](#postseason-bans)
  - [Coverage and weak spots](#coverage-and-weak-spots)
  - [Auto-bid cases](#auto-bid-cases)
  - [Maintaining the list](#maintaining-the-list)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NCAA tournament ineligible programs (`ncaa_tournament_ineligible.csv`)

Division I men's and women's basketball programs that could not play in the NCAA tournament, one
row per `(season, league, team_id)`, for the 2009-10 to 2026-27 seasons. ESPN does not flag them.
`mbb_tournament_ineligible()` reads the file, and the college season products (bracketology,
season sim, strength of schedule) use it by default to set `eligible`.

Researched 2026-11-06 from the sources in each row's `source_url`. Nothing here comes from
stats.ncaa.org or Sports Reference.

| Column | Meaning |
|---|---|
| `season` | End year: `2024` is 2023-24. |
| `league` | `mens` or `womens`. |
| `team_id` | ESPN team id (Utf8). Every id was checked against ESPN's team endpoint, and the 2024 and 2025 ones against the committed ESPN schedules. |
| `team` | School name. |
| `reason` | `reclassifying` (moving up to Division I), `apr_ban`, `infractions_ban` (an NCAA Committee on Infractions penalty), `self_imposed_ban`, or `other`. |
| `detail` | Short note, such as the reclassification year or the case behind the ban. |
| `source_url` | The most authoritative source found: NCAA, conference or school releases first, then news reports and Wikipedia. |
| `confidence` | `high` when the NCAA, school or conference says so; `medium` when only a secondary source does. Low-confidence findings are left out (see below). |

300 rows: 224 reclassifying (112 per league), 51 APR, 7 infractions, 14 self-imposed and 4 other.

## Reclassification rules

| Period | Rule | Source |
|---|---|---|
| to 2011 | A "five-year" move from Division II that included an application year, so in practice four ineligible seasons. A 2007 membership moratorium gave Presbyterian five (2008-2012). | [NCAA News 2010](https://ncaanewsarchive.s3.amazonaws.com/2010-news-stories/july-latest-news/division-i-gains-two-new-members,-loses-two-others-to-division-iii.html), [NCAA News 2011](https://ncaanewsarchive.s3.amazonaws.com/2011/july/four-schools-cleared-for-active-di-status.html) |
| 2011-2024 | Four years from Division II. | [Omaha 2015](https://unomaha.edu/news/2015/08/move-to-division-one-athletics-is-complete.php) |
| July 2020 | St. Thomas (MN) allowed to move straight up from Division III, on a five-year path. | [SI 2020](https://www.si.com/college/2020/07/15/ncaa-allows-st-thomas-jump-division-one) |
| January 2025 | Three years from Division II and four from Division III, for schools that meet academic and financial benchmarks; schools already moving could opt in. | [NCAA 2025](https://www.ncaa.org/media-center-division-i-adopts-new-reclassification-criteria/) |
| June 2025 | Lindenwood, Southern Indiana, East Texas A&M, Stonehill, Queens and St. Thomas approved early, eligible from 2025-26. | [OVC 2025](https://ovcsports.com/news/2025/6/23/general-lindenwood-and-southern-indiana-earn-full-division-i-status.aspx) |
| June 2026 | Le Moyne approved, eligible from 2026-27. West Georgia is on the three-year plan (eligible 2027-28). | [CNY Central](https://cnycentral.com/sports/le-moyne-cleared-to-compete-in-ncaa-championships-beginning-this-year), [UWG](https://uwgathletics.com/news/2025/8/6/athletics-uwg-to-fully-join-ncaa-division-i-by-fall-2027-under-accelerated-plan.aspx) |

New Haven (from 2025-26) and West Florida (ESPN `2697`, from 2026-27) entered under the three-year
rule, so the file lists them through 2027 only. Their later seasons are added once the season starts.

## Postseason bans

- **APR:** the NCAA's annual penalty list (spring) applies to the next season. The NCAA suspended APR
  penalties in October 2020, so no APR bans exist for 2022-2024. Penalties returned for 2024-25, but
  the first basketball postseason ban after the pause was Coppin State, for 2025-26.
- **Infractions and self-imposed:** from USC 2010 to Oklahoma State 2022. None were found for
  2023-2027. Since August 2024 the NCAA has mostly stopped imposing postseason bans on schools that
  cooperate.
- **Lifted on appeal, waived or overturned, so not listed:**
  - Cal State Bakersfield 2013.
  - Alabama State 2014.
  - Savannah State: women 2016, men 2018.
  - Hawaii 2017.
  - The conditional APR waivers of 2011 and 2012.
  - Oklahoma State's ban was stayed during its appeal, so it is listed for 2022, the season it was served.

## Coverage and weak spots

| Seasons | Reclassifying | Bans |
|---|---|---|
| 2010-2021 | complete | APR lists complete for the men; women's coverage is thinner (release articles and SWAC tournament pages) |
| 2022-2025 | complete | no APR bans (suspended); no infractions bans found |
| 2026-2027 | complete | APR partial: HBCU coverage plus Detroit Mercy and Alabama A&M; the NCAA's full lists were not retrieved, so a non-HBCU ban may be missing |

Low-confidence findings left out of the file:

- New Orleans 2011 and 2013 (an aborted move down, then a return; its 2012 provisional Division II
  season is kept as `other`).
- Hartford 2023: its last Division I season, as an independent; no explicit NCAA ban was found.
- Centenary 2011: last Division I season.
- Grambling women 2016: conflicting sources.
- Delaware State men 2021: the COVID deferral option means the season served is unconfirmed.
- Alabama A&M 2017: an all-sports data penalty that may have been cleared.

## Auto-bid cases

When a barred team wins its conference tournament the products pass the auto-bid to the final's
runner-up, and to the eligible member with the best conference record when both finalists are barred.
That is the NEC and OVC rule:

- 2023 NEC men: Merrimack beat Fairleigh Dickinson 67-66 and FDU took the bid
  (`tests/mbb/test_mbb_tournament_eligibility.py`).
- 2024 OVC women: Southern Indiana beat UT Martin and UT Martin took the bid.

The ASUN and WAC gave the bid to the best eligible regular-season team instead. The products do not
model that rule, so these two cases come out differently:

- 2022 ASUN men: Bellarmine won the final and Jacksonville State took the bid.
- 2021 WAC women: California Baptist won the final and Utah Valley took the bid.

## Maintaining the list

Add rows when one of these is published, with the source URL:

- The NCAA's APR penalty list (May): bans for the next season. Check the full list at
  `web3.ncaa.org/aprsearch`.
- Division I Board membership actions (June) and new members starting each July 1: reclassifying seasons.
- Committee on Infractions decisions and school announcements of self-imposed bans.

Then run `uv run pytest tests/mbb/test_mbb_tournament_eligibility.py` (format, ESPN-id and known-case checks).

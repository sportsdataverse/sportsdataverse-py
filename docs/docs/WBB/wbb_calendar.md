---
title: WBB Calendar
sidebar_label: WBB Calendar
---
### wehoop.wbb.wbb_calendar(season: int)
wbb_calendar - look up the women’s college basketball calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing
    calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.

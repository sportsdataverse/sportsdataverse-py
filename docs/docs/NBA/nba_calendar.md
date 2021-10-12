---
title: NBA Calendar
sidebar_label: NBA Calendar
---

### hoopR.nba.nba_calendar(season: int)
nba_calendar - look up the NBA calendar for a given season

Args:

    season (int): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing
    calendar dates for the requested season.

Raises:

    ValueError: If season is less than 2002.



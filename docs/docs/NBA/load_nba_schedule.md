---
title: Load NBA Schedule
sidebar_label: Load NBA Schedule
---

### hoopR.nba.load_nba_schedule(seasons: List[int])
Load NBA schedule data

Example:

    nba_df = hoopR.nba.load_nba_schedule(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    schedule for  the requested seasons.

Raises:

    ValueError: If season is less than 2002.



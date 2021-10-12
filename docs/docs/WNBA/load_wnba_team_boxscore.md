---
title: Load WNBA Team Boxscore
sidebar_label: Load WNBA Team Boxscore
---

### wehoop.wnba.load_wnba_team_boxscore(seasons: List[int])
Load WNBA team boxscore data

Example:

    wnba_df = wehoop.wnba.load_wnba_team_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    team boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.


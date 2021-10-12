---
title: Load NBA Player Boxscore
sidebar_label: Load NBA Player Boxscore
---

### hoopR.nba.load_nba_player_boxscore(seasons: List[int])
Load NBA player boxscore data

Example:

    nba_df = hoopR.nba.load_nba_player_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    player boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.
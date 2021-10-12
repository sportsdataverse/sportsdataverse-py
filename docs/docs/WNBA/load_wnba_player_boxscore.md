---
title: Load WNBA Player Boxscore
sidebar_label: Load WNBA Player Boxscore
---

### wehoop.wnba.load_wnba_player_boxscore(seasons: List[int])
Load WNBA player boxscore data

Example:

    wnba_df = wehoop.wnba.load_wnba_player_boxscore(seasons=range(2002,2022))

Args:

    seasons (list): Used to define different seasons. 2002 is the earliest available season.

Returns:

    pd.DataFrame: Pandas dataframe containing the
    player boxscores available for the requested seasons.

Raises:

    ValueError: If season is less than 2002.

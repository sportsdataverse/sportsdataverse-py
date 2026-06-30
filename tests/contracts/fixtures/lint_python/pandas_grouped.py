df.groupby("game_id")["ep"].shift(1)  # noqa: F821 -- pandas grouped, clean

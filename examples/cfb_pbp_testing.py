import pandas as pd

import sportsdataverse as sdv

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


def main():
    processor = sdv.cfb.CFBPlayProcess(gameId=401403867)
    pbp_init = processor.espn_cfb_pbp()
    pbp_fin = processor.run_processing_pipeline()
    plays_df = pd.DataFrame(pbp_fin["plays"])
    special_teams = plays_df[plays_df["sp"] == True]
    special_teams = special_teams[
        [
            "text",
            "type.text",
            "fg_kicker_player_name",
            "yds_fg",
            "fg_attempt",
            "fg_made",
            "fg_return_player_name",
            "fg_block_player_name",
            "kickoff_player_name",
            "yds_kickoff",
            "kickoff_return_player_name",
            "yds_kickoff_return",
            "punt_return_player_name",
            "yds_punt_return",
            "punter_player_name",
            "yds_punted",
            "punt_block_player_name",
            "yds_punt_gained",
        ]
    ]
    print(special_teams)


if __name__ == "__main__":
    main()

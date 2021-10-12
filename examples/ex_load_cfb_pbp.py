import sportsdataverse
def main():
    cfb_df = sportsdataverse.cfb.load_cfb_pbp(seasons=range(2011,2021))
    print(cfb_df.head())
    nfl_df = sportsdataverse.nfl.load_nfl_pbp(seasons=range(2011,2021))
    print(nfl_df.head())

if __name__ == "__main__":
    main()
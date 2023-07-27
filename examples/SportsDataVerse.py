import sportsdataverse

######################################################################################################
##                                              CFB Secion                                          ##
######################################################################################################


def getCfbEspnTeams():
    #############################################################################
    #      module 'sportsdataverse.cfb' has no attribute 'espn_cfb_teams'      ##
    #############################################################################

    print("sportsdataverse.cfb.espn_cfb_teams()")
    print("Load college football team ID information and logos")
    cfbEspnTeams_df = sportsdataverse.cfb.espn_cfb_teams()
    print(cfbEspnTeams_df)


def getEspnPbp():
    print("sportsdataverse.cfb.CFBPlayProcess(gameId=401256137).espn_cfb_pbp()")
    print(
        "espn_cfb_pbp() - Pull the game by id. Data from API endpoints: college-football/playbyplay, college-football/summary"
    )
    espnPbp = sportsdataverse.cfb.CFBPlayProcess(gameId=401256137).espn_cfb_pbp()
    print(espnPbp)


def getCfbPbp():
    print("sportsdataverse.cfb.load_cfb_pbp(seasons=range(2009,2010))")
    print("Load college football play by play data .")
    cfbPbp_df = sportsdataverse.cfb.load_cfb_pbp(seasons=range(2009, 2010))
    print(cfbPbp_df)


def getEspnCfbCalendar():
    print("sportsdataverse.cfb.espn_cfb_calendar(season=2020, groups=80)")
    print("cfb_calendar - look up the men’s college football calendar for a given season")
    espnCfbCalendar = sportsdataverse.cfb.espn_cfb_calendar(season=2020, groups=80)
    print(espnCfbCalendar)


def getEspnSchedule():
    print("sportsdataverse.cfb.espn_cfb_schedule(dates=2020, week=5, season_type=2, groups=80)")
    print("cfb_schedule - look up the college football schedule for a given season")
    espnSchedule = sportsdataverse.cfb.espn_cfb_schedule(dates=2020, week=5, season_type=2, groups=80)
    print(espnSchedule)


def loadCfbPbp():
    print("sportsdataverse.cfb.load_cfb_pbp(seasons=range(2009,2010))")
    print("Load college football play by play data .")
    loadedCfbPbp = sportsdataverse.cfb.load_cfb_pbp(seasons=range(2009, 2010))
    print(loadedCfbPbp)


def loadCfbRosters():
    #################################
    #      Load roster data        ##
    #      Empty DataFrame         ##
    #      Columns: []             ##
    #      Index: []               ##
    #################################

    print("sportsdataverse.cfb.load_cfb_rosters(seasons=range(2014,2010))")
    print("Load roster data")
    loadedCfbRosters = cfb_df = sportsdataverse.cfb.load_cfb_rosters(seasons=range(2014, 2010))
    print(loadedCfbRosters)


def loadCfbSchedule():
    #################################
    #      Load roster data        ##
    #      Empty DataFrame         ##
    #      Columns: []             ##
    #      Index: []               ##
    #################################

    print("sportsdataverse.cfb.load_cfb_schedule(seasons=range(2009,2010))")
    print("Load college football schedule data")
    loadedCfbSchedule = sportsdataverse.cfb.load_cfb_schedule(seasons=range(2009, 2010))
    print(loadedCfbSchedule)


def loadCfbTeamInfo():
    print("sportsdataverse.cfb.load_cfb_team_info(seasons=range(2009,2010))")
    print("Load college football team info")
    loadedCfbTeamInfo = cfb_df = sportsdataverse.cfb.load_cfb_team_info(seasons=range(2009, 2010))
    print(loadedCfbTeamInfo)


######################################################################################################
##                                              MBB Secion                                          ##
######################################################################################################


def loadMbbPbp():
    print("sportsdataverse.mbb.load_mbb_pbp(seasons=range(2020,2022))")
    print("Load men’s college basketball play by play data going back to 2002")
    mbbPbp = sportsdataverse.mbb.load_mbb_pbp(seasons=range(2020, 2022))
    print(mbbPbp)


def loadMbbPlayerBoxscore():
    print("sportsdataverse.mbb.load_mbb_player_boxscore(seasons=range(2020,2022))")
    print("Load men’s college basketball player boxscore data")
    mbbPlayerBoxscore = sportsdataverse.mbb.load_mbb_player_boxscore(seasons=range(2020, 2022))
    print(mbbPlayerBoxscore)


def loadMbbSchedule():
    print("sportsdataverse.mbb.load_mbb_schedule(seasons=range(2020,2022))")
    print("Load men’s college basketball schedule data")
    mbbSchedule = sportsdataverse.mbb.load_mbb_schedule(seasons=range(2020, 2022))
    print(mbbSchedule)


def loadMbbTeamBoxscore():
    ##########################################
    ##      HTTP Error 404: Not Found       ##
    ##########################################

    print("sportsdataverse.mbb.load_mbb_team_boxscore(seasons=range(2002,2022))")
    print("Load men’s college basketball team boxscore data")
    mbbTeamBoxscore = sportsdataverse.mbb.load_mbb_team_boxscore(seasons=range(2002, 2022))
    print(mbbTeamBoxscore)


def loadEspnMbbPbp():
    print("sportsdataverse.mbb.espn_mbb_pbp(game_id=401265031)")
    print(
        "mbb_pbp() - Pull the game by id. Data from API endpoints: mens-college-basketball/playbyplay, mens-college-basketball/summary"
    )
    espnMbbPbp = sportsdataverse.mbb.espn_mbb_pbp(game_id=401265031)
    print(espnMbbPbp)


def loadEspnMbbSchedule():
    print("sportsdataverse.mbb.espn_mbb_schedule(dates=2020, groups=50, season_type=2)")
    print("mbb_schedule - look up the men’s college basketball scheduler for a given season")
    espnMbbSchedule = sportsdataverse.mbb.espn_mbb_schedule(dates=2020, groups=50, season_type=2)
    print(espnMbbSchedule)


######################################################################################################
##                                              MBB Secion                                          ##
######################################################################################################


def loadNbaPbp():
    print("sportsdataverse.nba.load_nba_pbp(seasons=range(2020,2022))")
    print("Load NBA play by play data going back to 2002")
    nbaPbp = sportsdataverse.nba.load_nba_pbp(seasons=range(2020, 2022))
    print(nbaPbp)


def loadNbaPlayerBoxscores():
    print("sportsdataverse.nba.load_nba_player_boxscore(seasons=range(2020,2022))")
    print("Load NBA player boxscore data")
    nbaBoxscore = sportsdataverse.nba.load_nba_player_boxscore(seasons=range(2020, 2022))
    print(nbaBoxscore)


def loadNbaSchedule():
    print("sportsdataverse.nba.load_nba_schedule(seasons=range(2020,2022))")
    print("Load NBA schedule data")
    nbaSchedule = sportsdataverse.nba.load_nba_schedule(seasons=range(2020, 2022))
    print(nbaSchedule)


def loadNbaTeamBoxscores():
    print("sportsdataverse.nba.load_nba_team_boxscore(seasons=range(2020,2022))")
    print("Load NBA team boxscore data")
    nbaTeamBoxscores = sportsdataverse.nba.load_nba_team_boxscore(seasons=range(2020, 2022))
    print(nbaTeamBoxscores)


def loadEspnNbaPbp():
    print("sportsdataverse.nba.espn_nba_pbp(game_id=401307514)")
    print("nba_pbp() - Pull the game by id - Data from API endpoints - nba/playbyplay, nba/summary")
    espnNbaPbp = sportsdataverse.nba.espn_nba_pbp(game_id=401307514)
    print(espnNbaPbp)


def loadEspnNbaCalendar():
    ##########################################
    ##      Does not work as advertised     ##
    ##########################################
    print("nba_calendar - look up the NBA calendar for a given season from ESPN")
    print("sportsdataverse.nba.espn_nba_schedule(dates=range(2020,2022), season_type=2)")
    espnNbaCalendar = sportsdataverse.nba.espn_nba_schedule(dates=range(2020, 2022), season_type=2)
    print(espnNbaCalendar)


######################################################################################################
##                                              NFL Secion                                          ##
######################################################################################################


def loadNflPbp():
    print("sportsdataverse.nfl.load_nfl_pbp(seasons=range(2019,2021))")
    print("Load NFL play by play data going back to 1999")
    nflPbp = sportsdataverse.nfl.load_nfl_pbp(seasons=range(2019, 2021))
    print(nflPbp)


def loadNflPlayerStats():
    print("sportsdataverse.nfl.load_nfl_player_stats()")
    print("Load NFL player stats data")
    nflPlayerStats = sportsdataverse.nfl.load_nfl_player_stats()
    print(nflPlayerStats)


def loadNflRosters():
    print("sportsdataverse.nfl.load_nfl_rosters()")
    print("Load NFL roster data for all seasons")
    nflRosters = sportsdataverse.nfl.load_nfl_rosters()
    print(nflRosters)


def loadNflSchedules():
    print("sportsdataverse.nfl.load_nfl_schedule(seasons=range(2019,2021))")
    print("Load NFL schedule data")
    nflSchedules = sportsdataverse.nfl.load_nfl_schedule(seasons=range(2019, 2021))
    print(nflSchedules)


def loadNflTeams():
    print("sportsdataverse.nfl.load_nfl_teams()")
    print("Load NFL team ID information and logos")
    nflTeams = sportsdataverse.nfl.load_nfl_teams()
    print(nflTeams)


def isNflPlayInProgress():
    ## Idk how this one is suppose to be implemented.
    ## TBD how saiemgilani wants this to be implemented
    print("")


def loadEspnNflPbp():
    ##########################################
    ##      Does not work as advertised     ##
    ##########################################

    print("sportsdataverse.nfl.NFLPlayProcess(game_id=401220403).espn_nfl_pbp()")
    print("espn_nfl_pbp() - Pull the game by id - Data from API endpoints - nfl/playbyplay, nfl/summary")
    espnNflPbp = sportsdataverse.nfl.NFLPlayProcess(game_id=401220403).espn_nfl_pbp()
    print(espnNflPbp)


def loadEspnNflCalendar():
    print("portsdataverse.nfl.espn_nfl_calendar(season=2020)")
    print("espn_nfl_calendar - look up the NFL calendar for a given season from ESPN")
    espnNflCalendar = sportsdataverse.nfl.espn_nfl_calendar(season=2020)
    print(espnNflCalendar)


def loadEspnNflSchedule():
    print("sportsdataverse.nfl.espn_nfl_schedule(dates=2020, week=1, season_type=2)")
    print("espn_nfl_schedule - look up the NFL schedule for a given date from ESPN")
    espnNflSchedule = sportsdataverse.nfl.espn_nfl_schedule(dates=2020, week=1, season_type=2)
    print(espnNflSchedule)


######################################################################################################
##                                              NFL Secion                                          ##
######################################################################################################


def loadNhlPbp():
    print("sportsdataverse.nhl.load_nhl_pbp(seasons=range(2019,2021))")
    print("Load NHL play by play data going back to 1999")
    nhlPbp = sportsdataverse.nhl.load_nhl_pbp(seasons=range(2019, 2021))
    print(nhlPbp)


def loadNhlPlayerStats():
    ######################################
    ##      name 'i' is not defined     ##
    ######################################
    print("")
    print("Load NHL player stats data")
    nhlPlayerStats = sportsdataverse.nhl.load_nhl_player_stats()
    print(nhlPlayerStats)


def loadNhlSchedules():
    print("sportsdataverse.nhl.load_nhl_schedule(seasons=range(2020,2021))")
    print("Load NHL schedule data")
    nhlSchedules = sportsdataverse.nhl.load_nhl_schedule(seasons=range(2020, 2021))
    print(nhlSchedules)


def loadEspnNhlTeamInfo():
    ##############################################################################
    ##      module 'sportsdataverse.nhl' has no attribute 'espn_nhl_teams'      ##
    ##############################################################################
    print("sportsdataverse.nhl.espn_nhl_teams()")
    print("Load NHL team ID information and logos")
    espnNhlTeamInfo = sportsdataverse.nhl.espn_nhl_teams()
    print(espnNhlTeamInfo)


def loadEspnNhlPbp():
    print("sportsdataverse.nhl.espn_nhl_pbp(game_id=401247153)")
    print("espn_nhl_pbp() - Pull the game by id. Data from API endpoints - nhl/playbyplay, nhl/summary")
    espnNhlPbp = sportsdataverse.nhl.espn_nhl_pbp(game_id=401247153)
    print(espnNhlPbp)


def loadEspnNhlCalendar():
    print("sportsdataverse.nhl.espn_nhl_calendar(season=2010)")
    print("espn_nhl_calendar - look up the NHL calendar for a given season")
    espnNhlCalendar = sportsdataverse.nhl.espn_nhl_calendar(season=2010)
    print(espnNhlCalendar)


def loadEspnNhlSchedule():
    print("sportsdataverse.nhl.espn_nhl_schedule(dates=2010, season_type=2)")
    print("espn_nhl_schedule - look up the NHL schedule for a given date")
    espnNhlSchedule = sportsdataverse.nhl.espn_nhl_schedule(dates=2010, season_type=2)
    print(espnNhlSchedule)


######################################################################################################
##                                              WBB Secion                                          ##
######################################################################################################


def loadWbbPbp():
    print("sportsdataverse.wbb.load_wbb_pbp(seasons=range(2020,2022))")
    print("Load women’s college basketball play by play data going back to 2002")
    wbbPbp = sportsdataverse.wbb.load_wbb_pbp(seasons=range(2020, 2022))
    print(wbbPbp)


def loadWbbPlayerBoxscores():
    print("sportsdataverse.wbb.load_wbb_player_boxscore(seasons=range(2020,2022))")
    print("Load women’s college basketball player boxscore data")
    wbbPlayerBoxscore = sportsdataverse.wbb.load_wbb_player_boxscore(seasons=range(2020, 2022))
    print(wbbPlayerBoxscore)


def loadWbbSchedule():
    print("sportsdataverse.wbb.load_wbb_schedule(seasons=range(2020,2022))")
    print("Load women’s college basketball schedule data")
    wbbSchedule = sportsdataverse.wbb.load_wbb_schedule(seasons=range(2020, 2022))
    print(wbbSchedule)


def loadWbbTeamBoxscores():
    print("sportsdataverse.wbb.load_wbb_team_boxscore(seasons=range(2020,2022))")
    print("Load women’s college basketball team boxscore data")
    wbbTeamBoxscores = sportsdataverse.wbb.load_wbb_team_boxscore(seasons=range(2020, 2022))
    print(wbbTeamBoxscores)


def loadWbbPbpModule():
    ##################################################################
    ##      There's nothing there, but the docs reference this.     ##
    ##      Idk how it's suppose to be implemented right now.       ##
    ##      ¯\_(ツ)_/¯                                              ##
    ##################################################################
    print("")


def loadEspnWbbPbp():
    print("sportsdataverse.wbb.espn_wbb_pbp(game_id=401266534)")
    print(
        "espn_wbb_pbp() - Pull the game by id. Data from API endpoints - womens-college-basketball/playbyplay, womens-college-basketball/summary"
    )
    espnWbbPbp = sportsdataverse.wbb.espn_wbb_pbp(game_id=401266534)
    print(espnWbbPbp)


def loadEspnWbbCalendar():
    print("sportsdataverse.wbb.espn_wbb_calendar(season=2020)")
    print("espn_wbb_calendar - look up the women’s college basketball calendar for a given season")
    espnWbbCalendar = sportsdataverse.wbb.espn_wbb_calendar(season=2020)
    print(espnWbbCalendar)


def loadEspnWbbSchedule():
    print("sportsdataverse.wbb.espn_wbb_schedule(dates=2020, groups=50, season_type=2)")
    print("espn_wbb_schedule - look up the women’s college basketball schedule for a given season")
    espnWbbSchedule = sportsdataverse.wbb.espn_wbb_schedule(dates=2020, groups=50, season_type=2)
    print(espnWbbSchedule)


#######################################################################################################
##                                              WNBA Secion                                          ##
#######################################################################################################


def loadWnbaPbp():
    print("sportsdataverse.wnba.load_wnba_pbp(seasons=range(2020,2022))")
    print("Load WNBA play by play data going back to 2002")
    wnba_df = sportsdataverse.wnba.load_wnba_pbp(seasons=range(2020, 2022))
    print(wnba_df)


def loadWnbaPlayerBoxscores():
    print("sportsdataverse.wnba.load_wnba_player_boxscore(seasons=range(2020,2022))")
    print("Load WNBA player boxscore data")
    wnbaPlayerBoxscore = sportsdataverse.wnba.load_wnba_player_boxscore(seasons=range(2020, 2022))
    print(wnbaPlayerBoxscore)


def loadWnbaSchedules():
    print("sportsdataverse.wnba.load_wnba_schedule(seasons=range(2020,2022))")
    print("Load WNBA schedule data")
    wnbaSchedules = sportsdataverse.wnba.load_wnba_schedule(seasons=range(2020, 2022))
    print(wnbaSchedules)


def loadWnbaTeamBoxscores():
    print("sportsdataverse.wnba.load_wnba_team_boxscore(seasons=range(2020,2022))")
    print("Load WNBA team boxscore data")
    wnbaTeamBoxscores = sportsdataverse.wnba.load_wnba_team_boxscore(seasons=range(2020, 2022))
    print(wnbaTeamBoxscores)


def loadEspnWnbaPbp():
    print("sportsdataverse.wnba.espn_wnba_pbp(game_id=401370395)")
    print("espn_wnba_pbp() - Pull the game by id. Data from API endpoints - wnba/playbyplay, wnba/summary")
    espnWnbaPbp = sportsdataverse.wnba.espn_wnba_pbp(game_id=401370395)
    print(espnWnbaPbp)


def loadEspnWnbaCalendar():
    print("sportsdataverse.wnba.espn_wnba_schedule(dates=2020, season_type=2)")
    print("espn_wnba_calendar - look up the WNBA calendar for a given season")
    espnWnbaCalendar = sportsdataverse.wnba.espn_wnba_calendar(season=2020)
    print(espnWnbaCalendar)


def loadEspnWnbaSchedule():
    print("sportsdataverse.wnba.espn_wnba_schedule(dates=2020, season_type=2)")
    print("espn_wnba_schedule - look up the WNBA schedule for a given season")
    espnWnbaSchedule = sportsdataverse.wnba.espn_wnba_schedule(dates=2020, season_type=2)
    print(espnWnbaSchedule)


def main():
    ## If it's commented out, it didn't work.

    print("starting up")

    # ## CFB
    getCfbEspnTeams()
    # getEspnPbp()
    # getCfbPbp()
    # getEspnCfbCalendar()
    # getEspnSchedule()
    # loadCfbPbp()
    # #loadCfbRosters()
    # loadCfbSchedule()
    # loadCfbTeamInfo()

    # ## MBB
    # loadMbbPbp()
    # loadMbbPlayerBoxscore()
    # loadMbbSchedule()
    # #loadMbbTeamBoxscore()
    # loadEspnMbbPbp()
    # loadEspnMbbSchedule()

    # # NBA
    # loadNbaPbp()
    # loadNbaPlayerBoxscores()
    # loadNbaSchedule()
    # loadNbaTeamBoxscores()
    # loadEspnNbaPbp()
    # #loadEspnNbaCalendar()

    # # NFL
    # loadNflPbp()
    # loadNflPlayerStats()
    # loadNflRosters()
    # loadNflSchedules()
    # loadNflTeams()
    # #isNflPlayInProgress()
    # #loadEspnNflPbp()
    # loadEspnNflCalendar()
    # loadEspnNflSchedule()

    # # NHL
    # loadNhlPbp()
    # #loadNhlPlayerStats()
    # loadNhlSchedules()
    # #loadEspnNhlTeamInfo()
    # loadEspnNhlPbp()
    # loadEspnNhlCalendar()
    # loadEspnNhlSchedule()

    # # WBB
    # loadWbbPbp()
    # loadWbbPlayerBoxscores()
    # loadWbbSchedule()
    # loadWbbTeamBoxscores()
    # #loadWbbPbpModule()
    # loadEspnWbbPbp()
    # loadEspnWbbCalendar()
    # loadEspnWbbSchedule()

    # # WNBA
    # loadWnbaPbp()
    # loadWnbaPlayerBoxscores()
    # loadWnbaSchedules()
    # loadWnbaTeamBoxscores()
    # loadEspnWnbaPbp()
    # loadEspnWnbaCalendar()
    # loadEspnWnbaSchedule()

    # Failed to work properly
    # getCfbEspnTeams()
    # loadCfbRosters()
    # loadMbbTeamBoxscore()
    # loadEspnNbaCalendar()
    # isNflPlayInProgress()
    # loadNhlPlayerStats()
    # loadEspnNhlTeamInfo()


if __name__ == "__main__":
    main()

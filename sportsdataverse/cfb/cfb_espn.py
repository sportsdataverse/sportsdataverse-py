## Author: Joseph Armstrong (armstrongjoseph08@gmail.com)
## File: cfb_espn.py
## Purpose: Adds the ability to get box score stats directly from the 
##          ESPN CFB API.


from sportsdataverse.dl_utils import download
import pandas as pd
import os
import json
from tqdm import tqdm
from datetime import datetime
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def espn_cfb_box_score(gameID:int) -> pd.DataFrame():
    """
    espn_cfb_box_score(gameID:int) -> pd.DataFrame()

    Retrives the JSON file corresponding to a real ESPN CFB gameID,
    and parses it into a pandas dataframe containing box score data
    from that game.

    Args:
        gameId(int): Required input. This should correspond to the 
        ESPN CFB gameID you want box score stats from.

    Returns:

        A pd.DataFrame() object. If there are box score stats for this gameID,
        you will recive a pandas dataframe with any box score stats that ESPN has
        for that gameID. If there isn't any box score stats for that gameID, or 
        the inputted gameID doesn't correspond to an actual gameID for ESPN's CFB API,
        the function will return an empty dataframe.

    Example:
        sdv.cfb.getEspnCfbBoxScore(401301018)
    """
    url = f"http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event={gameID}"
    try:
        resp = download(url)
        resp_str = str(resp,'UTF-8')
        resp_json = json.loads(resp_str)
        df = parse_espn_cfb_json(resp_json)
        return df
    except:
        print(f'Something went wrong when attempting to acces the JSON file for game #{gameID}.')
        return pd.DataFrame()


def parse_espn_cfb_json(espnFile:dict) -> pd.DataFrame():
    """
    parseEspnJson(espnFile:dict) -> pd.DataFrame()

    DO NOT CALL THIS FUNCTION DIRECTLY!
    This function is to be called by getEspnCfbBoxScore(), and parses the JSON file
    into a pandas dataframe
    
    """
    main_df = pd.DataFrame(columns=['Season','Season_Week','Game_ID','Game_Date','Team_Name','Team_ABV','Team_ID','Home_Away','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID','Player_ID','Player_Name'])
    s_df = pd.DataFrame()

    passing_df = pd.DataFrame()
    rushing_df = pd.DataFrame()
    receiving_df = pd.DataFrame()
    fumbles_df = pd.DataFrame()
    defensive_df = pd.DataFrame()
    interceptions_df = pd.DataFrame()
    kickReturns_df = pd.DataFrame()
    puntReturns_df = pd.DataFrame()
    kicking_df = pd.DataFrame()
    punting_df = pd.DataFrame()

    # with open(filename, 'r',encoding='utf8') as j:
    #     data = json.load(j)
    data = espnFile

    game_id = data['header']['id']
    game_date = str(data['header']['competitions'][0]['date'])
    game_date = game_date[:10]
    season = data['header']['season']['year']
    game_type = int(data['header']['season']['type'])

    if game_type == 3:
        season_week = 20
    else:
        season_week = data['header']['week']

    py_date = datetime.strptime(game_date,'%Y-%m-%d')

    #sdv_id = f"{py_date.year}_{py_date.month}_{py_date.day}_"
    sdv_id = py_date.strftime('%Y-%m-%d')
    try:
        is_neutral_site = data['header']['competitions'][0]['neutralSite']
        #print(is_neutral_site)
        #print(type(is_neutral_site))
        #is_neutral_site = str(is_neutral_site)
    except:
        is_neutral_site = False
    #print(is_neutral_site.dat)
    away_team_name = data['boxscore']['teams'][0]['team']['location']
    away_team_id = data['boxscore']['teams'][0]['team']['id']
    away_team_abv = data['boxscore']['teams'][0]['team']['abbreviation']

    home_team_name = data['boxscore']['teams'][1]['team']['location']
    home_team_id = data['boxscore']['teams'][1]['team']['id']
    home_team_abv = data['boxscore']['teams'][1]['team']['abbreviation']

    sdv_id = sdv_id + '_' + home_team_abv
    #print(game_id)
    #print(f'{away_team_name} vs {home_team_name}')

    #print(game_id,game_date)
    for i in data['boxscore']['players']:

        print('')
        team_name = i['team']['location']
        team_abv = i['team']['abbreviation']
        team_id = i['team']['id']

        #print(team_id,team_abv,team_name)
        for j in i['statistics']:
            stat_labels = j['labels']
            stat_type = j['name']

            for k in j['athletes']:
                try:
                    player_id = k['athlete']['id']
                except:
                    player_id = ""

                try:
                    player_name = k['athlete']['displayName']
                except:
                    player_name = ""
                stats = k['stats']
                s_df = pd.DataFrame(columns=stat_labels,data=[stats])
                s_df['Player_ID'] = player_id
                s_df['Player_Name'] = player_name
                s_df['Team_Name'] = team_name
                s_df['Team_ABV'] = team_abv
                s_df['Team_ID'] = team_id
                s_df['Game_ID'] = game_id
                s_df['Game_Date'] = game_date
                s_df['Season'] = season
                s_df['Season_Week'] = season_week

                if team_id == away_team_id:
                    s_df['Opp_Team_Name'] = home_team_name
                    s_df['Opp_Team_ABV'] = home_team_abv
                    s_df['Opp_Team_ID'] = home_team_id
                    if is_neutral_site == True:
                        s_df['Home_Away'] = 'A'
                    else:
                        s_df['Home_Away'] = '@'
                else:
                    s_df['Opp_Team_Name'] = away_team_name
                    s_df['Opp_Team_ABV'] = away_team_abv 
                    s_df['Opp_Team_ID'] = away_team_id
                    if is_neutral_site == True:
                        s_df['Home_Away'] = 'H'
                    else:
                        s_df['Home_Away'] = None
                #print(s_df)
                if stat_type == "passing":
                    passing_df = pd.concat([s_df,passing_df],ignore_index=True)
                elif stat_type == "rushing":
                    rushing_df = pd.concat([s_df,rushing_df],ignore_index=True)
                elif stat_type == "receiving":
                    receiving_df = pd.concat([s_df,receiving_df],ignore_index=True)
                elif stat_type == "fumbles":
                    fumbles_df = pd.concat([s_df,fumbles_df],ignore_index=True)
                elif stat_type == "defensive":
                    defensive_df = pd.concat([s_df,defensive_df],ignore_index=True)
                elif stat_type == "interceptions":
                    interceptions_df = pd.concat([s_df,interceptions_df],ignore_index=True)
                elif stat_type == "kickReturns":
                    kickReturns_df = pd.concat([s_df,kickReturns_df],ignore_index=True)
                elif stat_type == "puntReturns":
                    puntReturns_df = pd.concat([s_df,puntReturns_df],ignore_index=True)
                elif stat_type == "kicking":
                    kicking_df = pd.concat([s_df,kicking_df],ignore_index=True)
                elif stat_type == "punting":
                    punting_df = pd.concat([s_df,punting_df],ignore_index=True)
                else:
                    print(f'\n\n\n\nNew stat type found: \n{stat_type}\nNew stat type found:\n\n\n\n')

    pass_column_names = ['Season','Season_Week','Game_ID','Game_Date','Team_Name','Team_ABV','Team_ID','Home_Away','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID','Player_ID','Player_Name','COMP','ATT','COMP%','PASS YDS','YPA','PASS TD','PASS INT','CFBQBR','ESPN QBR','SACKS TAKEN','SACK YDS LOST']
    
    try:
        passing_df[['COMP','ATT']] = passing_df['C/ATT'].str.split('/',expand=True)
        passing_df.drop(['C/ATT'], axis=1,inplace=True)
    except:
        pass
    
    try:
        passing_df.rename(columns={'YDS':'PASS YDS','AVG':'YPA','TD':'PASS TD','INT':'PASS INT','QBR':'ESPN QBR'},inplace=True)
        passing_df[["COMP","ATT","PASS YDS","PASS TD","PASS INT"]] = passing_df[["COMP","ATT","PASS YDS","PASS TD","PASS INT"]].apply(pd.to_numeric)
        passing_df['COMP%'] = passing_df['COMP']/passing_df['ATT']
        passing_df['CFBQBR'] = ((passing_df['PASS YDS']*8.4)+(passing_df['PASS TD']*330)+(passing_df['COMP']*100)-(passing_df['PASS INT']*200))/passing_df['ATT']
        passing_df['ESPN QBR'] = passing_df['ESPN QBR'].replace(['--'],None)
    except:
        pass
    
    passing_df = passing_df.reindex(columns=pass_column_names)
    #print(passing_df)

    rush_column_names = ['Season','Season_Week','Game_ID','Game_Date','Team_Name','Team_ABV','Team_ID','Home_Away','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID','Player_ID','Player_Name','RUSH','RUSH YDS','RUSH AVG','RUSH TD','RUSH LONG',]
    
    try:
        rushing_df.rename(columns={'CAR':'RUSH','YDS':'RUSH YDS','AVG':'RUSH AVG','TD':'RUSH TD','LONG':'RUSH LONG'},inplace=True)
    except:
        pass
    
    rushing_df = rushing_df.reindex(columns=rush_column_names)
    #print(rushing_df)

    rec_column_names = ['Season','Season_Week','Game_ID','Game_Date','Team_Name','Team_ABV','Team_ID','Home_Away','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID','Player_ID','Player_Name','REC TARGETS','REC','REC YDS','REC AVG','REC TD','REC LONG','CATCH_RATE','DROPS','2PM']
    
    try:
        receiving_df.rename(columns={'YDS':'REC YDS','AVG':'REC AVG','TD':'REC TD','LONG':'REC LONG'},inplace=True)
    except:
        pass
    
    receiving_df = receiving_df.reindex(columns=rec_column_names)
    #print(receiving_df)

    fum_column_names = ['Season','Season_Week','Game_ID','Game_Date','Team_Name','Team_ABV','Team_ID','Home_Away','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID','Player_ID','Player_Name','FUMBLES','FUMBLES LOST','FF','FR','FR YDS']
    
    try:
        fumbles_df.rename(columns={'FUM':'FUMBLES','LOST':'FUMBLES LOST','REC':'FR'},inplace=True)
    except:
        pass

    fumbles_df = fumbles_df.reindex(columns=fum_column_names)
    #print(fumbles_df)
    
    
    #print(defensive_df)

    int_column_names = ['Season','Season_Week','Game_ID','Game_Date','Team_Name','Team_ABV','Team_ID','Home_Away','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID','Player_ID','Player_Name','INT','INT YDS','INT TD']
    
    try:
        interceptions_df.rename(columns={'YDS':'INT YDS','TD':'INT TD'},inplace=True)
    except:
        pass
    
    interceptions_df = interceptions_df.reindex(columns=int_column_names)
    #print(interceptions_df)

    kr_column_names = ['Season','Season_Week','Game_ID','Game_Date','Team_Name','Team_ABV','Team_ID','Home_Away','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID','Player_ID','Player_Name','KR','KR YDS','KR AVG','KR TD','KR LONG']
   
    try:
        kickReturns_df.rename(columns={'NO':'KR','YDS':'KR YDS','AVG':'KR AVG','LONG':'KR LONG','TD':'KR TD'},inplace=True)
    except:
        pass
    
    kickReturns_df = kickReturns_df.reindex(columns=kr_column_names)
    #print(kickReturns_df)

    pr_column_names = ['Season','Season_Week','Game_ID','Game_Date','Team_Name','Team_ABV','Team_ID','Home_Away','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID','Player_ID','Player_Name','PR','PR YDS','PR AVG','PR TD','PR LONG']
    
    try:
        puntReturns_df.rename(columns={'NO':'PR','YDS':'PR YDS','AVG':'PR AVG','LONG':'PR LONG','TD':'PR TD'},inplace=True)
    except:
        pass
    
    puntReturns_df = puntReturns_df.reindex(columns=pr_column_names)
    #print(puntReturns_df)

    FG_column_names = ['Season','Season_Week','Game_ID','Game_Date','Team_Name','Team_ABV','Team_ID','Home_Away','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID','Player_ID','Player_Name','XPM','XPA','XP%','FGM','FGA','FG%','FG LONG']
    
    try:
        kicking_df[['FGM','FGA']] = kicking_df['FG'].str.split('/',expand=True)
        kicking_df.drop(['FG'], axis=1,inplace=True)
        kicking_df[['FGM','FGA']] = kicking_df[['FGM','FGA']].apply(pd.to_numeric)
        kicking_df['FG%'] = kicking_df['FGM'] / kicking_df['FGA']
    except:
        pass
    
    try:
        kicking_df[['XPM','XPA']] = kicking_df['XP'].str.split('/',expand=True)
        kicking_df.drop(['XP'], axis=1,inplace=True)
        kicking_df[['XPM','XPA']] = kicking_df[['XPM','XPA']].apply(pd.to_numeric)
        kicking_df['XP%'] = kicking_df['XPM'] / kicking_df['XPA']
    except:
        pass
    
    try:
        kicking_df.rename(columns={'LONG':'FG LONG'},inplace=True)
    except:
        pass

    kicking_df = kicking_df.reindex(columns=FG_column_names)
    #print(kicking_df)

    punt_column_names = ['Season','Season_Week','Game_ID','Game_Date','Team_Name','Team_ABV','Team_ID','Home_Away','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID','Player_ID','Player_Name','PUNTS','GROSS PUNT YDS','GROSS PUNT AVG','NET PUNT YDS','NET PUNT AVG','PUNT TB','PUNTS IN 20','PUNT LONG']
    try:
        punting_df.rename(columns={'NO':'PUNTS','YDS':'GROSS PUNT YDS','AVG':'GROSS PUNT AVG','TB':'PUNT TB','In 20':'PUNTS IN 20','LONG':'PUNT LONG'},inplace=True)
    except:
        pass

    punting_df = punting_df.reindex(columns=punt_column_names)
    #print(punting_df)

    def_column_names = ['Season','Season_Week','Game_ID','Game_Date','Team_Name','Team_ABV','Team_ID','Home_Away','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID','Player_ID','Player_Name','TOTAL','SOLO','AST','TFL','QB HUR','QB HITS','SACKS','PD','FR TD','BLK','SAFETY']
    
    try:
        defensive_df[["TOT","SOLO"]] = defensive_df[["TOT","SOLO"]].apply(pd.to_numeric)
        defensive_df['AST'] = defensive_df['TOT'] - defensive_df['SOLO']
    except:
        pass
    
    #defensive_df['QB HITS']
    
    try:
        defensive_df.rename(columns={'TOT':'TOTAL','TD':'FR TD'},inplace=True)
    except:
        pass
    
    defensive_df = defensive_df.reindex(columns=def_column_names)

    #print('main_df')
    main_df = pd.concat([passing_df,main_df],ignore_index=True)
    if len(main_df) > 0:
        main_df = pd.merge(main_df,rushing_df, on=['Player_ID','Player_Name','Team_Name','Team_ABV','Team_ID','Game_ID','Game_Date','Season','Home_Away','Season_Week','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID'],how='outer')
        main_df = pd.merge(main_df,receiving_df, on=['Player_ID','Player_Name','Team_Name','Team_ABV','Team_ID','Game_ID','Game_Date','Season','Home_Away','Season_Week','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID'],how='outer')
        main_df = pd.merge(main_df,fumbles_df, on=['Player_ID','Player_Name','Team_Name','Team_ABV','Team_ID','Game_ID','Game_Date','Season','Home_Away','Season_Week','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID'],how='outer')
        main_df = pd.merge(main_df,defensive_df, on=['Player_ID','Player_Name','Team_Name','Team_ABV','Team_ID','Game_ID','Game_Date','Season','Home_Away','Season_Week','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID'],how='outer')
        main_df = pd.merge(main_df,interceptions_df, on=['Player_ID','Player_Name','Team_Name','Team_ABV','Team_ID','Game_ID','Game_Date','Season','Home_Away','Season_Week','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID'],how='outer')
        ## DEF TD (changed to 'FR TD') composes of both FR TD's and INT TD's
        ## The following code is used to split the two.
        try:
            main_df[['INT TD','FR TD']] = main_df[['INT TD','FR TD']].apply(pd.to_numeric)
            main_df['FR TD'] = main_df['FR TD'] - main_df['INT TD']
        except:
            pass
        main_df = pd.merge(main_df,puntReturns_df, on=['Player_ID','Player_Name','Team_Name','Team_ABV','Team_ID','Game_ID','Game_Date','Season','Home_Away','Season_Week','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID'],how='outer')

        main_df = pd.merge(main_df,kickReturns_df, on=['Player_ID','Player_Name','Team_Name','Team_ABV','Team_ID','Game_ID','Game_Date','Season','Home_Away','Season_Week','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID'],how='outer')
        main_df = pd.merge(main_df,kicking_df, on=['Player_ID','Player_Name','Team_Name','Team_ABV','Team_ID','Game_ID','Game_Date','Season','Home_Away','Season_Week','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID'],how='outer')
        main_df = pd.merge(main_df,punting_df, on=['Player_ID','Player_Name','Team_Name','Team_ABV','Team_ID','Game_ID','Game_Date','Season','Home_Away','Season_Week','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID'],how='outer')
        #main_df.to_csv('test.csv',index=False)
        #print(main_df)
        main_df['G'] = 1
        main_df['GS'] = 0
        main_df['sdv_id'] = sdv_id
        main_column_names = ['Season','Season_Week','Game_ID','sdv_id','Game_Date','Team_Name','Team_ABV','Team_ID','Home_Away','Opp_Team_Name','Opp_Team_ABV','Opp_Team_ID','Player_ID','Player_Name','G','GS','COMP','ATT','COMP%','PASS YDS','YPA','PASS TD','PASS INT','CFBQBR','ESPN QBR','SACKS TAKEN','SACK YDS LOST','RUSH','RUSH YDS','RUSH AVG','RUSH TD','RUSH LONG','REC TARGETS','REC','REC YDS','REC AVG','REC TD','REC LONG','CATCH_RATE','DROPS','2PM','FUMBLES','FUMBLES LOST','TOTAL','SOLO','AST','TFL','QB HUR','QB HITS','SACKS','INT','INT YDS','INT TD','PD','FF','FR','FR YDS','FR TD','BLK','SAFETY','XPM','XPA','XP%','FGM','FGA','FG%','FG LONG','PUNTS','GROSS PUNT YDS','GROSS PUNT AVG','NET PUNT YDS','NET PUNT AVG','PUNT TB','PUNTS IN 20','PUNT LONG','KO','KO YDS','KO AVG','KO LONG','KO TB','KO OB','PR','PR YDS','PR AVG','PR TD','PR LONG','KR','KR YDS','KR AVG','KR TD','KR LONG']
        main_df = main_df.reindex(columns=main_column_names)

        return main_df
    else:
        return main_df
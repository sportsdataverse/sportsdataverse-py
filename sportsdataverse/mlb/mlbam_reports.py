## Script: reports.py
## Author: Joseph Armstrong (armstjc)

import pandas as pd
from pandas import json_normalize
import json
from sportsdataverse.dl_utils import download
from datetime import datetime

import os




def getTransactionsInRange(startDate=0,endDate=0):
	'''
	Retrives all transactions in a given range of dates.
	You MUST provide two dates for this function to work, and both dates must 
	be in YYYYMMDD format. For example, December 31st, 2021 would be represented
	as 20211231

	Args:
	
	startDate (int):
		Required paramater. If no startDate is provided, the function wil not work.
		Additionally, startDate must be in YYYYMMDD format.

	endDate (int):
		Required paramater. If no endDate is provided, the function wil not work.
		Additionally, endDate must be in YYYYMMDD format.
	'''
	#pullCopyrightInfo()
	#p_df = pd.DataFrame()
	main_df = pd.DataFrame()
	
	searchURL = "http://lookup-service-prod.mlb.com/json/named.transaction_all.bam?sport_code='mlb'&"

	try:
		sd = int(startDate)
		ed = int(endDate)

		searchURL = searchURL + f'start_date=\'{sd}\''
		searchURL = searchURL + f'start_date=\'{ed}\''

		if (endDate - startDate) > 90:
			print('Getting transaction data. This will take some time.')
		else:
			print('Getting transaction data.')
	except:
		print('There\'s an issue with the way you\'ve formmatted you inputs.')

	if len(str(sd)) == 8 and len(str(ed)) == 8:
		resp = download(searchURL)

		#print(searchURL)
		resp_str = str(resp, 'UTF-8')
		#print(resp_str)

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['transaction_all']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:
			#print(resp_json['player_teams']['queryResults']['row'])

			print(f'{result_count} statlines found,\nParsing results into a dataframe.')
			#players = resp_json['search_player_all']['queryResults']['row']
			main_df = json_normalize(resp_json['transaction_all']['queryResults']['row']) 
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')
		
		return main_df
	else:
		print('Please format your dates as YYYYMMDD. \nFor example, December 31st, 2021 would be formatted as \n\n20211231\n')

def getBroadcastInfo(season=0,home_away="e",startDate=0,endDate=0):
	'''
	Retrives the broadcasters (radio and TV) involved with certian games.

	Args:
	
	season (int):
		Required paramater. If no season is provided, the function wil not work.

	home_away (string):
		Optional paramater. Used to get broadcasters from either the home OR the away side.
		Leave blank if you want both home and away broadcasters.

		If you want home broadcasters only, set home_away='H' or home_away='a'.

		If you want away broadcasters only, set home_away='A' or home_away='a'.

		If you want both home and away broadcasters, set home_away='E' or home_away='e'.
		
	startDate (int):
		Optional paramater. If no startDate is provided, 
		the function will get all broadcasters starting at the start of the given MLB season.
		Additionally, startDate must be in YYYYMMDD format. If it is not in that format,
		the function may not work properly.

	endDate (int):
		Optional paramater. If no endDate is provided, 
		the function will get all broadcasters until the end of the given MLB season.
		Additionally, endDate must be in YYYYMMDD format. If it is not in that format,
		the function may not work properly.
	'''
	#pullCopyrightInfo()
	#p_df = pd.DataFrame()
	main_df = pd.DataFrame()
	
	searchURL = "http://lookup-service-prod.mlb.com/json/named.mlb_broadcast_info.bam?tcid=mm_mlb_schedule&"

	if home_away.lower() == "a":
		searchURL = searchURL + '&home_away=\'A\''
	elif home_away.lower() == "h":
		searchURL = searchURL + '&home_away=\'H\''
	else:
		pass

	try:
		sd = int(startDate)
		ed = int(endDate)

		if len(str(sd)) == 8:
			searchURL = searchURL + f'start_date=\'{sd}\''
		else:
			pass

		if len(str(ed)) == 8:
			searchURL = searchURL + f'start_date=\'{ed}\''
		else:
			pass


		if (endDate - startDate) > 90:
			print('Getting broacaster info. This will take some time.')
		elif(endDate + startDate) == 0:
			print(f'Getting broacaster info. Since you\'re grabbing every broadcaster for every game in {season}, this will take some time.')
		else:
			print('Getting broadcaster info.')
	except:
		print('There\'s an issue with the way you\'ve formmatted you inputs.')
	now = datetime.now()

	if season >1860 and season < now.year:
		searchURL = searchURL + f'&season=\'{season}\''

		resp = download(searchURL)

		#print(searchURL)
		resp_str = str(resp, 'UTF-8')
		#print(resp_str)

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['mlb_broadcast_info']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:
			#print(resp_json['player_teams']['queryResults']['row'])

			print(f'{result_count} statlines found,\nParsing results into a dataframe.')
			#players = resp_json['search_player_all']['queryResults']['row']
			main_df = json_normalize(resp_json['mlb_broadcast_info']['queryResults']['row']) 
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')
		
		return main_df
	else:
		print('Please enter a valid year to use this function.')

##def getCurrentInjuries():
##	searchURL = 'http://lookup-service-prod.mlb.com/fantasylookup/json/json/named.wsfb_news_injury.bam'
##	resp = download(searchURL)

##	print(searchURL)
##	resp_str = str(resp, 'UTF-8')
##	#print(resp_str)

##	resp_json = json.loads(resp_str)
##	try:
##		result_count = int(resp_json['wsfb_news_injury']['queryResults']['totalSize'])
##	except:
##		result_count = 0

##	if result_count > 0:
##		#print(resp_json['player_teams']['queryResults']['row'])

##		print(f'{result_count} statlines found,\nParsing results into a dataframe.')
##		#players = resp_json['search_player_all']['queryResults']['row']
##		main_df = json_normalize(resp_json['wsfb_news_injury']['queryResults']['row']) 
##		print('Done')
##	else:
##		print(f'No results found for the provided playerID. \nTry a diffrient search for better results.')
		
##	return main_df
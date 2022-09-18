## Script: reports.py
## Author: Joseph Armstrong (armstjc)

import pandas as pd
from pandas import json_normalize
import json
from sportsdataverse.dl_utils import download
from datetime import datetime

def mlbam_transactions(startDate:str,endDate:str):
	"""
	Retrieves all transactions in a given range of dates.
	You MUST provide two dates for this function to work, and both dates must be in MM/DD/YYYY format.
	For example, December 31st, 2021 would be represented as 12/31/2021.

	Args:
		startDate (int):
			Required parameter. If no startDate is provided, the function wil not work.
			Additionally, startDate must be in MM/DD/YYYY format.
		endDate (int):
			Required parameter. If no endDate is provided, the function wil not work.
			Additionally, endDate must be in MM/DD/YYYY format.
	Returns:
		A pandas dataframe containing MLB transactions between two dates.
	"""
	main_df = pd.DataFrame()

	searchURL = "http://lookup-service-prod.mlb.com/json/named.transaction_all.bam?sport_code='mlb'&"

	try:
		sd_date = datetime.strptime(startDate, '%m/%d/%Y')
		ed_date = datetime.strptime(endDate, '%m/%d/%Y')
		sd = sd_date.strftime("%Y%m%d")
		ed = ed_date.strftime("%Y%m%d")

		if sd > ed:
			print('There is an issue with your inputted dates.\nPlease verify that your start date is older than your end date.')
			return None

		diff_days = ed_date.date() - sd_date.date()
		if (diff_days.days)> 30:
			print('Getting transaction data. This will take some time.')
		else:
			print('Getting transaction data.')
		searchURL = searchURL + f'start_date=\'{sd}\''
		searchURL = searchURL + f'start_date=\'{ed}\''

	except:
		print('There\'s an issue with the way you\'ve formatted you inputs.')

	try:
		resp = download(searchURL)

		resp_str = str(resp, 'UTF-8')

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['transaction_all']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:

			print(f'{result_count} statlines found,\nParsing results into a dataframe.')
			main_df = json_normalize(resp_json['transaction_all']['queryResults']['row'])
			print('Done')
		else:
			print(f'No results found for the provided playerID. \nTry a different search for better results.')
		return main_df
	except:
		print('Could not locate dates ')

def mlbam_broadcast_info(season:int,home_away="e"):
	"""
	Retrieves the broadcasters (radio and TV) involved with certain games.

	Args:
		season (int):
			Required parameter. If no season is provided, the function wil not work.

		home_away (string):
			Optional parameter. Used to get broadcasters from either the home OR the away side.
			Leave blank if you want both home and away broadcasters.

			If you want home broadcasters only, set home_away='H' or home_away='a'.

			If you want away broadcasters only, set home_away='A' or home_away='a'.

			If you want both home and away broadcasters, set home_away='E' or home_away='e'.

	Returns:
		A pandas dataframe containing TV and radio broadcast information for various MLB games.

	"""
	main_df = pd.DataFrame()

	searchURL = "http://lookup-service-prod.mlb.com/json/named.mlb_broadcast_info.bam?tcid=mm_mlb_schedule&"

	if home_away.lower() == "a":
		searchURL = searchURL + '&home_away=\'A\''
	elif home_away.lower() == "h":
		searchURL = searchURL + '&home_away=\'H\''
	elif home_away.lower() == "e":
		searchURL = searchURL + '&home_away=\'E\''
	else:
		pass

	now = datetime.now()

	if season >1860 and season < now.year:
		searchURL = searchURL + f'&season=\'{season}\''

		resp = download(searchURL)

		resp_str = str(resp, 'UTF-8')

		resp_json = json.loads(resp_str)
		try:
			result_count = int(resp_json['mlb_broadcast_info']['queryResults']['totalSize'])
		except:
			result_count = 0

		if result_count > 0:

			print(f'{result_count} statlines found,\nParsing results into a dataframe.')
			main_df = json_normalize(resp_json['mlb_broadcast_info']['queryResults']['row'])
			print('Done')
		else:
			print(f'No results found for the provided inputs. \nTry a different search for better results.')

		return main_df
	else:
		print('Please enter a valid year to use this function.')


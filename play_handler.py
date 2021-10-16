from numpy.core.fromnumeric import mean
import pandas as pd
import numpy as np
import xgboost as xgb
import os
import re
import json
import time
import http
import urllib
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap


    #---------------------------------
class PlayProcess(object):

    gameId = 0
    ran_pipeline = False
    ran_cleaning_pipeline = False
    path_to_json = '/'

    def __init__(self, gameId = 0, path_to_json = '/'):
        self.gameId = int(gameId)
        self.path_to_json = path_to_json

    def download(self, url, num_retries=5):
        try:
            html = urllib.request.urlopen(url).read()
            time.sleep(1)
        except (ConnectionResetError, URLError, HTTPError, ContentTooShortError, http.client.HTTPException, http.client.IncompleteRead) as e:
            print('Download error:', url)
            html = None
            if num_retries > 0:
                if hasattr(e, 'code') and 500 <= e.code < 600:
                    time.sleep(10)
                    # recursively retry 5xx HTTP errors
                    return self.download(url, num_retries - 1)
            if num_retries > 0:
                if e == http.client.IncompleteRead:
                    time.sleep(10)
                    return self.download(url, num_retries - 1)
        return html

    def flatten_json_iterative(self, dictionary, sep = '.', ind_start = 0):
        """Flattening a nested json file"""

        def unpack_one(parent_key, parent_value):
            """Unpack one level (only one) of nesting in json file"""
            # Unpacking one level
            if isinstance(parent_value, dict):
                for key, value in parent_value.items():
                    t1 = parent_key + sep + key
                    yield t1, value
            elif isinstance(parent_value, list):
                i = ind_start 
                for value in parent_value:
                    t2 = parent_key + sep +str(i) 
                    i += 1
                    yield t2, value
            else:
                yield parent_key, parent_value
        # Continue iterating the unpack_one function until the terminating condition is satisfied
        while True:
            # Continue unpacking the json file until all values are atomic elements (aka neither a dictionary nor a list)
            dictionary = dict(chain.from_iterable(starmap(unpack_one, dictionary.items())))
            # Terminating condition: none of the values in the json file are a dictionary or a list
            if not any(isinstance(value, dict) for value in dictionary.values()) and \
            not any(isinstance(value, list) for value in dictionary.values()):
                break
        return dictionary

    def mbb_pbp(self):
        """
            mbb_pbp()
            Pull the game by id
            Data from API endpoints:
                * mens-college-basketball/playbyplay
                * mens-college-basketball/summary
        """
        # play by play
        pbp_url = "http://cdn.espn.com/core/mens-college-basketball/playbyplay?gameId={}&xhr=1&render=false&userab=18".format(self.gameId)
        pbp_resp = self.download(url=pbp_url)
        pbp_txt = {}
        pbp_txt['scoringPlays'] = np.array([])
        pbp_txt['standings'] = np.array([])
        pbp_txt['videos'] = np.array([])
        pbp_txt['broadcasts'] = np.array([])
        pbp_txt['winprobability'] = np.array([])
        pbp_txt['espnWP'] = np.array([])
        pbp_txt['gameInfo'] = np.array([])
        pbp_txt['season'] = np.array([])

        pbp_txt = json.loads(pbp_resp)['gamepackageJSON']
        pbp_txt['odds'] = np.array([])
        pbp_txt['predictor'] = {}
        pbp_txt['againstTheSpread'] = np.array([])
        pbp_txt['pickcenter'] = np.array([])

        pbp_txt['timeouts'] = {}
        # summary endpoint for pickcenter array
        summary_url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={}".format(self.gameId)
        summary_resp = self.download(summary_url)
        summary = json.loads(summary_resp)
        summary_txt = summary['pickcenter']
        summary_ats = summary['againstTheSpread']
        summary_odds = summary['odds']
        if 'againstTheSpread' in summary.keys():
            summary_ats = summary['againstTheSpread']
        else:
            summary_ats = np.array([])
        if 'odds' in summary.keys():
            summary_odds = summary['odds']
        else:
            summary_odds = np.array([])
        if 'predictor' in summary.keys():
            summary_predictor = summary['predictor']
        else:
            summary_predictor = {}
        # ESPN's win probability
        wp = "winprobability"
        if wp in summary.keys():
            espnWP = summary["winprobability"]
        else:
            espnWP = np.array([])

        if 'news' in pbp_txt.keys():
            del pbp_txt['news']
        if 'shop' in pbp_txt.keys():
            del pbp_txt['shop']
        pbp_txt['gameInfo'] = pbp_txt['header']['competitions'][0]
        pbp_txt['season'] = pbp_txt['header']['season']
        pbp_txt['playByPlaySource'] = pbp_txt['header']['competitions'][0]['playByPlaySource']
        pbp_txt['pickcenter'] = summary_txt
        pbp_txt['againstTheSpread'] = summary_ats
        pbp_txt['odds'] = summary_odds
        pbp_txt['predictor'] = summary_predictor
        pbp_txt['espnWP'] = espnWP
        # Home and Away identification variables
        homeTeamId = int(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['id'])
        awayTeamId = int(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['id'])
        homeTeamMascot = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['name'])
        awayTeamMascot = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['name'])
        homeTeamName = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['location'])
        awayTeamName = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['location'])
        homeTeamAbbrev = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['abbreviation'])
        awayTeamAbbrev = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['abbreviation'])
        homeTeamNameAlt = re.sub("Stat(.+)", "St", str(homeTeamName))
        awayTeamNameAlt = re.sub("Stat(.+)", "St", str(awayTeamName))

        if len(pbp_txt['pickcenter']) > 1:
            if 'spread' in pbp_txt['pickcenter'][1].keys():
                gameSpread =  pbp_txt['pickcenter'][1]['spread']
                homeFavorite = pbp_txt['pickcenter'][1]['homeTeamOdds']['favorite']
                gameSpreadAvailable = True
            else:
                gameSpread =  pbp_txt['pickcenter'][0]['spread']
                homeFavorite = pbp_txt['pickcenter'][0]['homeTeamOdds']['favorite']
                gameSpreadAvailable = True

        else:
            gameSpread = 2.5
            homeFavorite = True
            gameSpreadAvailable = False

        if (pbp_txt['playByPlaySource'] != "none") & (len(pbp_txt['plays'])>1):
            pbp_txt['plays_mod'] = []
            for play in pbp_txt['plays']:
                p = self.flatten_json_iterative(play)
                pbp_txt['plays_mod'].append(p)
            pbp_txt['plays'] = pd.json_normalize(pbp_txt,'plays_mod')
            pbp_txt['plays']['season'] = pbp_txt['season']['year']
            pbp_txt['plays']['seasonType'] = pbp_txt['season']['type']
            pbp_txt['plays']['game_id'] = self.gameId
            pbp_txt['plays']["awayTeamId"] = awayTeamId
            pbp_txt['plays']["awayTeamName"] = str(awayTeamName)
            pbp_txt['plays']["awayTeamMascot"] = str(awayTeamMascot)
            pbp_txt['plays']["awayTeamAbbrev"] = str(awayTeamAbbrev)
            pbp_txt['plays']["awayTeamNameAlt"] = str(awayTeamNameAlt)
            pbp_txt['plays']["homeTeamId"] = homeTeamId
            pbp_txt['plays']["homeTeamName"] = str(homeTeamName)
            pbp_txt['plays']["homeTeamMascot"] = str(homeTeamMascot)
            pbp_txt['plays']["homeTeamAbbrev"] = str(homeTeamAbbrev)
            pbp_txt['plays']["homeTeamNameAlt"] = str(homeTeamNameAlt)
            # Spread definition
            pbp_txt['plays']["homeTeamSpread"] = 2.5
            pbp_txt['plays']["gameSpread"] = abs(gameSpread)
            pbp_txt['plays']["homeTeamSpread"] = np.where(homeFavorite == True, abs(gameSpread), -1*abs(gameSpread))
            pbp_txt['homeTeamSpread'] = np.where(homeFavorite == True, abs(gameSpread), -1*abs(gameSpread))
            pbp_txt['plays']["homeFavorite"] = homeFavorite
            pbp_txt['plays']["gameSpread"] = gameSpread
            pbp_txt['plays']["gameSpreadAvailable"] = gameSpreadAvailable
            pbp_txt['plays'] = pbp_txt['plays'].to_dict(orient='records')
            pbp_txt['plays'] = pd.DataFrame(pbp_txt['plays'])
            pbp_txt['plays']['season'] = pbp_txt['header']['season']['year']
            pbp_txt['plays']['seasonType'] = pbp_txt['header']['season']['type']
            pbp_txt['plays']['game_id'] = int(self.gameId)
            pbp_txt['plays']["homeTeamId"] = homeTeamId
            pbp_txt['plays']["awayTeamId"] = awayTeamId
            pbp_txt['plays']["homeTeamName"] = str(homeTeamName)
            pbp_txt['plays']["awayTeamName"] = str(awayTeamName)
            pbp_txt['plays']["homeTeamMascot"] = str(homeTeamMascot)
            pbp_txt['plays']["awayTeamMascot"] = str(awayTeamMascot)
            pbp_txt['plays']["homeTeamAbbrev"] = str(homeTeamAbbrev)
            pbp_txt['plays']["awayTeamAbbrev"] = str(awayTeamAbbrev)
            pbp_txt['plays']["homeTeamNameAlt"] = str(homeTeamNameAlt)
            pbp_txt['plays']["awayTeamNameAlt"] = str(awayTeamNameAlt)
            pbp_txt['plays']['period.number'] = pbp_txt['plays']['period.number'].apply(lambda x: int(x))
            pbp_txt['plays']['qtr'] = pbp_txt['plays']['period.number'].apply(lambda x: int(x))

            

            pbp_txt['plays']["homeTeamSpread"] = 2.5
            if len(pbp_txt['pickcenter']) > 1:
                if 'spread' in pbp_txt['pickcenter'][1].keys():
                    gameSpread =  pbp_txt['pickcenter'][1]['spread']
                    homeFavorite = pbp_txt['pickcenter'][1]['homeTeamOdds']['favorite']
                    gameSpreadAvailable = True
                else:
                    gameSpread =  pbp_txt['pickcenter'][0]['spread']
                    homeFavorite = pbp_txt['pickcenter'][0]['homeTeamOdds']['favorite']
                    gameSpreadAvailable = True

            else:
                gameSpread = 2.5
                homeFavorite = True
                gameSpreadAvailable = False
            pbp_txt['plays']["gameSpread"] = abs(gameSpread)
            pbp_txt['plays']["gameSpreadAvailable"] = gameSpreadAvailable
            pbp_txt['plays']["homeTeamSpread"] = np.where(homeFavorite == True, abs(gameSpread), -1*abs(gameSpread))
            pbp_txt['homeTeamSpread'] = np.where(homeFavorite == True, abs(gameSpread), -1*abs(gameSpread))
            pbp_txt['plays']["homeFavorite"] = homeFavorite
            pbp_txt['plays']["gameSpread"] = gameSpread
            pbp_txt['plays']["homeFavorite"] = homeFavorite

            #----- Time ---------------
            pbp_txt['plays']['time'] = pbp_txt['plays']['clock.displayValue']
            pbp_txt['plays']['clock.mm'] = pbp_txt['plays']['clock.displayValue'].str.split(pat=':')
            pbp_txt['plays'][['clock.minutes','clock.seconds']] = pbp_txt['plays']['clock.mm'].to_list()
            pbp_txt['plays']['half'] = np.where(pbp_txt['plays']['qtr'] <= 2, "1","2")
            pbp_txt['plays']['game_half'] = np.where(pbp_txt['plays']['qtr'] <= 2, "1","2")
            pbp_txt['plays']['lag_qtr'] = pbp_txt['plays']['qtr'].shift(1)
            pbp_txt['plays']['lead_qtr'] = pbp_txt['plays']['qtr'].shift(-1)
            pbp_txt['plays']['lag_game_half'] = pbp_txt['plays']['game_half'].shift(1)
            pbp_txt['plays']['lead_game_half'] = pbp_txt['plays']['game_half'].shift(-1)
            pbp_txt['plays']['start.quarter_seconds_remaining'] = 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
            pbp_txt['plays']['start.half_seconds_remaining'] = np.where(
                pbp_txt['plays']['qtr'].isin([1,3]),
                600 + 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int),
                60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
            )
            pbp_txt['plays']['start.game_seconds_remaining'] = np.select(
                [
                    pbp_txt['plays']['qtr'] == 1,
                    pbp_txt['plays']['qtr'] == 2,
                    pbp_txt['plays']['qtr'] == 3,
                    pbp_txt['plays']['qtr'] == 4
                ],
                [
                    1800 + 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int),
                    1200 + 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int),
                    600 + 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int),
                    60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
                ], default = 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
            )
            # Pos Team - Start and End Id
            pbp_txt['plays']['game_play_number'] = np.arange(len(pbp_txt['plays']))+1
            pbp_txt['plays']['text'] = pbp_txt['plays']['text'].astype(str)
            pbp_txt['plays']['id'] = pbp_txt['plays']['id'].apply(lambda x: int(x))
            pbp_txt['plays']['end.quarter_seconds_remaining'] = pbp_txt['plays']['start.quarter_seconds_remaining'].shift(1)
            pbp_txt['plays']['end.half_seconds_remaining'] = pbp_txt['plays']['start.half_seconds_remaining'].shift(1)
            pbp_txt['plays']['end.game_seconds_remaining'] = pbp_txt['plays']['start.game_seconds_remaining'].shift(1)
            pbp_txt['plays']['end.quarter_seconds_remaining'] = np.select(
                [
                    (pbp_txt['plays']['game_play_number'] == 1)|
                    ((pbp_txt['plays']['qtr'] == 2) & (pbp_txt['plays']['lag_qtr'] == 1))|
                    ((pbp_txt['plays']['qtr'] == 3) & (pbp_txt['plays']['lag_qtr'] == 2))|
                    ((pbp_txt['plays']['qtr'] == 4) & (pbp_txt['plays']['lag_qtr'] == 3))
                ],
                [
                    600
                ], default = pbp_txt['plays']['end.quarter_seconds_remaining']
            )
            pbp_txt['plays']['end.half_seconds_remaining'] = np.select(
                [
                    (pbp_txt['plays']['game_play_number'] == 1)|
                    ((pbp_txt['plays']['game_half'] == "2") & (pbp_txt['plays']['lag_game_half'] == "1"))
                ],
                [
                    1200
                ], default = pbp_txt['plays']['end.half_seconds_remaining']
            )
            pbp_txt['plays']['end.game_seconds_remaining'] = np.select(
                [
                    (pbp_txt['plays']['game_play_number'] == 1),
                    ((pbp_txt['plays']['game_half'] == "2") & (pbp_txt['plays']['lag_game_half'] == "1"))
                ],
                [
                    2400,
                    1200
                ], default = pbp_txt['plays']['end.game_seconds_remaining']
            )

            pbp_txt['plays']['period'] = pbp_txt['plays']['qtr']

            del pbp_txt['plays']['clock.mm']
        else:
            pbp_txt['plays'] = pd.DataFrame()
            pbp_txt['timeouts'] = {}
            pbp_txt['timeouts'][homeTeamId] = {"1": [], "2": []}
            pbp_txt['timeouts'][awayTeamId] = {"1": [], "2": []}
        if 'scoringPlays' not in pbp_txt.keys():
            pbp_txt['scoringPlays'] = np.array([])
        if 'winprobability' not in pbp_txt.keys():
            pbp_txt['winprobability'] = np.array([])
        if 'standings' not in pbp_txt.keys():
            pbp_txt['standings'] = np.array([])
        if 'videos' not in pbp_txt.keys():
            pbp_txt['videos'] = np.array([])
        if 'broadcasts' not in pbp_txt.keys():
            pbp_txt['broadcasts'] = np.array([])
        pbp_txt['plays'] = pbp_txt['plays'].replace({np.nan: None})
        self.plays_json = pbp_txt['plays']
        pbp_json = {
            "gameId": self.gameId,
            "plays" : pbp_txt['plays'].to_dict(orient='records'),
            "winprobability" : np.array(pbp_txt['winprobability']).tolist(),
            "boxscore" : pbp_txt['boxscore'],
            "header" : pbp_txt['header'],
            # "homeTeamSpread" : np.array(pbp_txt['homeTeamSpread']).tolist(),
            "broadcasts" : np.array(pbp_txt['broadcasts']).tolist(),
            "videos" : np.array(pbp_txt['videos']).tolist(),
            "playByPlaySource": pbp_txt['playByPlaySource'],
            "standings" : pbp_txt['standings'],
            "timeouts" : pbp_txt['timeouts'],
            "pickcenter" : np.array(pbp_txt['pickcenter']).tolist(),
            "againstTheSpread" : np.array(pbp_txt['againstTheSpread']).tolist(),
            "odds" : np.array(pbp_txt['odds']).tolist(),
            "predictor" : pbp_txt['predictor'],
            "espnWP" : np.array(pbp_txt['espnWP']).tolist(),
            "gameInfo" : np.array(pbp_txt['gameInfo']).tolist(),
            "season" : np.array(pbp_txt['season']).tolist()
        }
        return pbp_json

    def mbb_pbp_disk(self):
        with open(os.path.join(self.path_to_json, "{}.json".format(self.gameId))) as json_file:
            json_text = json.load(json_file)
            self.plays_json = pd.json_normalize(json_text,'plays')
        return json_text

    def nba_pbp(self):
        """
            nba_pbp()
            Pull the game by id
            Data from API endpoints:
                * nba/playbyplay
                * nba/summary
        """
        # play by play
        pbp_url = "http://cdn.espn.com/core/nba/playbyplay?gameId={}&xhr=1&render=false&userab=18".format(self.gameId)
        pbp_resp = self.download(url=pbp_url)
        pbp_txt = {}
        pbp_txt['scoringPlays'] = np.array([])
        pbp_txt['standings'] = np.array([])
        pbp_txt['videos'] = np.array([])
        pbp_txt['broadcasts'] = np.array([])
        pbp_txt['winprobability'] = np.array([])
        pbp_txt['espnWP'] = np.array([])
        pbp_txt['gameInfo'] = np.array([])
        pbp_txt['season'] = np.array([])

        pbp_txt = json.loads(pbp_resp)['gamepackageJSON']
        pbp_txt['odds'] = np.array([])
        pbp_txt['predictor'] = {}
        pbp_txt['againstTheSpread'] = np.array([])
        pbp_txt['pickcenter'] = np.array([])

        pbp_txt['timeouts'] = {}
        # summary endpoint for pickcenter array
        summary_url = "http://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={}".format(self.gameId)
        summary_resp = self.download(summary_url)
        summary = json.loads(summary_resp)
        summary_txt = summary['pickcenter']
        summary_ats = summary['againstTheSpread']
        summary_odds = summary['odds']
        if 'againstTheSpread' in summary.keys():
            summary_ats = summary['againstTheSpread']
        else:
            summary_ats = np.array([])
        if 'odds' in summary.keys():
            summary_odds = summary['odds']
        else:
            summary_odds = np.array([])
        if 'predictor' in summary.keys():
            summary_predictor = summary['predictor']
        else:
            summary_predictor = {}
        if 'seasonseries' in summary.keys():
            summary_seasonseries = summary['seasonseries']
        else:
            summary_seasonseries = np.array([])
        if 'leaders' in summary.keys():
            summary_leaders = summary['leaders']
        else:
            summary_leaders = np.array([])
        # ESPN's win probability
        wp = "winprobability"
        if wp in summary.keys():
            espnWP = summary["winprobability"]
        else:
            espnWP = np.array([])

        if 'news' in pbp_txt.keys():
            del pbp_txt['news']
        if 'shop' in pbp_txt.keys():
            del pbp_txt['shop']
        pbp_txt['gameInfo'] = pbp_txt['header']['competitions'][0]
        pbp_txt['season'] = pbp_txt['header']['season']
        pbp_txt['playByPlaySource'] = pbp_txt['header']['competitions'][0]['playByPlaySource']
        pbp_txt['pickcenter'] = summary_txt
        pbp_txt['againstTheSpread'] = summary_ats
        pbp_txt['odds'] = summary_odds
        pbp_txt['predictor'] = summary_predictor
        pbp_txt['seasonseries'] = summary_seasonseries
        pbp_txt['leaders'] = summary_leaders
        pbp_txt['espnWP'] = espnWP
        # Home and Away identification variables
        homeTeamId = int(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['id'])
        awayTeamId = int(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['id'])
        homeTeamMascot = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['name'])
        awayTeamMascot = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['name'])
        homeTeamName = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['location'])
        awayTeamName = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['location'])
        homeTeamAbbrev = str(pbp_txt['header']['competitions'][0]['competitors'][0]['team']['abbreviation'])
        awayTeamAbbrev = str(pbp_txt['header']['competitions'][0]['competitors'][1]['team']['abbreviation'])
        homeTeamNameAlt = re.sub("Stat(.+)", "St", str(homeTeamName))
        awayTeamNameAlt = re.sub("Stat(.+)", "St", str(awayTeamName))

        if len(pbp_txt['pickcenter']) > 1:
            if 'spread' in pbp_txt['pickcenter'][1].keys():
                gameSpread =  pbp_txt['pickcenter'][1]['spread']
                homeFavorite = pbp_txt['pickcenter'][1]['homeTeamOdds']['favorite']
                gameSpreadAvailable = True
            else:
                gameSpread =  pbp_txt['pickcenter'][0]['spread']
                homeFavorite = pbp_txt['pickcenter'][0]['homeTeamOdds']['favorite']
                gameSpreadAvailable = True

        else:
            gameSpread = 2.5
            homeFavorite = True
            gameSpreadAvailable = False

        if (pbp_txt['playByPlaySource'] != "none") & (len(pbp_txt['plays'])>1):
            pbp_txt['plays_mod'] = []
            for play in pbp_txt['plays']:
                p = self.flatten_json_iterative(play)
                pbp_txt['plays_mod'].append(p)
            pbp_txt['plays'] = pd.json_normalize(pbp_txt,'plays_mod')
            pbp_txt['plays']['season'] = pbp_txt['season']['year']
            pbp_txt['plays']['seasonType'] = pbp_txt['season']['type']
            pbp_txt['plays']['game_id'] = self.gameId 
            pbp_txt['plays']["awayTeamId"] = awayTeamId
            pbp_txt['plays']["awayTeamName"] = str(awayTeamName)
            pbp_txt['plays']["awayTeamMascot"] = str(awayTeamMascot)
            pbp_txt['plays']["awayTeamAbbrev"] = str(awayTeamAbbrev)
            pbp_txt['plays']["awayTeamNameAlt"] = str(awayTeamNameAlt)
            pbp_txt['plays']["homeTeamId"] = homeTeamId
            pbp_txt['plays']["homeTeamName"] = str(homeTeamName)
            pbp_txt['plays']["homeTeamMascot"] = str(homeTeamMascot)
            pbp_txt['plays']["homeTeamAbbrev"] = str(homeTeamAbbrev)
            pbp_txt['plays']["homeTeamNameAlt"] = str(homeTeamNameAlt)
            # Spread definition
            pbp_txt['plays']["homeTeamSpread"] = 2.5
            pbp_txt['plays']["gameSpread"] = abs(gameSpread)
            pbp_txt['plays']["homeTeamSpread"] = np.where(homeFavorite == True, abs(gameSpread), -1*abs(gameSpread))
            pbp_txt['homeTeamSpread'] = np.where(homeFavorite == True, abs(gameSpread), -1*abs(gameSpread))
            pbp_txt['plays']["homeFavorite"] = homeFavorite
            pbp_txt['plays']["gameSpread"] = gameSpread
            pbp_txt['plays']["gameSpreadAvailable"] = gameSpreadAvailable
            pbp_txt['plays'] = pbp_txt['plays'].to_dict(orient='records')
            pbp_txt['plays'] = pd.DataFrame(pbp_txt['plays'])
            pbp_txt['plays']['season'] = pbp_txt['header']['season']['year']
            pbp_txt['plays']['seasonType'] = pbp_txt['header']['season']['type']
            pbp_txt['plays']['game_id'] = int(self.gameId)
            pbp_txt['plays']["homeTeamId"] = homeTeamId
            pbp_txt['plays']["awayTeamId"] = awayTeamId
            pbp_txt['plays']["homeTeamName"] = str(homeTeamName)
            pbp_txt['plays']["awayTeamName"] = str(awayTeamName)
            pbp_txt['plays']["homeTeamMascot"] = str(homeTeamMascot)
            pbp_txt['plays']["awayTeamMascot"] = str(awayTeamMascot)
            pbp_txt['plays']["homeTeamAbbrev"] = str(homeTeamAbbrev)
            pbp_txt['plays']["awayTeamAbbrev"] = str(awayTeamAbbrev)
            pbp_txt['plays']["homeTeamNameAlt"] = str(homeTeamNameAlt)
            pbp_txt['plays']["awayTeamNameAlt"] = str(awayTeamNameAlt)
            pbp_txt['plays']['period.number'] = pbp_txt['plays']['period.number'].apply(lambda x: int(x))
            pbp_txt['plays']['qtr'] = pbp_txt['plays']['period.number'].apply(lambda x: int(x))


            pbp_txt['plays']["homeTeamSpread"] = 2.5
            if len(pbp_txt['pickcenter']) > 1:
                if 'spread' in pbp_txt['pickcenter'][1].keys():
                    gameSpread =  pbp_txt['pickcenter'][1]['spread']
                    homeFavorite = pbp_txt['pickcenter'][1]['homeTeamOdds']['favorite']
                    gameSpreadAvailable = True
                else:
                    gameSpread =  pbp_txt['pickcenter'][0]['spread']
                    homeFavorite = pbp_txt['pickcenter'][0]['homeTeamOdds']['favorite']
                    gameSpreadAvailable = True

            else:
                gameSpread = 2.5
                homeFavorite = True
                gameSpreadAvailable = False
            pbp_txt['plays']["gameSpread"] = abs(gameSpread)
            pbp_txt['plays']["gameSpreadAvailable"] = gameSpreadAvailable
            pbp_txt['plays']["homeTeamSpread"] = np.where(homeFavorite == True, abs(gameSpread), -1*abs(gameSpread))
            pbp_txt['homeTeamSpread'] = np.where(homeFavorite == True, abs(gameSpread), -1*abs(gameSpread))
            pbp_txt['plays']["homeFavorite"] = homeFavorite
            pbp_txt['plays']["gameSpread"] = gameSpread
            pbp_txt['plays']["homeFavorite"] = homeFavorite

            #----- Time ---------------

            pbp_txt['plays']['clock.displayValue'] = np.select(
                [
                    pbp_txt['plays']['clock.displayValue'].str.contains(":") == False
                ],
                [
                    "0:" + pbp_txt['plays']['clock.displayValue'].apply(lambda x: str(x))
                ], default = pbp_txt['plays']['clock.displayValue']
            )
            pbp_txt['plays']['time'] = pbp_txt['plays']['clock.displayValue']
            pbp_txt['plays']['clock.mm'] = pbp_txt['plays']['clock.displayValue'].str.split(pat=':')
            pbp_txt['plays'][['clock.minutes','clock.seconds']] = pbp_txt['plays']['clock.mm'].to_list()            
            pbp_txt['plays']['clock.minutes'] = pbp_txt['plays']['clock.minutes'].apply(lambda x: int(x))

            pbp_txt['plays']['clock.seconds'] = pbp_txt['plays']['clock.seconds'].apply(lambda x: float(x))
            # pbp_txt['plays']['clock.mm'] = pbp_txt['plays']['clock.displayValue'].apply(lambda x: datetime.strptime(str(x),'%M:%S'))
            pbp_txt['plays']['half'] = np.where(pbp_txt['plays']['qtr'] <= 2, "1","2")
            pbp_txt['plays']['game_half'] = np.where(pbp_txt['plays']['qtr'] <= 2, "1","2")
            pbp_txt['plays']['lag_qtr'] = pbp_txt['plays']['qtr'].shift(1)
            pbp_txt['plays']['lead_qtr'] = pbp_txt['plays']['qtr'].shift(-1)
            pbp_txt['plays']['lag_game_half'] = pbp_txt['plays']['game_half'].shift(1)
            pbp_txt['plays']['lead_game_half'] = pbp_txt['plays']['game_half'].shift(-1)
            pbp_txt['plays']['start.quarter_seconds_remaining'] = 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
            pbp_txt['plays']['start.half_seconds_remaining'] = np.where(
                pbp_txt['plays']['qtr'].isin([1,3]),
                600 + 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int),
                60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
            )
            pbp_txt['plays']['start.game_seconds_remaining'] = np.select(
                [
                    pbp_txt['plays']['qtr'] == 1,
                    pbp_txt['plays']['qtr'] == 2,
                    pbp_txt['plays']['qtr'] == 3,
                    pbp_txt['plays']['qtr'] == 4
                ],
                [
                    1800 + 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int),
                    1200 + 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int),
                    600 + 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int),
                    60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
                ], default = 60*pbp_txt['plays']['clock.minutes'].astype(int) + pbp_txt['plays']['clock.seconds'].astype(int)
            )
            # Pos Team - Start and End Id
            pbp_txt['plays']['game_play_number'] = np.arange(len(pbp_txt['plays']))+1
            pbp_txt['plays']['text'] = pbp_txt['plays']['text'].astype(str)
            pbp_txt['plays']['id'] = pbp_txt['plays']['id'].apply(lambda x: int(x))
            pbp_txt['plays']['end.quarter_seconds_remaining'] = pbp_txt['plays']['start.quarter_seconds_remaining'].shift(1)
            pbp_txt['plays']['end.half_seconds_remaining'] = pbp_txt['plays']['start.half_seconds_remaining'].shift(1)
            pbp_txt['plays']['end.game_seconds_remaining'] = pbp_txt['plays']['start.game_seconds_remaining'].shift(1)
            pbp_txt['plays']['end.quarter_seconds_remaining'] = np.select(
                [
                    (pbp_txt['plays']['game_play_number'] == 1)|
                    ((pbp_txt['plays']['qtr'] == 2) & (pbp_txt['plays']['lag_qtr'] == 1))|
                    ((pbp_txt['plays']['qtr'] == 3) & (pbp_txt['plays']['lag_qtr'] == 2))|
                    ((pbp_txt['plays']['qtr'] == 4) & (pbp_txt['plays']['lag_qtr'] == 3))
                ],
                [
                    600
                ], default = pbp_txt['plays']['end.quarter_seconds_remaining']
            )
            pbp_txt['plays']['end.half_seconds_remaining'] = np.select(
                [
                    (pbp_txt['plays']['game_play_number'] == 1)|
                    ((pbp_txt['plays']['game_half'] == "2") & (pbp_txt['plays']['lag_game_half'] == "1"))
                ],
                [
                    1200
                ], default = pbp_txt['plays']['end.half_seconds_remaining']
            )
            pbp_txt['plays']['end.game_seconds_remaining'] = np.select(
                [
                    (pbp_txt['plays']['game_play_number'] == 1),
                    ((pbp_txt['plays']['game_half'] == "2") & (pbp_txt['plays']['lag_game_half'] == "1"))
                ],
                [
                    2400,
                    1200
                ], default = pbp_txt['plays']['end.game_seconds_remaining']
            )

            pbp_txt['plays']['period'] = pbp_txt['plays']['qtr']

            del pbp_txt['plays']['clock.mm']
        else:
            pbp_txt['plays'] = pd.DataFrame()
        if 'scoringPlays' not in pbp_txt.keys():
            pbp_txt['scoringPlays'] = np.array([])
        if 'winprobability' not in pbp_txt.keys():
            pbp_txt['winprobability'] = np.array([])
        if 'standings' not in pbp_txt.keys():
            pbp_txt['standings'] = np.array([])
        if 'videos' not in pbp_txt.keys():
            pbp_txt['videos'] = np.array([])
        if 'broadcasts' not in pbp_txt.keys():
            pbp_txt['broadcasts'] = np.array([])
        pbp_txt['plays'] = pbp_txt['plays'].replace({np.nan: None})
        self.plays_json = pbp_txt['plays']
        pbp_json = {
            "gameId": self.gameId,
            "plays" : pbp_txt['plays'].to_dict(orient='records'),
            "winprobability" : np.array(pbp_txt['winprobability']).tolist(),
            "boxscore" : pbp_txt['boxscore'],
            "header" : pbp_txt['header'],
            # "homeTeamSpread" : np.array(pbp_txt['homeTeamSpread']).tolist(),
            "broadcasts" : np.array(pbp_txt['broadcasts']).tolist(),
            "videos" : np.array(pbp_txt['videos']).tolist(),
            "playByPlaySource": pbp_txt['playByPlaySource'],
            "standings" : pbp_txt['standings'],
            "seasonseries" : np.array(pbp_txt['seasonseries']).tolist(),
            "timeouts" : pbp_txt['timeouts'],
            "pickcenter" : np.array(pbp_txt['pickcenter']).tolist(),
            "againstTheSpread" : np.array(pbp_txt['againstTheSpread']).tolist(),
            "odds" : np.array(pbp_txt['odds']).tolist(),
            "predictor" : pbp_txt['predictor'],
            "espnWP" : np.array(pbp_txt['espnWP']).tolist(),
            "gameInfo" : np.array(pbp_txt['gameInfo']).tolist(),
            "season" : np.array(pbp_txt['season']).tolist()
        }
        return pbp_json

    def nba_pbp_disk(self):
        with open(os.path.join(self.path_to_json, "{}.json".format(self.gameId))) as json_file:
            json_text = json.load(json_file)
            self.plays_json = pd.json_normalize(json_text,'plays')
            if isinstance(json_text['plays'], pd.DataFrame):
                json_text['plays'] = pd.json_normalize(json_text,'plays')
                json_text['plays'] = json_text['plays'].replace({np.nan: None})
        return json_text
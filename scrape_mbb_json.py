import os, json
import re
import http
import xgboost as xgb
import time
import urllib.request
import pyarrow.parquet as pq
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap
import pandas as pd
from pathlib import Path
from play_handler import PlayProcess
path_to_raw = "mbb"
def main():
    years_arr = range(2022,2023)
    schedule = pd.read_parquet('mbb_schedule_master.parquet', engine='auto', columns=None)
    schedule["game_id"] = schedule["game_id"].astype(int)
    schedule_in_repo = pd.read_parquet('mbb/mbb_games_in_data_repo.parquet', engine='auto', columns=None)
    schedule_in_repo["game_id"] = schedule_in_repo["game_id"].astype(int)
    done_already = schedule_in_repo['game_id']
    print(len(schedule))
    print(len(done_already))
    schedule = schedule[schedule['status.type.completed']==True]
    print(len(schedule))
    schedule = schedule[~schedule['game_id'].isin(done_already)]
    print(len(schedule))
    schedule = schedule.sort_values(by=['season'], ascending = False)


    for year in reversed(years_arr):
        print(year)
        games = schedule[(schedule['season']==year)].reset_index()['game_id']
        print(f"Number of Games: {len(games)}, first gameId: {games[0]}")

        # this finds our json files
        path_to_raw_json = "{}/{}/".format(path_to_raw, year)
        Path(path_to_raw_json).mkdir(parents=True, exist_ok=True)
        # json_files = [pos_json.replace('.json', '') for pos_json in os.listdir(path_to_raw_json) if pos_json.endswith('.json')]
        i = 0
        for game in games[i:]:
            if i == len(games):
                print("done with year")
                continue
            if (i % 500 == 0 ):
                print("Working on game {}/{}, gameId: {}".format(i+1, len(games[i:]) + i, game))
            if len(str(game))<9:
                i+=1
                continue
            try:
                processor = PlayProcess( gameId = game, path_to_json = path_to_raw_json)
                pbp = processor.mbb_pbp()

            except (TypeError) as e:
                print("TypeError: yo", e)
                time.sleep(10)
                pbp = processor.mbb_pbp()

            except (KeyError) as e:
                print("KeyError: yo", e)
                i+=1
                if i < len(games):
                    print(f"Skipping game {i+1} of {len(games)}, gameId: {games[i]}")
                else:
                    print(f"Skipping game {i} of {len(games)}")
                continue
            except (ValueError) as e:
                print("DecodeError: yo", e)
                i+=1

                if i < len(games):
                    print(f"Skipping game {i+1} of {len(games)}, gameId: {games[i]}")
                else:
                    print(f"Skipping game {i} of {len(games)}")
                continue
            fp = "{}{}.json".format(path_to_raw_json, game)
            with open(fp,'w') as f:
                json.dump(pbp, f, indent=0, sort_keys=False)
            i+=1



if __name__ == "__main__":
    main()
3
import os, json
import re
import http
import time
import urllib.request
import pandas as pd
import pyreadr
import pyarrow as pa
import sportsdataverse as sdv
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap
from pathlib import Path
path_to_schedules = "mbb/schedules"
final_file_name = "mbb_schedule_master"
pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.width=None


def download_schedule(season, path_to_schedules=None):
    df = sdv.mbb.espn_mbb_calendar(season)
    calendar = df['dateURL'].tolist()
    ev = pd.DataFrame()
    for d in calendar:
        date_schedule = sdv.mbb.espn_mbb_schedule(dates=d)
        ev = ev.append(date_schedule)
    ev = ev[ev['season_type'].isin([2,3])]
    ev = ev.drop('competitors', axis=1)
    ev = ev.drop_duplicates(subset=['game_id'], ignore_index=True)
    if path_to_schedules is not None:
        ev.to_csv(f"{path_to_schedules}/csv/mbb_schedule_{season}.csv", index = False)
        ev.to_parquet(f"{path_to_schedules}/parquet/mbb_schedule_{season}.parquet", index = False)
        pyreadr.write_rds(f"{path_to_schedules}/rds/mbb_schedule_{season}.rds", ev)
    return ev
def main():

    years_arr = range(2022,2023)
    schedule_table = pd.DataFrame()
    for year in years_arr:
        year_schedule = download_schedule(year, path_to_schedules)
        schedule_table = schedule_table.append(year_schedule)
    parquet_files = [pos_csv.replace('.parquet', '') for pos_csv in os.listdir(path_to_schedules+'/parquet') if pos_csv.endswith('.parquet')]
    glued_data = pd.DataFrame()
    for index, js in enumerate(parquet_files):
        x = pd.read_parquet(f"{path_to_schedules}/parquet/{js}.parquet", engine='auto', columns=None)
        glued_data = glued_data.append(x)
    # print(glued_data.head())
    print(glued_data.dtypes)
    glued_data.to_parquet(f"{final_file_name}.parquet", index=False)
if __name__ == "__main__":
    main()

import os, json
import re
import http
import time
import urllib.request
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap
import pandas as pd
from pathlib import Path
from schedule_handler import ScheduleProcess
path_to_raw = "nba/schedules"

def main():

    years_arr = range(2001,2022)
    schedule_table = pd.DataFrame()
    for year in years_arr:
        print(year)
        processor = ScheduleProcess(year)
        year_schedule = processor.nba_schedule()
         # this finds our json files
        year_schedule.to_csv(f"{path_to_raw}/nba_games_info_{year}.csv")
        schedule_table = schedule_table.append(year_schedule)

if __name__ == "__main__":
    main()
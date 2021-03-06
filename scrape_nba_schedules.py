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
path_to_schedules = "nba/schedules/csv"
final_file_name = "nba_schedule_master.csv"
def main():

    years_arr = range(2022,2023)
    schedule_table = pd.DataFrame()
    for year in years_arr:
        processor = ScheduleProcess(year)
        year_schedule = processor.nba_schedule()
        year_schedule = year_schedule[year_schedule['season_type'].isin([2,3])]
         # this finds our json files
        year_schedule.to_csv(f"{path_to_schedules}/nba_schedule_{year}.csv", index=False)
        schedule_table = schedule_table.append(year_schedule)
    csv_files = [pos_csv.replace('.csv', '') for pos_csv in os.listdir(path_to_schedules) if pos_csv.endswith('.csv')]
    glued_data = pd.DataFrame()
    for index, js in enumerate(csv_files):
        x = pd.read_csv(f"{path_to_schedules}/{js}.csv", low_memory=False)
        glued_data = pd.concat([glued_data,x],axis=0)
    glued_data.to_csv(final_file_name, index=False)


if __name__ == "__main__":
    main()

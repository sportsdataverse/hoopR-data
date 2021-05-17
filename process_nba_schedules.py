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
final_file_name = "nba_schedule_2002_2021.csv"
def main():
    csv_files = [pos_csv.replace('.csv', '') for pos_csv in os.listdir(path_to_raw) if pos_csv.endswith('.csv')]
    glued_data = pd.DataFrame()
    for index, js in enumerate(csv_files):
        x = pd.read_csv(f"{path_to_raw}/{js}.csv", low_memory=False)
        glued_data = pd.concat([glued_data,x],axis=0)
    glued_data['game_id'] = glued_data['id']
    glued_data.to_csv(final_file_name)

if __name__ == "__main__":
    main()
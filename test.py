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
import sportsdataverse
from sportsdataverse.nba import *
path_to_schedules = "nba/schedules/csv"
final_file_name = "nba_schedule_master.csv"
def main():

    print(sportsdataverse.nba.espn_nba_calendar(2020))

if __name__ == "__main__":
    main()

#!/bin/bash
python scrape_nba_schedules.py
python scrape_nba_json.py
git add .
git commit -m "daily update" || echo "No changes to commit"
git push
"C:\Program Files\R\R-4.0.5\bin\Rscript.exe" R/01_nba_pbp_creation.R
git add .
git commit -m "NBA Play-by-play and Schedules update" || echo "No changes to commit"
git push
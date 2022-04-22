#!/bin/bash
bash daily_nba_scraper.sh
bash daily_mbb_scraper.sh
git pull
git commit -m "NBA and MBB Play-by-Play and Schedules update" || echo "No changes to commit"
git pull
git push
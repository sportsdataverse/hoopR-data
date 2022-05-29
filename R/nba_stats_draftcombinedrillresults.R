
combine_players_stats <- hoopR::nba_draftcombinestats(season_year = 2022)$DraftCombineStats
combine_players_shooting <- hoopR::nba_draftcombinenonstationaryshooting(season_year = 2022)$Results
combine_players_spot <- hoopR::nba_draftcombinespotshooting(season_year = 2022)$Results

combine_players <- combine_players_stats %>% 
  dplyr::left_join(combine_players_spot,by=c("PLAYER_ID","FIRST_NAME","LAST_NAME","PLAYER_NAME","POSITION")) %>% 
  dplyr::left_join(combine_players_shooting,by=c("PLAYER_ID","FIRST_NAME","LAST_NAME","PLAYER_NAME","POSITION"))

# combine_players_history <- hoopR::nba_drafthistory(season=2021)$DraftHistory
readr::write_csv(combine_players,"nba/draft/nba_draft_2022.csv")


jsonlite::write_json(combine_players,"nba/draft/json/nba_draft_2022.json")
jsonlite::write_json(combine_players_shooting,"nba/draft/json/nba_draftcombinenonstationaryshooting_2022.json")
jsonlite::write_json(combine_players_spot,"nba/draft/json/nba_draftcombinespotshooting_2022.json")
jsonlite::write_json(combine_players_stats,"nba/draft/json/nba_draftcombinestats_2022.json")
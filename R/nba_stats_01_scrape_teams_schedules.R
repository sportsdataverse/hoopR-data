pacman::p_load("dplyr","purrr","stringr","data.table", "qs","arrow")
source('R/utils.R')
years_vec <- hoopR:::most_recent_nba_season()-1

seasons_vec <- unlist(purrr::map(years_vec,function(x){year_to_season(x)}))

# teams_df <- purrr::map_df(1:length(seasons_vec), function(x){
#   teams <- hoopR::nba_leaguestandingsv3(league_id='00',season = seasons_vec[[x]])$Standings %>% 
#     dplyr::select(.data$TeamID, .data$TeamCity, .data$TeamName, .data$TeamSlug,
#                   .data$Conference, .data$Division, .data$LeagueID, .data$SeasonID) %>% 
#     dplyr::mutate(
#       Season = seasons_vec[[x]],
#       TeamNameFull = glue::glue("{TeamCity} {TeamName}"))
#   return(teams)
# })
# 
# data.table::fwrite(teams_df,'nba_stats/nba_stats_teams.csv')
# saveRDS(teams_df,'nba_stats/nba_stats_teams.rds')
# qs::qsave(teams_df,'nba_stats/nba_stats_teams.qs')
# arrow::write_parquet(teams_df, 'nba_stats/nba_stats_teams.parquet')

schedules_df <- purrr::map_dfr(1:length(seasons_vec), function(x){
  season_pull <- seasons_vec[[x]]
  
  completed_sched <- hoopR::nba_leaguegamefinder(season=seasons_vec[[x]])$LeagueGameFinderResults %>% 
    rejoin_schedules() %>% 
    dplyr::mutate(
      Season = seasons_vec[[x]])
  playoff_sched <- data.frame()
  if(nrow(playoff_sched)>0){
  playoff_sched <- hoopR::nba_leaguegamefinder(season=seasons_vec[[x]],season_type="Playoffs")$LeagueGameFinderResults %>% 
    rejoin_schedules() %>% 
    dplyr::mutate(
      Season = seasons_vec[[x]])
  }
  completed_sched <- dplyr::bind_rows(completed_sched,playoff_sched)
  data.table::fwrite(completed_sched,paste0('nba_stats/schedules/csv/schedule_',seasons_vec[[x]],'.csv'))
  saveRDS(completed_sched,paste0('nba_stats/schedules/rds/schedule_',seasons_vec[[x]],'.rds'))
  qs::qsave(completed_sched,paste0('nba_stats/schedules/qs/schedule_',seasons_vec[[x]],'.qs'))
  arrow::write_parquet(completed_sched, paste0('nba_stats/schedules/parquet/schedule_',seasons_vec[[x]],'.parquet'))
  Sys.sleep(2.5)
  return(completed_sched)
})


sched_list <- list.files(path = glue::glue('nba_stats/schedules/csv'))

master_schedules_df <- purrr::map_dfr(sched_list, function(x){
  sched <- data.table::fread(paste0('nba_stats/schedules/csv/',x))
  return(sched)
})
data.table::fwrite(master_schedules_df,'nba_stats_schedule_master.csv')
saveRDS(master_schedules_df,'nba_stats_schedule_master.rds')
qs::qsave(master_schedules_df,'nba_stats_schedule_master.qs')
arrow::write_parquet(master_schedules_df, 'nba_stats_schedule_master.parquet')
pacman::p_load("dplyr","purrr","stringr","data.table", "qs","arrow", "progressr")
source('R/utils.R')

years_vec <- 1996
schedules_df <-data.table::fread("nba_stats_schedule_master.csv")
seasons_vec <- unlist(purrr::map(years_vec,function(x){year_to_season(x)})) 
pbp_list <- list.files(path = glue::glue('nba_stats/1996/'))
pbp_list <- gsub('.json','',pbp_list)
schedules_df_remaining <- schedules_df %>% dplyr::filter(.data$Season == "1996-97",
                                                         !(.data$GAME_ID %in% pbp_list))


year_scrape <- function(seasons_vec){
 
  for(i in 1:length(seasons_vec)){
    print(seasons_vec[[i]])
    schedules_year <- schedules_df %>% dplyr::filter(.data$Season==seasons_vec[[i]],
                                                     !(.data$GAME_ID %in% pbp_list))
    ifelse(!dir.exists(file.path(paste0("nba_stats/",years_vec[[i]]))), dir.create(paste0("nba_stats/",years_vec[[i]])), FALSE)
    yr <- years_vec[[i]]
    nba_pbp_stats <- purrr::map_dfr(unique(schedules_year$GAME_ID), function(x){
      df <- hoopR::nba_pbp(game_id = x)
      jsonlite::write_json(df, path=paste0("nba_stats/",yr,"/",x,".json"))
      Sys.sleep(2.5)
    })
    data.table::fwrite(nba_pbp_stats,paste0('nba_stats/pbp/csv/',seasons_vec[[i]],'nba_pbp_stats.csv'))
    saveRDS(nba_pbp_stats,paste0('nba_stats/pbp/rds/',seasons_vec[[i]],'nba_pbp_stats.rds'))
    qs::qsave(nba_pbp_stats,paste0('nba_stats/pbp/qs/',seasons_vec[[i]],'nba_pbp_stats.qs'))
    arrow::write_parquet(nba_pbp_stats,paste0('nba_stats/pbp/parquet/',seasons_vec[[i]],'nba_pbp_stats.parquet'))
    
   
  }
}

year_scrape(seasons_vec)
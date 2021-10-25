pacman::p_load("dplyr","purrr","stringr","data.table", "qs","arrow", "progressr")
source('R/utils.R')

years_vec <- 1997
schedules_df <-data.table::fread("nba_stats_schedule_master.csv")
seasons_vec <- unlist(purrr::map(years_vec,function(x){year_to_season(x)})) 

year_json_scrape <- function(seasons_vec){
 
  for(i in 1:length(seasons_vec)){
    print(seasons_vec[[i]])
    ifelse(!dir.exists(file.path(paste0("nba_stats/",years_vec[[i]]))), dir.create(paste0("nba_stats/",years_vec[[i]])), FALSE)
    
    pbp_list <- list.files(path = glue::glue('nba_stats/',years_vec[[i]]))
    pbp_list <- gsub('.json','',pbp_list)
    schedules_year <- schedules_df %>% dplyr::filter(.data$Season==seasons_vec[[i]],
                                                     !(.data$GAME_ID %in% pbp_list))
    yr <- years_vec[[i]]
    nba_pbp_stats <- purrr::map_dfr(unique(schedules_year$GAME_ID), function(x){
      df <- hoopR::nba_pbp(game_id = x)
      jsonlite::write_json(df, path=paste0("nba_stats/",yr,"/",x,".json"))
      Sys.sleep(2.5)
    })
  }
}

year_json_scrape(seasons_vec)
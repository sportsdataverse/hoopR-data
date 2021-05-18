
library(tidyverse)
library(dplyr)
library(stringr)
library(arrow)

years_vec <- 2002:2021
# --- compile into play_by_play_{year}.parquet ---------
future::plan("multisession")
progressr::with_progress({
  p <- progressr::progressor(along = years_vec)
  pbp_games <- purrr::map_dfr(years_vec, function(y){
    
    pbp_g <- data.frame()
    pbp_list <- list.files(path = glue::glue('nba/{y}/'))
    print(glue::glue('nba/{y}/'))
    pbp_g <- furrr::future_map_dfr(pbp_list, function(x){
      pbp <- jsonlite::fromJSON(glue::glue('nba/{y}/{x}'))$plays
      pbp$game_id <- gsub(".json","", x)
      return(pbp)
    })
    pbp_g <- pbp_g %>% janitor::clean_names()
    ifelse(!dir.exists(file.path("nba/pbp")), dir.create(file.path("nba/pbp")), FALSE)
    ifelse(!dir.exists(file.path("nba/pbp/csv")), dir.create(file.path("nba/pbp/csv")), FALSE)
    write.csv(pbp_g, file=gzfile(glue::glue("nba/pbp/csv/play_by_play_{y}.csv.gz")), row.names = FALSE)
    ifelse(!dir.exists(file.path("nba/pbp/rds")), dir.create(file.path("nba/pbp/rds")), FALSE)
    saveRDS(pbp_g,glue::glue("nba/pbp/rds/play_by_play_{y}.rds"))
    ifelse(!dir.exists(file.path("nba/pbp/parquet")), dir.create(file.path("nba/pbp/parquet")), FALSE)
    
    arrow::write_parquet(pbp_g, glue::glue("nba/pbp/parquet/play_by_play_{y}.parquet"))
    p(sprintf("y=%s", as.integer(y)))
    return(pbp_g)
  })
})

sched <- read.csv('nba_schedule_2002_2021.csv')



df_game_ids <- as.data.frame(
  dplyr::distinct(pbp_games %>% 
                    dplyr::select(game_id, season, season_type, home_team_name, away_team_name))) %>% 
  dplyr::arrange(-season) %>% 
  dplyr::mutate(
    game_id = as.integer(.data$game_id)
  )

sched <- sched %>% 
  dplyr::left_join(df_game_ids %>% dplyr::select(game_id,season), by = "game_id",
                   suffix = c("", ".y"),)
sched <- sched %>% 
  dplyr::mutate(
    PBP = ifelse(is.na(.data$season.y), FALSE, TRUE)
  ) %>% 
  dplyr::select(-.data$season.y)

write.csv(sched, 'nba_schedule_2002_2021.csv', row.names = FALSE)
write.csv(sched %>% dplyr::filter(.data$PBP == TRUE), 'nba/nba_games_in_data_repo.csv', row.names = FALSE)


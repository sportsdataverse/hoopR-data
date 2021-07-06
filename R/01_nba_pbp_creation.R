
.libPaths("C:/Users/saiem/Documents/R/win-library/4.0")
Sys.setenv(R_LIBS="C:/Users/saiem/Documents/R/win-library/4.0")
if (!requireNamespace('pacman', quietly = TRUE)){
  install.packages('pacman',lib=Sys.getenv("R_LIBS"), repos='http://cran.us.r-project.org')
}
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(furrr, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(future, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc="C:/Users/saiem/Documents/R/win-library/4.0")))

years_vec <- 2002:2021
# --- compile into play_by_play_{year}.parquet ---------
future::plan("multisession")
pbp_games <- purrr::map_dfr(years_vec, function(y){
  pbp_g <- data.frame()
  pbp_list <- list.files(path = glue::glue('nba/{y}/'))
  pbp_g <- furrr::future_map_dfr(pbp_list, function(x){
    pbp <- jsonlite::fromJSON(glue::glue('nba/{y}/{x}'))$plays
    pbp$game_id <- gsub(".json","", x)
    return(pbp)
  })
  if(nrow(pbp_g)>0){
    pbp_g <- pbp_g %>% janitor::clean_names()
    pbp_g <- pbp_g %>% dplyr::mutate(game_id = as.integer(.data$game_id))
  }
  ifelse(!dir.exists(file.path("nba/pbp")), dir.create(file.path("nba/pbp")), FALSE)
  ifelse(!dir.exists(file.path("nba/pbp/csv")), dir.create(file.path("nba/pbp/csv")), FALSE)
  write.csv(pbp_g, file=gzfile(glue::glue("nba/pbp/csv/play_by_play_{y}.csv.gz")), row.names = FALSE)
  ifelse(!dir.exists(file.path("nba/pbp/rds")), dir.create(file.path("nba/pbp/rds")), FALSE)
  saveRDS(pbp_g,glue::glue("nba/pbp/rds/play_by_play_{y}.rds"))
  ifelse(!dir.exists(file.path("nba/pbp/parquet")), dir.create(file.path("nba/pbp/parquet")), FALSE)
  arrow::write_parquet(pbp_g, glue::glue("nba/pbp/parquet/play_by_play_{y}.parquet"))
  sched <- read.csv(glue::glue('nba/schedules/nba_schedule_{y}.csv'))
  sched <- sched %>%
  dplyr::mutate(
    status.displayClock = as.character(.data$status.displayClock),
    PBP = ifelse(game_id %in% unique(pbp_g$game_id), TRUE,FALSE)
  )
  write.csv(dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$date)),glue::glue('nba/schedules/nba_schedule_{y}.csv'), row.names=FALSE)
  arrow::write_parquet(dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$date)),glue::glue('nba/schedules/nba_schedule_{y}.parquet'))
  return(pbp_g)
})
future::plan("multisession")
all_games <- purrr::map(years_vec, function(y){
  pbp_g <- pbp_games %>% 
      dplyr::filter(.data$season == y)
  
  ifelse(!dir.exists(file.path("nba/pbp")), dir.create(file.path("nba/pbp")), FALSE)
  ifelse(!dir.exists(file.path("nba/pbp/csv")), dir.create(file.path("nba/pbp/csv")), FALSE)
  write.csv(pbp_g, file=gzfile(glue::glue("nba/pbp/csv/play_by_play_{y}.csv.gz")), row.names = FALSE)
  ifelse(!dir.exists(file.path("nba/pbp/rds")), dir.create(file.path("nba/pbp/rds")), FALSE)
  saveRDS(pbp_g,glue::glue("nba/pbp/rds/play_by_play_{y}.rds"))
  ifelse(!dir.exists(file.path("nba/pbp/parquet")), dir.create(file.path("nba/pbp/parquet")), FALSE)
  arrow::write_parquet(pbp_g, glue::glue("nba/pbp/parquet/play_by_play_{y}.parquet"))
  return(pbp_g)
})
sched_list <- list.files(path = glue::glue('nba/schedules/'))
sched_g <-  purrr::map_dfr(sched_list, function(x){
  sched <- read.csv(glue::glue('nba/schedules/{x}')) %>%
    dplyr::mutate(
      status.displayClock = as.character(.data$status.displayClock)
    )
  return(sched)
})


write.csv(sched_g %>% dplyr::arrange(desc(.data$date)), 'nba_schedule_2002_2021.csv', row.names = FALSE)
write.csv(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)), 'nba/nba_games_in_data_repo.csv', row.names = FALSE)

arrow::write_parquet(sched_g %>% dplyr::arrange(desc(.data$date)),glue::glue('nba_schedule_2002_2021.parquet'))
arrow::write_parquet(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)),glue::glue('nba/nba_games_in_data_repo.parquet'))

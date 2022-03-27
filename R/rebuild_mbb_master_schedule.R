
.libPaths("C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")
Sys.setenv(R_LIBS="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")
if (!requireNamespace('pacman', quietly = TRUE)){
  install.packages('pacman',lib=Sys.getenv("R_LIBS"), repos='http://cran.us.r-project.org')
}
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.1")))


options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- hoopR:::most_recent_mbb_season()
# pbp_list <- list.files(path = glue::glue('mbb/pbp/parquet/'))
# pbp_g <-  purrr::map_dfr(pbp_list, function(x){
#   pbp <- arrow::read_parquet(paste0('mbb/pbp/parquet/',x)) %>%
#     dplyr::filter(is.numeric(.data$id)) %>%
#     dplyr::mutate(
#       status.displayClock = as.character(.data$status.displayClock)
#     )
#   return(pbp)
# })
sched_list <- list.files(path = glue::glue('mbb/schedules/parquet/'))
sched_g <-  purrr::map_dfr(sched_list, function(x){
  sched <- arrow::read_parquet(paste0('mbb/schedules/parquet/',x)) %>%
    dplyr::mutate(
      id = as.integer(.data$id),
      game_id = as.integer(.data$game_id),
      type.id = as.integer(.data$`type.id`),
      venue.id = as.integer(.data$`venue.id`),
      status.type.id = as.integer(.data$`status.type.id`),
      home.id = as.integer(.data$`home.id`),
      home.venue.id = as.integer(.data$`home.venue.id`),
      home.conferenceId = as.integer(.data$`home.conferenceId`),
      away.id = as.integer(.data$`away.id`),
      away.venue.id = as.integer(.data$`away.venue.id`),
      away.conferenceId = as.integer(.data$`away.conferenceId`),
      groups.id = as.integer(.data$`groups.id`),
      status.displayClock = as.character(.data$status.displayClock)
    )
  return(sched)
})

print(sched_g)
data.table::fwrite(sched_g %>% dplyr::arrange(desc(.data$date)), 'mbb_schedule_master.csv')
data.table::fwrite(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)), 'mbb/mbb_games_in_data_repo.csv')
qs::qsave(sched_g %>% dplyr::arrange(desc(.data$date)), 'mbb_schedule_master.qs')
qs::qsave(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)), 'mbb/mbb_games_in_data_repo.qs')
arrow::write_parquet(sched_g %>% dplyr::arrange(desc(.data$date)),glue::glue('mbb_schedule_master.parquet'))
arrow::write_parquet(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)), 'mbb/mbb_games_in_data_repo.parquet')
print(nrow(sched_g %>% dplyr::filter(.data$season == years_vec, is.numeric(.data$id))))
rm(sched_g)
rm(sched_list)
rm(years_vec)
gc()
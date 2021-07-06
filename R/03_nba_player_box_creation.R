
.libPaths("C:\\Users\\saiem\\Documents\\R\\win-library\\4.0")
Sys.setenv(R_LIBS="C:\\Users\\saiem\\Documents\\R\\win-library\\4.0")
if (!requireNamespace('pacman', quietly = TRUE)){
  install.packages('pacman',lib=Sys.getenv("R_LIBS"), repos='http://cran.us.r-project.org')
}
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.0")))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.0")))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.0")))
suppressPackageStartupMessages(suppressMessages(library(furrr, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.0")))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.0")))
suppressPackageStartupMessages(suppressMessages(library(future, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.0")))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.0")))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.0")))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc="C:\\Users\\saiem\\Documents\\R\\win-library\\4.0")))

options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- 2002:2021
# --- compile into player_box_{year}.parquet ---------
future::plan("multisession")

player_box_games <- purrr::map_dfr(sort(years_vec, decreasing = TRUE), function(y){
  player_box_g <- data.frame()
  player_box_list <- list.files(path = glue::glue('nba/{y}/'))
  print(glue::glue('nba/{y}/'))
  player_box_g <- furrr::future_map_dfr(player_box_list, function(x){
    game_json <- jsonlite::fromJSON(glue::glue('nba/{y}/{x}'))
    
    player_box_score <- data.frame()
    players_box_score_df <- data.frame()
    players_box_score_df <- data.frame(jsonlite::fromJSON(jsonlite::toJSON(game_json[['boxscore']][['players']]), flatten=TRUE))
    gameId <- game_json[["gameId"]]
    season <- game_json[['header']][['season']][['year']]
    season_type <- game_json[['header']][['season']][['type']]
    boxScoreAvailable = game_json[['header']][['competitions']][["boxscoreAvailable"]]
    
    boxScoreSource = game_json[['header']][['competitions']][["boxscoreSource"]]
    homeTeamId = game_json[['header']][['competitions']][['competitors']][[1]][['team']][['id']][1]
    awayTeamId = game_json[['header']][['competitions']][['competitors']][[1]][['team']][['id']][2]
    homeTeamMascot = game_json[['header']][['competitions']][['competitors']][[1]][['team']][['name']][1]
    awayTeamMascot = game_json[['header']][['competitions']][['competitors']][[1]][['team']][['name']][2]
    homeTeamName = game_json[['header']][['competitions']][['competitors']][[1]][['team']][['location']][1]
    awayTeamName = game_json[['header']][['competitions']][['competitors']][[1]][['team']][['location']][2]
    
    homeTeamAbbrev = game_json[['header']][['competitions']][['competitors']][[1]][['team']][['abbreviation']][1]
    awayTeamAbbrev = game_json[['header']][['competitions']][['competitors']][[1]][['team']][['abbreviation']][2]
    game_date = as.Date(substr(game_json[['header']][['competitions']][['date']],0,10))
    
    tryCatch(
      expr = {
        if(length(players_box_score_df[["statistics"]])>0){
          if(length(players_box_score_df[["statistics"]][[1]][["athletes"]][[1]])>0){
            players_df <- players_box_score_df %>%
              tidyr::unnest(.data$statistics) %>%
              tidyr::unnest(.data$athletes)
            stat_cols <- players_df$names[[1]]
            stats <- players_df$stats
            
            stats_df <- as.data.frame(do.call(rbind,stats))
            colnames(stats_df) <- stat_cols
            cols <- c('starter','ejected', 'didNotPlay','active',
                      'athlete.displayName','athlete.jersey',
                      'athlete.id','athlete.shortName',
                      'athlete.headshot.href','athlete.position.name',
                      'athlete.position.abbreviation', 'team.shortDisplayName',
                      'team.name', 'team.logo', 'team.id', 'team.abbreviation',
                      'team.color')
            
            players_df <- players_df %>%
              dplyr::filter(!.data$didNotPlay) %>%
              dplyr::select(tidyselect::any_of(cols))
            if(length(stats_df)>0){
              players_df <- dplyr::bind_cols(stats_df,players_df) %>%
                dplyr::select(.data$athlete.displayName,.data$team.shortDisplayName, tidyr::everything())
              
              
              players_df <- players_df %>%
                janitor::clean_names() %>%
                dplyr::rename(
                  '+/-'=.data$x,
                  fg3 = .data$x3pt
                )
              player_box_score <- players_df %>%
                dplyr::mutate(
                  game_id = gameId,
                  season = season,
                  season_type = season_type,
                  game_date = game_date
                ) 
            }
          }
        }
      },
      error = function(e) {
        message(glue::glue("{Sys.time()}: Invalid arguments or no player box data available!"))
      },
      warning = function(w) {
      },
      finally = {
      }
    )
    
    drop <- c("statistics")
    player_box_score = player_box_score[,!(names(player_box_score) %in% drop)]
    
    return(player_box_score)
  })
    
    
  ifelse(!dir.exists(file.path("nba/player_box")), dir.create(file.path("nba/player_box")), FALSE)
  ifelse(!dir.exists(file.path("nba/player_box/csv")), dir.create(file.path("nba/player_box/csv")), FALSE)
  write.csv(player_box_g, file=gzfile(glue::glue("nba/player_box/csv/player_box_{y}.csv.gz")), row.names = FALSE)
  ifelse(!dir.exists(file.path("nba/player_box/rds")), dir.create(file.path("nba/player_box/rds")), FALSE)
  saveRDS(player_box_g,glue::glue("nba/player_box/rds/player_box_{y}.rds"))
  ifelse(!dir.exists(file.path("nba/player_box/parquet")), dir.create(file.path("nba/player_box/parquet")), FALSE)
  arrow::write_parquet(player_box_g, glue::glue("nba/player_box/parquet/player_box_{y}.parquet"))
  sched <- read.csv(glue::glue('nba/schedules/nba_schedule_{y}.csv'))
  sched <- sched %>%
    dplyr::mutate(
      status.displayClock = as.character(.data$status.displayClock),
      player_box = ifelse(.data$game_id %in% unique(player_box_g$game_id), TRUE,FALSE)
    )
  write.csv(dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$date)),glue::glue('nba/schedules/nba_schedule_{y}.csv'), row.names=FALSE)
  arrow::write_parquet(dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$date)),glue::glue('nba/schedules/nba_schedule_{y}.parquet'))
  return(player_box_g)
})
future::plan("multisession")
all_games <- purrr::map(years_vec, function(y){
  player_box_g <- player_box_games %>% 
    dplyr::filter(.data$season == y)
  ifelse(!dir.exists(file.path("nba/player_box")), dir.create(file.path("nba/player_box")), FALSE)
  ifelse(!dir.exists(file.path("nba/player_box/csv")), dir.create(file.path("nba/player_box/csv")), FALSE)
  write.csv(player_box_g, file=gzfile(glue::glue("nba/player_box/csv/player_box_{y}.csv.gz")), row.names = FALSE)
  ifelse(!dir.exists(file.path("nba/player_box/rds")), dir.create(file.path("nba/player_box/rds")), FALSE)
  saveRDS(player_box_g,glue::glue("nba/player_box/rds/player_box_{y}.rds"))
  ifelse(!dir.exists(file.path("nba/player_box/parquet")), dir.create(file.path("nba/player_box/parquet")), FALSE)
  arrow::write_parquet(player_box_g, glue::glue("nba/player_box/parquet/player_box_{y}.parquet"))
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
length(unique(player_box_games$game_id))

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
# --- compile into team_box_{year}.parquet ---------
future::plan("multisession")

team_box_games <- purrr::map_dfr(sort(years_vec, decreasing = TRUE), function(y){
  team_box_g <- data.frame()
  team_box_list <- list.files(path = glue::glue('nba/{y}/'))
  print(glue::glue('nba/{y}/'))
  team_box_g <- furrr::future_map_dfr(team_box_list, function(x){
    game_json <- jsonlite::fromJSON(glue::glue('nba/{y}/{x}'))

    team_box_score <- data.frame()
    teams_box_score_df <- data.frame()
    teams_box_score_df <- data.frame(jsonlite::fromJSON(jsonlite::toJSON(game_json[['boxscore']][['teams']]), flatten=TRUE))
    gameId <- game_json[["gameId"]]
    season <- game_json[['header']][['season']][['year']]
    season_type <- game_json[['header']][['season']][['type']]
    boxScoreAvailable = game_json[['header']][['competitions']][["boxscoreAvailable"]]
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
        if(boxScoreAvailable == TRUE){
          if(length(teams_box_score_df[["statistics"]][[1]])>0){
            teams_box_score_df_2 <- teams_box_score_df[['statistics']][[2]] %>%
              dplyr::select(.data$displayValue, .data$name) %>%
              dplyr::rename(Home = .data$displayValue)
            teams_box_score_df_1 <- teams_box_score_df[['statistics']][[1]] %>%
              dplyr::select(.data$displayValue, .data$name) %>%
              dplyr::rename(Away = .data$displayValue)
            
            teams2 <- data.frame(t(teams_box_score_df_2$Home))
            colnames(teams2) <- t(teams_box_score_df_2$name)
            teams2$Team <- "Home"
            teams2$OpponentId <- as.integer(awayTeamId)
            teams2$OpponentName <- awayTeamName
            teams2$OpponentMascot <- awayTeamMascot
            teams2$OpponentAbbrev <- awayTeamAbbrev
    
            teams1 <- data.frame(t(teams_box_score_df_1$Away))
            colnames(teams1) <- t(teams_box_score_df_1$name)
            teams1$Team <- "Away"
            teams1$OpponentId <- as.integer(homeTeamId)
            teams1$OpponentName <- homeTeamName
            teams1$OpponentMascot <- homeTeamMascot
            teams1$OpponentAbbrev <- homeTeamAbbrev
            teams <- dplyr::bind_rows(teams1,teams2)
            team_box_score <- teams_box_score_df %>%
              # dplyr::select(-.data$statistics) %>%
              dplyr::bind_cols(teams)
    
            team_box_score <- team_box_score %>%
              dplyr::mutate(
                game_id = gameId,
                season = season,
                season_type = season_type,
                game_date = game_date
              ) %>%
              janitor::clean_names()
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
    team_box_score = team_box_score[,!(names(team_box_score) %in% drop)]
    return(team_box_score)
  })

  ifelse(!dir.exists(file.path("nba/team_box")), dir.create(file.path("nba/team_box")), FALSE)
  ifelse(!dir.exists(file.path("nba/team_box/csv")), dir.create(file.path("nba/team_box/csv")), FALSE)
  write.csv(team_box_g, file=gzfile(glue::glue("nba/team_box/csv/team_box_{y}.csv.gz")), row.names = FALSE)
  ifelse(!dir.exists(file.path("nba/team_box/rds")), dir.create(file.path("nba/team_box/rds")), FALSE)
  saveRDS(team_box_g,glue::glue("nba/team_box/rds/team_box_{y}.rds"))
  ifelse(!dir.exists(file.path("nba/team_box/parquet")), dir.create(file.path("nba/team_box/parquet")), FALSE)
  arrow::write_parquet(team_box_g, glue::glue("nba/team_box/parquet/team_box_{y}.parquet"))
  sched <- read.csv(glue::glue('nba/schedules/nba_schedule_{y}.csv'))
  sched <- sched %>%
    dplyr::mutate(
      status.displayClock = as.character(.data$status.displayClock),
      team_box = ifelse(.data$game_id %in% unique(team_box_g$game_id), TRUE,FALSE)
    )
  write.csv(dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$date)),glue::glue('nba/schedules/nba_schedule_{y}.csv'), row.names=FALSE)

  return(team_box_g)
})
future::plan("multisession")
all_games <- purrr::map(years_vec, function(y){
  team_box_g <- team_box_games %>% 
    dplyr::filter(.data$season == y)
  ifelse(!dir.exists(file.path("nba/team_box")), dir.create(file.path("nba/team_box")), FALSE)
  ifelse(!dir.exists(file.path("nba/team_box/csv")), dir.create(file.path("nba/team_box/csv")), FALSE)
  write.csv(team_box_g, file=gzfile(glue::glue("nba/team_box/csv/team_box_{y}.csv.gz")), row.names = FALSE)
  ifelse(!dir.exists(file.path("nba/team_box/rds")), dir.create(file.path("nba/team_box/rds")), FALSE)
  saveRDS(team_box_g,glue::glue("nba/team_box/rds/team_box_{y}.rds"))
  ifelse(!dir.exists(file.path("nba/team_box/parquet")), dir.create(file.path("nba/team_box/parquet")), FALSE)
  arrow::write_parquet(team_box_g, glue::glue("nba/team_box/parquet/team_box_{y}.parquet"))
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


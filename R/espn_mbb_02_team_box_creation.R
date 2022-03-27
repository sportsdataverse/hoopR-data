
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
# --- compile into team_box_{year}.parquet ---------

mbb_team_box_games <- function(y){
  cli::cli_process_start("Starting mbb team_box parse for {y}!")
  team_box_g <- data.frame()
  team_box_list <- list.files(path = glue::glue('mbb/{y}/'))
  team_box_g <- purrr::map_dfr(team_box_list, function(x){
    game_json <- jsonlite::fromJSON(glue::glue('mbb/{y}/{x}'))
    
    team_box_score <- data.frame()
    teams_box_score_df <- data.frame()
    teams_box_score_df <- data.frame(jsonlite::fromJSON(jsonlite::toJSON(game_json[['boxscore']][['teams']]), flatten=TRUE))
    gameId <- game_json[["gameId"]]
    season <- game_json[['header']][['season']][['year']]
    season_type <- game_json[['header']][['season']][['type']]
    boxScoreAvailable = game_json[['header']][['competitions']][["boxscoreAvailable"]]
    
    homeAwayTeam1 = toupper(game_json[['header']][['competitions']][['competitors']][[1]][['homeAway']][1])
    homeAwayTeam2 = toupper(game_json[['header']][['competitions']][['competitors']][[1]][['homeAway']][2])
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
        if(boxScoreAvailable == TRUE && length(teams_box_score_df[["statistics"]][[1]])>1){
          
            teams_box_score_df_2 <- teams_box_score_df[['statistics']][[2]] %>%
              dplyr::select(.data$displayValue, .data$name) %>%
              dplyr::rename(Home = .data$displayValue)
            teams_box_score_df_1 <- teams_box_score_df[['statistics']][[1]] %>%
              dplyr::select(.data$displayValue, .data$name) %>%
              dplyr::rename(Away = .data$displayValue)
            
            teams2 <- data.frame(t(teams_box_score_df_2$Home))
            colnames(teams2) <- t(teams_box_score_df_2$name)
            teams2$homeAway <- homeAwayTeam2
            teams2$OpponentId <- as.integer(awayTeamId)
            teams2$OpponentName <- awayTeamName
            teams2$OpponentMascot <- awayTeamMascot
            teams2$OpponentAbbrev <- awayTeamAbbrev
            
            teams1 <- data.frame(t(teams_box_score_df_1$Away))
            colnames(teams1) <- t(teams_box_score_df_1$name)
            teams1$homeAway <- homeAwayTeam1
            teams1$OpponentId <- as.integer(homeTeamId)
            teams1$OpponentName <- homeTeamName
            teams1$OpponentMascot <- homeTeamMascot
            teams1$OpponentAbbrev <- homeTeamAbbrev
            teams <- dplyr::bind_rows(teams1,teams2)
            team_box_score <- teams_box_score_df %>%
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
      },
      error = function(e) {
        # message(glue::glue("{Sys.time()}: Invalid arguments or no team box data available!"))
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
  if(nrow(team_box_g)>0 && !("largest_lead" %in% colnames(team_box_g))){
    team_box_g$largestLead <- NA_character_
    team_box_g <- team_box_g %>% 
      dplyr::relocate(.data$largestLead, .after = last_col())
    
  }
  if(nrow(team_box_g)>0){
    ifelse(!dir.exists(file.path("mbb/team_box")), dir.create(file.path("mbb/team_box")), FALSE)
    ifelse(!dir.exists(file.path("mbb/team_box/csv")), dir.create(file.path("mbb/team_box/csv")), FALSE)
    data.table::fwrite(team_box_g, file=paste0("mbb/team_box/csv/team_box_",y,".csv.gz"))
    
    ifelse(!dir.exists(file.path("mbb/team_box/qs")), dir.create(file.path("mbb/team_box/qs")), FALSE)
    qs::qsave(team_box_g,glue::glue("mbb/team_box/qs/team_box_{y}.qs"))
    
    ifelse(!dir.exists(file.path("mbb/team_box/rds")), dir.create(file.path("mbb/team_box/rds")), FALSE)
    saveRDS(team_box_g,glue::glue("mbb/team_box/rds/team_box_{y}.rds"))
    
    ifelse(!dir.exists(file.path("mbb/team_box/parquet")), dir.create(file.path("mbb/team_box/parquet")), FALSE)
    arrow::write_parquet(team_box_g, glue::glue("mbb/team_box/parquet/team_box_{y}.parquet"))
  }
  sched <- arrow::read_parquet(paste0('mbb/schedules/parquet/mbb_schedule_',y,'.parquet'))
  sched <- sched %>%
    dplyr::mutate(
      game_id = as.integer(.data$id),
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
      status.displayClock = as.character(.data$status.displayClock))
  if(nrow(team_box_g)>0){
    sched <- sched %>%
      dplyr::mutate(
        team_box = ifelse(.data$game_id %in% unique(team_box_g$game_id), TRUE,FALSE)
      )
  } else {
    sched$team_box <- FALSE
  }
  final_sched <- dplyr::distinct(sched) %>% dplyr::arrange(desc(.data$date))
  data.table::fwrite(final_sched,paste0("mbb/schedules/csv/mbb_schedule_",y,".csv"))
  qs::qsave(final_sched,glue::glue('mbb/schedules/qs/mbb_schedule_{y}.qs'))
  saveRDS(final_sched, glue::glue('mbb/schedules/rds/mbb_schedule_{y}.rds'))
  arrow::write_parquet(final_sched, glue::glue('mbb/schedules/parquet/mbb_schedule_{y}.parquet'))
  rm(sched)
  rm(final_sched)
  rm(team_box_g)
  rm(team_box_list)
  gc()
  cli::cli_process_done(msg_done = "Finished mbb team_box parse for {y}!")
  return(NULL)
}


all_games <- purrr::map(years_vec, function(y){
  mbb_team_box_games(y)
  return(NULL)
})

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



# data.table::fwrite(sched_g %>% dplyr::arrange(desc(.data$date)), 'mbb_schedule_master.csv')
data.table::fwrite(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)), 'mbb/mbb_games_in_data_repo.csv')
qs::qsave(sched_g %>% dplyr::arrange(desc(.data$date)), 'mbb_schedule_master.qs')
qs::qsave(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)), 'mbb/mbb_games_in_data_repo.qs')
arrow::write_parquet(sched_g %>% dplyr::arrange(desc(.data$date)),glue::glue('mbb_schedule_master.parquet'))
arrow::write_parquet(sched_g %>% dplyr::filter(.data$PBP == TRUE) %>% dplyr::arrange(desc(.data$date)), 'mbb/mbb_games_in_data_repo.parquet')


rm(all_games)
rm(sched_g)
rm(sched_list)
rm(years_vec)
gc()
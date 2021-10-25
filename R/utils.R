
year_to_season <- function(year){
  first_year <- substr(year,3,4)
  next_year <- as.numeric(first_year)+1
  next_year <- dplyr::case_when(
    next_year <10 & first_year > 0 ~ glue::glue("0{next_year}"),
    first_year == 99 ~ "00",
    TRUE ~ as.character(next_year))
  return(glue::glue("{year}-{next_year}"))
}

rejoin_schedules <- function(df){
  df <- df %>% 
    dplyr::mutate(
      HOME_AWAY = ifelse(stringr::str_detect(.data$MATCHUP,"@"),"AWAY","HOME")) %>% 
    dplyr::select(-.data$WL,.data$MATCHUP)
  away_df <- df %>% 
    dplyr::filter(.data$HOME_AWAY == "AWAY") %>% 
    dplyr::select(-.data$HOME_AWAY) %>% 
    dplyr::select(.data$SEASON_ID, .data$GAME_ID, .data$GAME_DATE, .data$MATCHUP, tidyr::everything())
  colnames(away_df)[5:ncol(away_df)]<-paste0("AWAY_", colnames(away_df)[5:ncol(away_df)])
  home_df <- df %>% 
    dplyr::filter(.data$HOME_AWAY == "HOME") %>% 
    dplyr::select(-.data$HOME_AWAY, -.data$MATCHUP) %>% 
    dplyr::select(.data$SEASON_ID, .data$GAME_ID, .data$GAME_DATE,  tidyr::everything())
  colnames(home_df)[4:ncol(home_df)]<-paste0("HOME_", colnames(home_df)[4:ncol(home_df)])
  sched_df <- away_df %>% 
    dplyr::left_join(home_df, by=c("GAME_ID", "SEASON_ID", "GAME_DATE"))
  return(sched_df)
}

# This function takes an NBA game id, pulls down play by play data
# for that game, and then converts it to lineup data
process_pbp = function(game_id) {
  pbp_url = paste0(
    "https://stats.nba.com/stats/playbyplayv2?GameID=",
    game_id,
    "&StartPeriod=0&EndPeriod=12"
  )

  json = curl::curl(url = pbp_url) %>%
    readr::read_lines() %>%
    jsonlite::fromJSON(simplifyVector = T)

  varnames = json$resultSets$headers[[1]]
  n_vars = length(varnames)
  vals = json$resultSets$rowSet[[1]]
  pbp_df = data.frame(vals,
                      stringsAsFactors = FALSE)
  colnames(pbp_df) = varnames
  pbp_df = pbp_df %>%
    dplyr::mutate(PERIOD_LAG = dplyr::lag(PERIOD))

  teams = sort(na.omit(unique(pbp_df$PLAYER1_TEAM_ABBREVIATION)))

  team_one_lineup_mat = team_two_lineup_mat =
    matrix(NA_character_, nrow = nrow(pbp_df), ncol = 5)
  colnames(team_one_lineup_mat) = paste0("team_one_player_", 1:5)
  colnames(team_two_lineup_mat) = paste0("team_two_player_", 1:5)

  for (i in 1:nrow(pbp_df)) {
    # if it's the beginning of a period, reset lineups
    period = pbp_df[i, ] %>%
      dplyr::select(PERIOD, PERIOD_LAG)

    if (is.na(period$PERIOD_LAG) || period$PERIOD != period$PERIOD_LAG) {
      team_one_lineup = team_two_lineup = rep(NA, 5)
    }

    for (j in 1:3) {
      # check for subs and remove the appropriate player if necessary
      if (j == 1) {

        events = pbp_df[i, ] %>%
          dplyr::select(dplyr::matches("DESCRIPTION")) %>%
          as.character()
        subs = any(grepl("SUB", events))

        if (subs) {
          player_id = pbp_df[i, ] %>%
            dplyr::select(dplyr::matches(sprintf("PLAYER%s_ID", j))) %>%
            dplyr::pull()
          player_team = pbp_df[i, ] %>%
            dplyr::select(dplyr::matches(sprintf("PLAYER%s_TEAM_ABBREVIATION", j))) %>%
            dplyr::pull()
          if (player_team == teams[1]) {
            if (player_id %in% team_one_lineup) {
              team_one_lineup[team_one_lineup == player_id] = NA
            } else {
              team_one_lineup_mat[i-1, ][is.na(team_one_lineup_mat[i-1, ])] =
                player_id
            }
          } else if (player_team == teams[2]) {
            if (player_id %in% team_two_lineup) {
              team_two_lineup[team_two_lineup == player_id] = NA
            } else {
              team_two_lineup_mat[i-1, ][is.na(team_two_lineup_mat[i-1, ])] =
                player_id
            }
          } else {
            stop("error")
          }
          # if there are subs increment player index
          j = j + 1
        }
      }

      player_id = pbp_df[i, ] %>%
        dplyr::select(dplyr::matches(sprintf("PLAYER%s_ID", j))) %>%
        dplyr::pull()
      player_team = pbp_df[i, ] %>%
        dplyr::select(dplyr::matches(sprintf("Player%s_TEAM_ABBREVIATION", j))) %>%
        dplyr::pull()
      if (!is.na(player_team) && player_id != 0) {
        if (player_team == teams[1] & !(player_id %in% team_one_lineup)) {
          if (sum(is.na(team_one_lineup)) > 0) {
            team_one_lineup[which(is.na(team_one_lineup))[1]] = player_id
          }
        } else if (player_team == teams[2] & !(player_id %in% team_two_lineup))
        {
          if (sum(is.na(team_two_lineup)) > 0) {
            team_two_lineup[is.na(team_two_lineup)][1] = player_id
          }
        }
      }
    }
    team_one_lineup = sort(team_one_lineup, na.last = TRUE)
    team_two_lineup = sort(team_two_lineup, na.last = TRUE)

    team_one_lineup_mat[i, ] = team_one_lineup
    team_two_lineup_mat[i, ] = team_two_lineup
  }

  fill_missing_lineup = function(lineup_mat) {
    lineup_sort = apply(lineup_mat, 1, sort, na.last = TRUE) %>%
      unlist() %>%
      matrix(nrow = nrow(pbp_df), byrow = TRUE)
    na_idx = apply(lineup_sort, 1, function(x) { any(is.na(x)) })
    lineup_sort[na_idx, ] = rep(NA, 5)
    lineup_final = zoo::na.locf(lineup_sort, na.rm = FALSE, fromLast = TRUE)
    colnames(lineup_final) = colnames(lineup_mat)
    assertthat::assert_that(all(dim(lineup_mat) == dim(lineup_final)))
    lineup_final
  }

  team_one_lineup_final = fill_missing_lineup(team_one_lineup_mat)
  team_two_lineup_final = fill_missing_lineup(team_two_lineup_mat)

  pbp_df = cbind(pbp_df, team_one_lineup_final, team_two_lineup_final)
  pbp_df
}


game_ids = data.table::fread("nba_stats_schedule_master.csv") %>%
  dplyr::select(.data$GAME_ID) %>%
  dplyr::unlist()
n_games = length(game_ids)
pbp_list = vector("list", length = n_games)
for (i in 1:n_games) {
  pbp_list[[i]] = process_pbp(game_id = game_ids[i])
}
pbp_df = dplyr::bind_rows(pbp_list)
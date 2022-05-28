combine_players <- hoopR::nba_draftcombinedrillresults(season = 2022)$Results

readr::write_csv(combine_players,"nba/draft/nba_draftcombinedrillresults_2022.csv")
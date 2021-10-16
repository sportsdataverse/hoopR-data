future::plan("multisession")
all_games <- purrr::map(years_vec, function(y){
  pbp_g <- pbp_games %>% 
    dplyr::filter(.data$season == y)
  ifelse(!dir.exists(file.path("mbb/pbp")), dir.create(file.path("mbb/pbp")), FALSE)
  
  ifelse(!dir.exists(file.path("mbb/pbp/csv")), dir.create(file.path("mbb/pbp/csv")), FALSE)
  data.table::fwrite(pbp_g, file=paste0("mbb/pbp/csv/play_by_play_",y,".csv.gz"))
  
  ifelse(!dir.exists(file.path("mbb/pbp/qs")), dir.create(file.path("mbb/pbp/qs")), FALSE)
  qs::qsave(pbp_g,glue::glue("mbb/pbp/qs/play_by_play_{y}.qs"))
  
  ifelse(!dir.exists(file.path("mbb/pbp/rds")), dir.create(file.path("mbb/pbp/rds")), FALSE)
  saveRDS(pbp_g,glue::glue("mbb/pbp/rds/play_by_play_{y}.rds"))
  
  ifelse(!dir.exists(file.path("mbb/pbp/parquet")), dir.create(file.path("mbb/pbp/parquet")), FALSE)
  arrow::write_parquet(pbp_g, glue::glue("mbb/pbp/parquet/play_by_play_{y}.parquet"))
})
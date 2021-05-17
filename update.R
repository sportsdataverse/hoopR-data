library(kenpomR)
library(tidyverse)
library(rvest)
library(xml2)
library(dplyr)
library(stringr)
library(tidyr)
library(stringr)
library(xml2)
library(git2r)
source("pull_team_page.R")
teams = kenpomR::teams_links$Team
yr=2021

browser <- kenpomR::login(Sys.getenv("kp_user"),Sys.getenv("kp_pw"))
gather_team_pages(browser, year = yr, fp="data")
# New Year Data Repo Recreation
repo <- git2r::repository('./') # Set up connection to repository folder
cred <- git2r::cred_token()
version <- "0.2.3"

git2r::add(repo, glue::glue("data/*"))
git2r::commit(repo, message = glue::glue("Updated Team Pages {Sys.time()} using kenpomR version {version}")) # commit the staged files with the chosen message
git2r::pull(repo) # pull repo (and pray there are no merge commits)
git2r::push(repo, credentials = git2r::cred_user_pass()) # push commit

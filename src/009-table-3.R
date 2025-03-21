
# Setup ---------------------------------------------------------------------------------
#load libraries
library(glue)
library(lubridate)
library(tidyverse)

#load helper scripts
source("src/-Global-Parameters.R")
source("src/utils.R")
source("src/ZZ-matching-functions.R")



table_data <- read_parquet(glue("{data_path}/t1-KNN-peer-data.parquet")) |>   
  filter(Subj.MVE > 10)

table_data |> distinct(Subj.gvkey, Subj.datadate) |> count()

# Panel A
descriptives_a <- table_data |>
  mutate(diff.ACC    = Subj.ACC - Peer.ACC,
         Subj.Growth = Subj.EARN - Subj.EARN_lag_1,
         Peer.Growth = Peer.EARN - Peer.EARN_lag_1,
         diff.Growth = Subj.Growth - Peer.Growth,
         diff.EARN   = Subj.EARN - Peer.EARN,
         #diff.MVE    = Peer.MVE / Subj.MVE,
         diff.age    = Subj.age - Peer.age,
         diff.years  = floor(time_length(interval(Peer.datadate,Subj.datadate),
                                         "years")),
         
  ) |> 
  mutate(across(starts_with("diff."), winsorize_x)) |> 
  summarize(across(starts_with("diff."),
                   list(N = ~ n(),
                        Mean = ~ mean(.x, na.rm = TRUE),
                        StD = ~ sd(.x, na.rm = TRUE),  
                        #Min = ~ min(.x, na.rm=T),
                        #Max = ~ max(.x, na.rm=T),
                        P05 = ~ quantile(.x, 0.05, na.rm=T),
                        P25 = ~ quantile(.x, 0.25, na.rm=T),
                        Med = ~ median(.x, na.rm=T),
                        P75 = ~ quantile(.x, 0.75, na.rm=T),
                        P95 = ~ quantile(.x, 0.95, na.rm=T)
                   )
  )
  , .groups = "drop")

descriptives_panela <- descriptives_a |> 
  pivot_longer(
    everything(),
    names_to = c("variable",".value"),
    names_pattern = "(.+)_(.+)"
  ) |> 
  mutate_if(is.numeric, round, digit = 2) |> 
  mutate(order = case_when(
    variable == "diff.EARN"   ~ 1,
    variable == "diff.ACC"    ~ 2,
    variable == "diff.Growth" ~ 3,
    variable == "diff.years"  ~ 4,
    variable == "diff.age"    ~ 5)) |>
  arrange(order) |>
  select(-order)

write_csv(descriptives_panela, glue("out/Table-3A.csv"), na = '')


# Panel B
descriptives_b <- table_data |>
  mutate(
    Peer.IBES_covered = if_else(is.na(Peer.IBES_covered), FALSE, TRUE),
    Subj.IBES_covered = if_else(is.na(Subj.IBES_covered), FALSE, TRUE)
  ) |>
  mutate(same.ff12   = Subj.FF12      == Peer.FF12,
         same.sic2   = Subj.sic2      == Peer.sic2,
         same.gics   = Subj.gics      == Peer.gics,
         same.gvkey  = Subj.gvkey     == Peer.gvkey,
         same.lcycle = Subj.lifecycle == Peer.lifecycle
  ) |> 
  mutate(across(starts_with("same."), winsorize_x)) |> 
  summarize(across(starts_with(c("same.", "follow.")),
                   list(N = ~ n(),
                        Mean = ~ mean(.x, na.rm = TRUE) * 100
                   )
  )
  , .groups = "drop")


size_deciles <- read_parquet(glue("{data_path}/data_M2.parquet")) |> 
  mutate(year = lubridate::year(datadate)) |> 
  group_by(year) |> 
  summarize_at(vars(MVE),
               list(P01 = ~quantile(., probs=0.1, na.rm = TRUE),
                    P02 = ~quantile(., probs=0.2, na.rm = TRUE),
                    P03 = ~quantile(., probs=0.3, na.rm = TRUE),
                    P04 = ~quantile(., probs=0.4, na.rm = TRUE),
                    P05 = ~quantile(., probs=0.5, na.rm = TRUE),
                    P06 = ~quantile(., probs=0.6, na.rm = TRUE),
                    P07 = ~quantile(., probs=0.7, na.rm = TRUE),
                    P08 = ~quantile(., probs=0.8, na.rm = TRUE),
                    P09 = ~quantile(., probs=0.9, na.rm = TRUE)
               )) |>
  ungroup()

same_size <- data.frame(t(table_data |> 
                            mutate(year = lubridate::year(Subj.datadate)) |> 
                            inner_join(size_deciles, by = "year") |> 
                            mutate(subj_size_decile = case_when(
                              Subj.MVE < P01 ~  1L,
                              Subj.MVE < P02 ~  2L,
                              Subj.MVE < P03 ~  3L,
                              Subj.MVE < P04 ~  4L,
                              Subj.MVE < P05 ~  5L,
                              Subj.MVE < P06 ~  6L,
                              Subj.MVE < P07 ~  7L,
                              Subj.MVE < P08 ~  8L,
                              Subj.MVE < P09 ~  9L,
                              TRUE           ~ 10L
                            ),
                            peer_size_decile = case_when(
                              Peer.MVE < P01 ~  1L,
                              Peer.MVE < P02 ~  2L,
                              Peer.MVE < P03 ~  3L,
                              Peer.MVE < P04 ~  4L,
                              Peer.MVE < P05 ~  5L,
                              Peer.MVE < P06 ~  6L,
                              Peer.MVE < P07 ~  7L,
                              Peer.MVE < P08 ~  8L,
                              Peer.MVE < P09 ~  9L,
                              TRUE           ~ 10L
                            ),
                            same_size = if_else(
                              subj_size_decile == peer_size_decile, 100, 0)) |> 
                            summarize(across(starts_with("same"),
                                             list(N = ~ n(),
                                                  Mean = ~ mean(.x, na.rm = TRUE)
                                             )), 
                                      .groups = "drop") |> 
                            mutate_if(is.numeric, round, digit = 2) |> 
                            mutate(variable = "same.size") |> 
                            select(variable, same_size_N, same_size_Mean)))


descriptives_panelb <- as.data.frame(t(descriptives_b |> 
                                         pivot_longer(
                                           everything(),
                                           names_to = c("variable",".value"),
                                           names_pattern = "(.+)_(.+)"
                                         ) |>  
                                         mutate_if(is.numeric, round, digit = 2))) |> 
  cbind(same_size)

write_csv(descriptives_panelb, glue("out/Table-3B.csv"), na = '')



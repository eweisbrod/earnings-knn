
# Setup ---------------------------------------------------------------------------------
library(glue)
library(sandwich)
library(lmtest)
library(tidyverse)
library(lubridate)

# load helper scripts
source("src/-Global-Parameters.R")
source("src/utils.R")

min_year_comp_sample <- 1998L
max_year_comp_sample <- 2018L

fcast_horizon <- 1L
estimation <- "OLS"


nw_coef_std <- function(data, lags) {
  
  coeff_names <- data |> 
    distinct(term)
  
  coef_out <- list()
  
  for (i in 1:nrow(coeff_names)) {
    
    dta <- data |>
      filter(term == paste0(coeff_names[i,1]))
    
    nw <- sandwich::NeweyWest(lm(estimate ~ 1, data = dta),
                              lag = lags, prewhite = FALSE, adjust = TRUE)
    
    lm <- lm(estimate ~ 1, data = dta)
    
    coef <- lmtest::coeftest(lm, vcov = nw)
    
    out <- tibble(coefficient = coef[1,1], tvalue = coef[1,3]) |> 
      mutate_if(is.numeric, round, digits = 2) |> 
      mutate(term = paste0(coeff_names[i,1]))
    
    coef_out[[i]] <- out
    
  }
  
  coeff_out <- bind_rows(coef_out)
  
  return(coeff_out)
  
}


# Table 2A ---------------------------------------------------------------------

samp_data <- read_parquet(glue("{data_path}/data_M1.parquet")) |> 
  filter(datadate >= testing_datelist[1] - years(roll_win_size),
         (datadate <= testing_datelist[[length(testing_datelist) - 12 * fcast_horizon]])) |> 
  rename(TA=AT_HVZ, DIV = D_HVZ, ACC = ACC_HVZ)  |>
  mutate(DD   = DIV > 0,
         LOSS = EARN < 0)

samp_desc_1 <- samp_data |>  
  filter(!is.na(EARN_lead_1),
         abs(EARN) < 1,
         abs(EARN_lead_1) < 1,
         !is.na(ACC),
         month(datadate_lead_1) == month(datadate),
         year(datadate_lead_1)  == year(datadate) + fcast_horizon) |>
  descs(EARN, LOSS, ACC, TA, DD, DIV) |> 
  mutate(order = case_when(
    Variable == "ACC"  ~ 1,
    Variable == "DD"   ~ 2,
    Variable == "DIV"  ~ 3,
    Variable == "EARN" ~ 4,
    Variable == "LOSS" ~ 5,
    Variable == "TA"   ~ 6
  )) |>
  arrange(order) |>
  select(-order)

samp_desc_2 <- samp_data |> 
  filter(!is.na(EARN_lead_1),
         abs(EARN) < 1,
         abs(EARN_lead_1) < 1,
         month(datadate_lead_1) == month(datadate),
         year(datadate_lead_1)  == year(datadate) + fcast_horizon) |>
  filter(!is.na(PM),
         !is.na(ATO),
         !is.na(LEV),
         !is.na(SG),
         !is.na(at) & at > 0,
         !is.na(MVE) & MVE > 0) |> 
  descs(PM, ATO, LEV, SG) |> 
  mutate(order = case_when(
    Variable == "PM"  ~ 1,
    Variable == "ATO"  ~ 2,
    Variable == "LEV" ~ 3,
    Variable == "SG"  ~ 4)) |>
  arrange(order) |>
  select(-order)

samp_desc <- bind_rows(samp_desc_1, samp_desc_2) |> 
  select(-Min, -Max)


write_csv2(samp_desc,
           glue("out/t{fcast_horizon}-table-2a.csv"))


# Table 2B ---------------------------------------------------------------------

rbhvz_coefs <- read_csv(
  glue("out/t{fcast_horizon}-{estimation}-HVZ-coeffs.csv")) |> 
  mutate(term = if_else(term == "(Intercept)", "Intercept", term))

hvz_data <- 
  rbhvz_coefs |> 
  filter(fcst_date >= testing_datelist[1],
         (fcst_date <= testing_datelist[[length(testing_datelist) - 12 * fcast_horizon]])) |> 
  filter(term != "stat_nobs",
         term != "stat_r2")

hvz_out <- hvz_data |> 
  nw_coef_std(12)

hvz_stats <- 
  rbhvz_coefs |> 
  filter(fcst_date >= testing_datelist[1],
         (fcst_date <= testing_datelist[[length(testing_datelist) - 12 * fcast_horizon]])) |> 
  filter(term == "stat_nobs" | term == "stat_r2") |> 
  group_by(Model, term) |> 
  summarize(coefficient  = mean(estimate)) |> 
  ungroup() |> 
  mutate(tvalue = NA_real_) |> 
  mutate_if(is.numeric, round, digits = 2) |> 
  select(-Model)

coeff_table_2b <- as.data.frame(t(rbind(hvz_out, hvz_stats) |> 
                                    mutate(order = case_when(
                                      term == "stat_nobs" ~ 1,
                                      term == "Intercept" ~ 2,
                                      term == "ACC"       ~ 3,
                                      term == "DDTRUE"    ~ 4,
                                      term == "DIV"       ~ 5,
                                      term == "EARN"      ~ 6,
                                      term == "LOSSTRUE"  ~ 7,
                                      term == "TA"        ~ 8,
                                      term == "stat_r2"   ~ 9)) |>
                                    arrange(order) |>
                                    select(-order)))

write_csv2(coeff_table_2b,
           glue("out/t{fcast_horizon}-table-2b.csv"))


# Table 2C ---------------------------------------------------------------------

rbep_coefs <- read_csv(
  glue("out/t{fcast_horizon}-{estimation}-EP-ALL-coeffs.csv")) |> 
  mutate(term = if_else(term == "(Intercept)", "Intercept", term),
         term = if_else(term == "LOSSTRUE:EARN", "LOSSTRUEEARN", term))


ep_data <- 
  rbep_coefs |> 
  filter(fcst_date >= testing_datelist[1],
         (fcst_date <= testing_datelist[[length(testing_datelist) - 12 * fcast_horizon]])) |> 
  filter(term != "stat_nobs",
         term != "stat_r2")

ep_out <- ep_data |> 
  nw_coef_std(12)


ep_stats <- 
  rbep_coefs |> 
  filter(fcst_date >= testing_datelist[1],
         (fcst_date <= testing_datelist[[length(testing_datelist) - 12 * fcast_horizon]])) |> 
  filter(term == "stat_nobs" | term == "stat_r2") |> 
  group_by(Model, term) |> 
  summarize(coefficient  = mean(estimate)) |> 
  ungroup() |> 
  mutate(tvalue = NA_real_) |> 
  mutate_if(is.numeric, round, digits = 2) |> 
  select(-Model)

coeff_table_2c1 <- as.data.frame(t(rbind(ep_out, ep_stats) |> 
                                     mutate(order = case_when(
                                       term == "stat_nobs"    ~ 1,
                                       term == "Intercept"    ~ 2,
                                       term == "EARN"         ~ 3,
                                       term == "LOSSTRUE"     ~ 4,
                                       term == "LOSSTRUEEARN" ~ 5,
                                       term == "stat_r2"      ~ 6)) |>
                                     arrange(order) |>
                                     select(-order)))

write_csv2(coeff_table_2c1,
           glue("out/t{fcast_horizon}-table-2c1.csv"))


rblc_coefs <- read_csv(
  glue("out/t{fcast_horizon}-{estimation}-EP-LCYCLE-coeffs.csv")) |> 
  mutate(term = if_else(term == "(Intercept)", "Intercept", term),
         term = if_else(term == "LOSSTRUE:EARN", "LOSSTRUEEARN", term))

eplife_data <- 
  rblc_coefs |> 
  filter(fcst_date >= testing_datelist[1],
         (fcst_date <= testing_datelist[[length(testing_datelist) - 12 * fcast_horizon]])) |> 
  filter(term != "stat_nobs",
         term != "stat_r2")

eplife_coef <- list()

for (i in 1:5) {
  
  eplife_out <- eplife_data |> 
    filter(Partition == i) |> 
    nw_coef_std(12) |> 
    mutate(lc = i)
  
  eplife_coef[[i]] <- eplife_out
  
}

eplife_coefficients <- bind_rows(eplife_coef)

eplife_stats <- 
  rblc_coefs |> 
  filter(fcst_date >= testing_datelist[1],
         (fcst_date <= testing_datelist[[length(testing_datelist) - 12 * fcast_horizon]])) |> 
  filter(term == "stat_nobs" | term == "stat_r2") |> 
  group_by(Model, Partition, term) |> 
  summarize(coefficient  = mean(estimate)) |> 
  ungroup() |> 
  mutate(tvalue = NA_real_) |> 
  mutate_if(is.numeric, round, digits = 2) |> 
  select(-Model) |> 
  rename(lc = Partition)

coeff_table_2c2_lc1 <- as.data.frame(t(rbind(eplife_coefficients |> filter(lc == 1),
                                             eplife_stats |> filter(lc == 1)) |> 
                                         mutate(order = case_when(
                                           term == "stat_nobs"    ~ 1,
                                           term == "Intercept"    ~ 2,
                                           term == "EARN"         ~ 3,
                                           term == "LOSSTRUE"     ~ 4,
                                           term == "LOSSTRUEEARN" ~ 5,
                                           term == "stat_r2"      ~ 6)) |>
                                         arrange(order) |>
                                         select(-order) |> 
                                         select(-lc))) |>
  mutate(lc = 1)

coeff_table_2c2_lc2 <- as.data.frame(t(rbind(eplife_coefficients |> filter(lc == 2),
                                             eplife_stats |> filter(lc == 2)) |> 
                                         mutate(order = case_when(
                                           term == "stat_nobs"    ~ 1,
                                           term == "Intercept"    ~ 2,
                                           term == "EARN"         ~ 3,
                                           term == "LOSSTRUE"     ~ 4,
                                           term == "LOSSTRUEEARN" ~ 5,
                                           term == "stat_r2"      ~ 6)) |>
                                         arrange(order) |>
                                         select(-order, - term) |> 
                                         select(-lc))) |> 
  mutate(lc = 2)

coeff_table_2c2_lc3 <- as.data.frame(t(rbind(eplife_coefficients |> filter(lc == 3),
                                             eplife_stats |> filter(lc == 3)) |> 
                                         mutate(order = case_when(
                                           term == "stat_nobs"    ~ 1,
                                           term == "Intercept"    ~ 2,
                                           term == "EARN"         ~ 3,
                                           term == "LOSSTRUE"     ~ 4,
                                           term == "LOSSTRUEEARN" ~ 5,
                                           term == "stat_r2"      ~ 6)) |>
                                         arrange(order) |>
                                         select(-order, - term) |> 
                                         select(-lc))) |> 
  mutate(lc = 3)

coeff_table_2c2_lc4 <- as.data.frame(t(rbind(eplife_coefficients |> filter(lc == 4),
                                             eplife_stats |> filter(lc == 4)) |> 
                                         mutate(order = case_when(
                                           term == "stat_nobs"    ~ 1,
                                           term == "Intercept"    ~ 2,
                                           term == "EARN"         ~ 3,
                                           term == "LOSSTRUE"     ~ 4,
                                           term == "LOSSTRUEEARN" ~ 5,
                                           term == "stat_r2"      ~ 6)) |>
                                         arrange(order) |>
                                         select(-order, - term) |> 
                                         select(-lc))) |> 
  mutate(lc = 4)

coeff_table_2c2_lc5 <- as.data.frame(t(rbind(eplife_coefficients |> filter(lc == 5),
                                             eplife_stats |> filter(lc == 5)) |> 
                                         mutate(order = case_when(
                                           term == "stat_nobs"    ~ 1,
                                           term == "Intercept"    ~ 2,
                                           term == "EARN"         ~ 3,
                                           term == "LOSSTRUE"     ~ 4,
                                           term == "LOSSTRUEEARN" ~ 5,
                                           term == "stat_r2"      ~ 6)) |>
                                         arrange(order) |>
                                         select(-order, - term) |> 
                                         select(-lc))) |> 
  mutate(lc = 5)

coeff_table_2c2 <- rbind(coeff_table_2c2_lc1, coeff_table_2c2_lc2,
                         coeff_table_2c2_lc3, coeff_table_2c2_lc4,
                         coeff_table_2c2_lc5)

write_csv2(coeff_table_2c2,
           glue("{data_path}/tabs/{deflator}/t{fcast_horizon}-table-2c2.csv"))



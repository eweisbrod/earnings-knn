#' This script creates the forecast error comparison table
## coverage data is still obtained from old data files

# Setup ---------------------------------------------------------------------------------

#load libraries
library(glue)
library(tictoc)
library(broom)
library(lfe)
library(quantreg)
library(lubridate)
library(tidyverse)

# load helper scripts
source("src/-Global-Parameters.R")
source("src/utils.R")

modelnames_ols <- map_chr(ols_models, function(x) names(x))
modelnames_knn <- models
fcst_horizon <- 1


# Define pairwise function -----------------------------------------------------

pairwise_table <- function(knn_data, other_data) {
  
  other_model <- as.character(unique(other_data$Model))
  
  merged_data <- bind_rows(other_data, knn_data) |> 
    group_by(gvkey, datadate) |> 
    filter(n() == 2) |> 
    ungroup() |> 
    mutate(
      abs_error    = abs(FEBSI - fcast) / MVE * 100,
      signed_error = (FEBSI - fcast) / MVE,
      sq_error     = (signed_error)^2 * 100,
      year         = year(datadate)
    ) |>
    group_by(Model) |>
    mutate(wins_error = winsorize_x(signed_error, cut=0.001),
           wsq_error = wins_error^2) |>
    mutate(trunc_error = truncate_x(signed_error, cut=0.001),
           tsq_error = trunc_error^2 * 100) |>
    ungroup() |>
    mutate(Model = forcats::fct_relevel(Model,"KNN")) |> 
    arrange(gvkey,datadate, Model) |> 
    mutate(econ_improve = case_when(
      Model == "KNN" ~ if_else(dplyr::lead(abs_error / 100, 1L) - abs_error / 100 > .005, 1, 0),
      TRUE ~ if_else(dplyr::lag(abs_error / 100, 1L) - abs_error / 100 > .005, 1, 0)))
  
  table <- merged_data |>  
    group_by(Model) |> 
    summarise(
      N= n(),
      MAFE = mean(abs_error,na.rm=T),
      MDAFE = median(abs_error, na.rm = T),
      MSE = mean(sq_error, na.rm = T),
      TMSE = mean(tsq_error, na.rm = T),
      ECON_IMPROVE = mean(econ_improve * 100, na.rm = T)
    ) |> 
    mutate_if(is.numeric, round, digit = 2)
  
  mafe_reg <- tidy(felm(abs_error ~ Model | 0 | 0 | gvkey + year,
                        data = merged_data), se.type = "cluster")
  
  mdafe_reg <- summary(quantreg::rq(abs_error ~ Model, data=merged_data, method = "pfn"))
  
  mse_reg <- tidy(felm(sq_error ~ Model | 0 | 0 | gvkey + year,
                       data = merged_data), se.type = "cluster")
  
  tmse_reg <- tidy(felm(tsq_error ~ Model | 0 | 0 | gvkey + year,
                        data = merged_data), se.type = "cluster")
  
  econ_improve_reg <- tidy(felm(econ_improve ~ Model | 0 | 0 | gvkey + year,
                                data = merged_data), se.type = "cluster")
  
  mafe   <- as.numeric(round(mafe_reg[2,2], 2))
  mdafe  <- as.numeric(round(mdafe_reg$coefficients[2,1], 2))
  mse    <- as.numeric(round(mse_reg[2,2], 2))
  tmse   <- as.numeric(round(tmse_reg[2,2], 2))
  econ_improve <- as.numeric(round(econ_improve_reg[2,2], 4) * 100)
  
  
  row <- tibble(Model = as.character(glue("{other_model} - KNN")),
                N = NA_real_,
                MAFE = case_when(
                  as.numeric(mafe_reg[2,5]) < 0.01 ~ glue("{mafe}***"),
                  as.numeric(mafe_reg[2,5]) < 0.05 ~ glue("{mafe}**"),
                  as.numeric(mafe_reg[2,5]) < 0.10 ~ glue("{mafe}*"),
                  TRUE                             ~ glue("{mafe}")),
                MDAFE = case_when(
                  as.numeric(mdafe_reg$coefficients[2,4]) < 0.01 ~ glue("{mdafe}***"),
                  as.numeric(mdafe_reg$coefficients[2,4]) < 0.05 ~ glue("{mdafe}**"),
                  as.numeric(mdafe_reg$coefficients[2,4]) < 0.10 ~ glue("{mdafe}*"),
                  TRUE                                           ~ glue("{mdafe}")),
                MSE = case_when(
                  as.numeric(mse_reg[2,5]) < 0.01 ~ glue("{mse}***"),
                  as.numeric(mse_reg[2,5]) < 0.05 ~ glue("{mse}**"),
                  as.numeric(mse_reg[2,5]) < 0.10 ~ glue("{mse}*"),
                  TRUE                            ~ glue("{mse}")),
                TMSE = case_when(
                  as.numeric(tmse_reg[2,5]) < 0.01 ~ glue("{tmse}***"),
                  as.numeric(tmse_reg[2,5]) < 0.05 ~ glue("{tmse}**"),
                  as.numeric(tmse_reg[2,5]) < 0.10 ~ glue("{tmse}*"),
                  TRUE                             ~ glue("{tmse}")),
                ECON_IMPROVE = case_when(
                  as.numeric(econ_improve_reg[2,5]) < 0.01 ~ glue("{econ_improve}***"),
                  as.numeric(econ_improve_reg[2,5]) < 0.05 ~ glue("{econ_improve}**"),
                  as.numeric(econ_improve_reg[2,5]) < 0.10 ~ glue("{econ_improve}*"),
                  TRUE                               ~ glue("{econ_improve}")))
  
  table2 <- rbind(table,row)
  
  return(table2)
  
}


# Data --------------------------------------------------------------------

# We still cannot get rid of this fully because of EBSI. 
data_filter <-
  read_parquet(glue("{data_path}/data0.parquet")) |>
  mutate(FEBSI = .data[[glue("EBSI_lead_{fcst_horizon}")]],
         Fdatadate = .data[[paste0("datadate_lead_", fcst_horizon)]]
  ) |>
  # additional filter for non-missing forward EARN
  filter(
    !is.na(FEBSI),
    month(Fdatadate) == month(datadate),
    year(Fdatadate) == year(datadate) + fcst_horizon,
    datadate >= testing_datelist[[1]],
    datadate <= testing_datelist[[(length(testing_datelist) - 12 * fcst_horizon)]],
    # Screen 1
    MVE > min_mve,
    # Screen 2
    abs(EBSI/MVE) < 1
  ) |>
  select(gvkey, datadate)


# TABLE 4 ######################################################################

#read forecast data
knn     <- read_parquet(glue("{data_path}//t{fcst_horizon}-KNN-fcasts.parquet")) |> 
  inner_join(data_filter)

knndup1 <- read_parquet(glue("{data_path}/t{fcst_horizon}-KNNDUP1-fcasts.parquet")) |> 
  inner_join(data_filter)

knndup2 <- read_parquet(glue("{data_path}/t{fcst_horizon}-KNNDUP2-fcasts.parquet")) |> 
  inner_join(data_filter)

knndup3 <- read_parquet(glue("{data_path}/t{fcst_horizon}-KNNDUP3-fcasts.parquet")) |> 
  inner_join(data_filter)

knndup4 <- read_parquet(glue("{data_path}/t{fcst_horizon}-KNNDUP4-fcasts.parquet")) |> 
  inner_join(data_filter)

knnhvz1 <- read_parquet(glue("{data_path}/t{fcst_horizon}-KNNHVZ1-fcasts.parquet")) |> 
  inner_join(data_filter)

knnhvz2 <- read_parquet(glue("{data_path}/t{fcst_horizon}-KNNHVZ2-fcasts.parquet")) |> 
  inner_join(data_filter)

knnhvz3 <- read_parquet(glue("{data_path}/t{fcst_horizon}-KNNHVZ3-fcasts.parquet")) |> 
  inner_join(data_filter)

knnhvz4 <- read_parquet(glue("{data_path}/t{fcst_horizon}-KNNHVZ4-fcasts.parquet")) |> 
  inner_join(data_filter)


knndup1_table     <- pairwise_table(knn, knndup1)

knndup2_table     <- pairwise_table(knn, knndup2)

knndup3_table     <- pairwise_table(knn, knndup3)

knndup4_table     <- pairwise_table(knn, knndup4)

knnhvz1_table     <- pairwise_table(knn, knnhvz1)

knnhvz2_table     <- pairwise_table(knn, knnhvz2)

knnhvz3_table     <- pairwise_table(knn, knnhvz3)

knnhvz4_table     <- pairwise_table(knn, knnhvz4)


out_table <- rbind(knndup1_table, knndup2_table, knndup3_table, knndup4_table,
                   knnhvz1_table, knnhvz2_table, knnhvz3_table, knnhvz4_table)

write_csv2(out_table, glue("out/Table-4.csv"), na = '')

rm(knn, knndup1, knndup2, knndup3, knndup4, knnhvz1, knnhvz2, knnhvz3, knnhvz4,
   knndup1_table, knndup2_table, knndup3_table, knndup4_table,
   knnhvz1_table, knnhvz2_table, knnhvz3_table, knnhvz4_table, out_table)



# TABLE 5 ######################################################################

#read forecast data
knn <- read_parquet(glue("{data_path}/t{fcst_horizon}-KNN-fcasts.parquet")) |> 
  inner_join(data_filter)

rw <- read_parquet(glue("{data_path}/t{fcst_horizon}-RW-fcasts.parquet")) |> 
  inner_join(data_filter)

bcg <- read_parquet(glue("{data_path}/t{fcst_horizon}-BCG-fcasts.parquet")) |> 
  inner_join(data_filter)

hvz <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-HVZ-fcasts.parquet")) |> 
  inner_join(data_filter)

epall <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-EP-ALL-fcasts.parquet")) |> 
  inner_join(data_filter)

epgics <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-EP-GIC-fcasts.parquet")) |> 
  inner_join(data_filter)

eplife <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-EP-LCYCLE-fcasts.parquet")) |> 
  inner_join(data_filter)


rw_table     <- pairwise_table(knn, rw)

bcg_table    <- pairwise_table(knn, bcg)

hvz_table    <- pairwise_table(knn, hvz)

ep_table     <- pairwise_table(knn, epall)

epgic_table  <- pairwise_table(knn, epgics)

eplife_table <- pairwise_table(knn, eplife)

out_table <- rbind(rw_table, bcg_table, hvz_table, ep_table, epgic_table, eplife_table)


write_csv2(out_table, glue("out/Table-5.csv"), na = '')

rm(knn, rw, bcg, hvz, epall, epgics, eplife, rw_table, bcg_table, hvz_table,
   ep_table, epgic_table, eplife_table, out_table)



# TABLE 6A #####################################################################

#read forecast data
ibes_fcasts_t1 <- read_parquet("out/data/t1-analyst-fcasts.parquet") |> 
  select(gvkey, datadate)

knn_cov <- read_parquet(glue("{data_path}/t{fcst_horizon}-KNN-fcasts.parquet")) |>
  semi_join(ibes_fcasts_t1, by = c("gvkey", "datadate")) |> 
  inner_join(data_filter)

knn_nocov <- read_parquet(glue("{data_path}/t{fcst_horizon}-KNN-fcasts.parquet")) |>
  anti_join(ibes_fcasts_t1, by = c("gvkey", "datadate")) |> 
  inner_join(data_filter)

rw_cov <- read_parquet(glue("{data_path}/t{fcst_horizon}-RW-fcasts.parquet")) |> 
  semi_join(ibes_fcasts_t1, by = c("gvkey", "datadate")) |> 
  inner_join(data_filter)

rw_nocov <- read_parquet(glue("{data_path}/t{fcst_horizon}-RW-fcasts.parquet")) |> 
  anti_join(ibes_fcasts_t1, by = c("gvkey", "datadate")) |> 
  inner_join(data_filter)

hvz_cov <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-HVZ-fcasts.parquet")) |> 
  semi_join(ibes_fcasts_t1, by = c("gvkey", "datadate")) |> 
  inner_join(data_filter)

hvz_nocov <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-HVZ-fcasts.parquet")) |> 
  anti_join(ibes_fcasts_t1, by = c("gvkey", "datadate")) |> 
  inner_join(data_filter)

eplife_cov <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-EP-LCYCLE-fcasts.parquet")) |> 
  semi_join(ibes_fcasts_t1, by = c("gvkey", "datadate")) |> 
  inner_join(data_filter)

eplife_nocov <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-EP-LCYCLE-fcasts.parquet")) |> 
  anti_join(ibes_fcasts_t1, by = c("gvkey", "datadate")) |> 
  inner_join(data_filter)


rw_cov_table       <- pairwise_table(knn_cov, rw_cov)

hvz_cov_table      <- pairwise_table(knn_cov, hvz_cov)

eplife_cov_table   <- pairwise_table(knn_cov, eplife_cov)

out_table_cov      <- rbind(rw_cov_table, hvz_cov_table, eplife_cov_table)

rw_nocov_table     <- pairwise_table(knn_nocov, rw_nocov)

hvz_nocov_table    <- pairwise_table(knn_nocov, hvz_nocov)

eplife_nocov_table <- pairwise_table(knn_nocov, eplife_nocov)

out_table_nocov    <- rbind(rw_nocov_table, hvz_nocov_table, eplife_nocov_table)

out_table <- 
  rbind(
    out_table_cov |>  mutate(Panel = "(a) t+1 forecast error with analyst coverage"), 
    out_table_nocov |>  mutate(Panel = "(b) t+1 forecast error without analyst coverage")
  )

write_csv2(out_table, glue("{data_path}/tabs/{deflator}/T6a.csv"), na = '')

rm(ibes_fcasts_t1, knn_cov, knn_nocov, rw_cov, rw_nocov, hvz_cov, hvz_nocov,
   eplife_cov, eplife_nocov, rw_cov_table, hvz_cov_table, eplife_cov_table,
   out_table_cov, rw_nocov_table, hvz_nocov_table, eplife_nocov_table,
   out_table_nocov, out_table)


# TABLE 6B #####################################################################

fcst_horizon <- 1

#read forecast data
knn_t1 <- read_parquet(glue("{data_path}/t{fcst_horizon}-KNN-fcasts.parquet")) |>
  inner_join(data_filter)

rw_t1 <- read_parquet(glue("{data_path}/t{fcst_horizon}-RW-fcasts.parquet")) |> 
  inner_join(data_filter)

hvz_t1 <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-HVZ-fcasts.parquet")) |> 
  inner_join(data_filter)

eplife_t1 <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-EP-LCYCLE-fcasts.parquet")) |> 
  inner_join(data_filter)

fcst_horizon <- 2

data_filter_t2 <-
  read_parquet(glue("{data_path}/data0.parquet")) |>
  mutate(FEBSI = .data[[glue("EBSI_lead_{fcst_horizon}")]],
         # Fcalyear = .data[[glue("calyear_lead_{fcst_horizon}")]],
         Fdatadate = .data[[paste0("datadate_lead_", fcst_horizon)]]
  ) |>
  # additional filter for non-missing forward EARN
  filter(
    !is.na(FEBSI),
    # Fcalyear == calyear + fcst_horizon,
    month(Fdatadate) == month(datadate),
    year(Fdatadate) == year(datadate) + fcst_horizon,
    datadate >= testing_datelist[[1]],
    datadate <= testing_datelist[[(length(testing_datelist) - 12 * fcst_horizon)]],
    # datadate >= min_test_date,
    # datadate <= max_test_date - fcst_horizon,
    # Screen 1
    MVE > min_mve,
    # Screen 2
    #  at > 10,
    #  sale > 10,
    #  B > 1
    # abs(EBSI/sale) < 1,
    # abs(sale/sale_lag_1-1) < 1)
    # Screen 3
    abs(EBSI/MVE) < 1
  ) |>
  select(gvkey, datadate)

knn_t2 <- read_parquet(glue("{data_path}/t{fcst_horizon}-KNN-fcasts.parquet")) |>
  inner_join(data_filter_t2)

rw_t2 <- read_parquet(glue("{data_path}/t{fcst_horizon}-RW-fcasts.parquet")) |> 
  inner_join(data_filter_t2)

hvz_t2 <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-HVZ-fcasts.parquet")) |> 
  inner_join(data_filter_t2)

eplife_t2 <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-EP-LCYCLE-fcasts.parquet")) |> 
  inner_join(data_filter_t2)

fcst_horizon <- 3

data_filter_t3 <-
  read_parquet(glue("{data_path}/data0.parquet")) |>
  mutate(FEBSI = .data[[glue("EBSI_lead_{fcst_horizon}")]],
         # Fcalyear = .data[[glue("calyear_lead_{fcst_horizon}")]],
         Fdatadate = .data[[paste0("datadate_lead_", fcst_horizon)]]
  ) |>
  # additional filter for non-missing forward EARN
  filter(
    !is.na(FEBSI),
    # Fcalyear == calyear + fcst_horizon,
    month(Fdatadate) == month(datadate),
    year(Fdatadate) == year(datadate) + fcst_horizon,
    datadate >= testing_datelist[[1]],
    datadate <= testing_datelist[[(length(testing_datelist) - 12 * fcst_horizon)]],
    # datadate >= min_test_date,
    # datadate <= max_test_date - fcst_horizon,
    # Screen 1
    MVE > min_mve,
    # Screen 2
    #  at > 10,
    #  sale > 10,
    #  B > 1
    # abs(EBSI/sale) < 1,
    # abs(sale/sale_lag_1-1) < 1)
    # Screen 3
    abs(EBSI/MVE) < 1
  ) |>
  select(gvkey, datadate)

knn_t3 <- read_parquet(glue("{data_path}/t{fcst_horizon}-KNN-fcasts.parquet")) |>
  inner_join(data_filter_t3)

rw_t3 <- read_parquet(glue("{data_path}/t{fcst_horizon}-RW-fcasts.parquet")) |> 
  inner_join(data_filter_t3)

hvz_t3 <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-HVZ-fcasts.parquet")) |> 
  inner_join(data_filter_t3)

eplife_t3 <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-EP-LCYCLE-fcasts.parquet")) |> 
  inner_join(data_filter_t3)


rw_t2_table     <- pairwise_table(knn_t2, rw_t2)

hvz_t2_table    <- pairwise_table(knn_t2, hvz_t2)

eplife_t2_table <- pairwise_table(knn_t2, eplife_t2)

out_table_t2    <- rbind(rw_t2_table, hvz_t2_table, eplife_t2_table)

rm(rw_t2_table, hvz_t2_table, eplife_t2_table)


rw_t3_table     <- pairwise_table(knn_t3, rw_t3)

hvz_t3_table    <- pairwise_table(knn_t3, hvz_t3)

eplife_t3_table <- pairwise_table(knn_t3, eplife_t3)

out_table_t3    <- rbind(rw_t3_table, hvz_t3_table, eplife_t3_table)

rm(rw_t3_table, hvz_t3_table, eplife_t3_table)


knn_agg <- knn_t1 |> 
  rename(fcast_t1 = fcast,
         FEBSI_t1 = FEBSI) |> 
  inner_join(select(knn_t2, gvkey, datadate, fcast_t2 = fcast, FEBSI_t2 = FEBSI),
             by = c("gvkey", "datadate")) |> 
  inner_join(select(knn_t3, gvkey, datadate, fcast_t3 = fcast, FEBSI_t3 = FEBSI),
             by = c("gvkey", "datadate")) |> 
  mutate(fcast = fcast_t1 + fcast_t2 + fcast_t3,
         FEBSI = FEBSI_t1 + FEBSI_t2 + FEBSI_t3)

rw_agg <- rw_t1 |> 
  rename(fcast_t1 = fcast,
         FEBSI_t1 = FEBSI) |> 
  inner_join(select(rw_t2, gvkey, datadate, fcast_t2 = fcast, FEBSI_t2 = FEBSI),
             by = c("gvkey", "datadate")) |> 
  inner_join(select(rw_t3, gvkey, datadate, fcast_t3 = fcast, FEBSI_t3 = FEBSI),
             by = c("gvkey", "datadate")) |> 
  mutate(fcast = fcast_t1 + fcast_t2 + fcast_t3,
         FEBSI = FEBSI_t1 + FEBSI_t2 + FEBSI_t3)

hvz_agg <- hvz_t1 |> 
  rename(fcast_t1 = fcast,
         FEBSI_t1 = FEBSI) |> 
  inner_join(select(hvz_t2, gvkey, datadate, fcast_t2 = fcast, FEBSI_t2 = FEBSI),
             by = c("gvkey", "datadate")) |> 
  inner_join(select(hvz_t3, gvkey, datadate, fcast_t3 = fcast, FEBSI_t3 = FEBSI),
             by = c("gvkey", "datadate")) |> 
  mutate(fcast = fcast_t1 + fcast_t2 + fcast_t3,
         FEBSI = FEBSI_t1 + FEBSI_t2 + FEBSI_t3)

eplife_agg <- eplife_t1 |> 
  rename(fcast_t1 = fcast,
         FEBSI_t1 = FEBSI) |> 
  inner_join(select(eplife_t2, gvkey, datadate, fcast_t2 = fcast, FEBSI_t2 = FEBSI),
             by = c("gvkey", "datadate")) |> 
  inner_join(select(eplife_t3, gvkey, datadate, fcast_t3 = fcast, FEBSI_t3 = FEBSI),
             by = c("gvkey", "datadate")) |> 
  mutate(fcast = fcast_t1 + fcast_t2 + fcast_t3,
         FEBSI = FEBSI_t1 + FEBSI_t2 + FEBSI_t3)

rw_agg_table     <- pairwise_table(knn_agg, rw_agg)

hvz_agg_table    <- pairwise_table(knn_agg, hvz_agg)

eplife_agg_table <- pairwise_table(knn_agg, eplife_agg)

out_table_agg    <- rbind(rw_agg_table, hvz_agg_table, eplife_agg_table)

rm(rw_agg_table, hvz_agg_table, eplife_agg_table)

out_table <- 
  rbind(
    out_table_t2  |>  mutate(Panel = "(a) t+2 forecast error"), 
    out_table_t3 |>  mutate(Panel = "(b) t+3 forecast error"), 
    out_table_agg  |>  mutate(Panel = "(c) Aggregate forecast error (t+1 + t+2 + t+3)")
  )

write_csv2(out_table, glue("{data_path}/tabs/{deflator}/T6b.csv"), na = '')


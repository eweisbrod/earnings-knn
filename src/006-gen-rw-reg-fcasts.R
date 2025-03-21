# This script computes regression based forecasting models
# as well as the Random walk forecast for a given scalar (currently MVE) 
# and forecast horizons (t+1, t+2, t+3).
# It also computes both normal OLS and MEDIAN regression forecasts.

# Setup ------------------------------------------------------------------------

#load libraries
library(glue)
library(tidyverse)
library(quantreg)
library(tictoc)
library(furrr)
library(lubridate)

# load helper scripts
source("src/-Global-Parameters.R")
source("src/utils.R")


fcast_horizons <- c(1, 2, 3)  # applies to all models being generated
# estimation_type <- c("OLS", "MEDIAN")  # OLS versus Median regression
estimation_type <- c("OLS")  # Only OLS for now
min_n_losses <- 10  # Making sure industry, or lifecycle cells have enough obs



# Loading Data here rather than IO across hundred workers
# data_ols2 <- read_parquet(glue("{data_path}/data_ols.parquet"))
data_ols <- read_parquet(glue("{data_path}/data_M1.parquet")) |> 
  rename(TA=AT_HVZ, DIV = D_HVZ, ACC = ACC_HVZ)  |>  
  mutate(
    # TA        = at   / Scalar,
    # DIV       = D  / Scalar,
    DD        = DIV > 0,
    # ACC       = acc_HVZ  / Scalar,
    # BV        = B    / Scalar,
    # LOSS      = EBSI < 0
    LOSS      = EARN < 0
  )

# Random walk forecast ---------------------------------------------------------
for (fhorizon in fcast_horizons){
  rw_fcasts <-
    read_parquet(glue('{data_path}/data0.parquet')) |>
    filter(
      !is.na(.data[[deflator]]) & .data[[deflator]] > 0,
      !is.na(MVE) & MVE  > 0,  # always filter for MVE, even if not deflator
      !is.na(EBSI),
      !(sic4 %in% (4900:4999)),
      !(sic4 %in% (6000:6999))
    ) |>
    mutate(
      Model = "RW",
      FEBSI = .data[[glue("EBSI_lead_{fhorizon}")]]
    ) |>
    select(gvkey, datadate, Model, fcast=EBSI, FEBSI, MVE)
  
  write_parquet(rw_fcasts,
                glue("{data_path}/t{fhorizon}-RW-fcasts.parquet"))
}




# Function definitions ---------------------------------------------------------

# Main forecasting function used for looping by time and "partition". Returns
# a list containing a forecast data.frame and a regression results data.frame.
ols_fcst_for_window <- function(
    .date_idx,
    .partition_idx,
    .dta,
    .model_name,
    .model_equation,
    .fcst_horizon,
    .roll_win_size,
    .estimation,
    .min_losses = 10
) {
  train_window <-
    .dta |>
    filter(partition == .partition_idx) |>
    # set forecast horizon
    filter(
      datadate <= (.date_idx  %m-% months(.fcst_horizon * 12))
      & (datadate >= 
           (.date_idx %m-% months((.fcst_horizon * 12) + 12 * .roll_win_size)) 
         )
    ) |>
    mutate(
      FEARN = .data[[paste0("EARN_lead_", .fcst_horizon)]],
      Fdatadate = .data[[paste0("datadate_lead_", .fcst_horizon)]]
      # Fcalyear = .data[[paste0("calyear_lead_", .fcst_horizon)]]
    ) |>
    # additional filter for non-missing forward EARN
    filter(
      !is.na(FEARN),
      abs((EARN * Scalar)/MVE) < 1,
      abs(EBSI_lead_1/MVE) < 1,
      month(Fdatadate) == month(datadate),
      year(Fdatadate) == year(datadate) + .fcst_horizon 
    ) |>
    select(-Fdatadate) |> 
    rename(deflator = Scalar) |>
    mutate(
       EARN  = truncate_x(EARN, cut = 0.01),
      FEARN = truncate_x(FEARN, cut = 0.01),
    ) |>
    mutate(mean_loss = mean(LOSS), 
           n         = n()) |>
    # filter here on mean_loss as if () {} statement only takes one condition
    filter((mean_loss * n) >= .min_losses)
  
  test_window  <-
    .dta |>
    filter(partition == .partition_idx) |>
    filter(datadate == .date_idx) |>
    rename(deflator = Scalar)
  
  if (.model_name == "HVZ") {
    train_window <-
      train_window |>
      mutate(
        TA = truncate_x(TA, cuts = c(0.01,0.01)),
        DIV = truncate_x(DIV, cuts = c(NA,0.01)),
        ACC = truncate_x(ACC, cuts = c(0.01,0.01))
      )
    
    test_window <-
      test_window |>
      filter(!is.na(ACC))
  }
  
  # only run if there are enough obs
  if (nrow(train_window) >= 100) {
    
    if (.estimation == "MEDIAN") {
      reg_model   <- function(x) quantreg::rq(as.formula(x), 
                                              tau = .5, data = train_window)
    } else if (.estimation == "OLS")  {
      reg_model   <- function(x) lm(as.formula(x), data = train_window)
    } else {
      stop("Wrong estimation method. Must be 'OLS' or 'MEDIAN'")
    }
    
    model_fit <- reg_model(.model_equation)
    scaled_fcast  <- predict(model_fit, newdata = test_window)
    predictions <-
      cbind(test_window, scaled_fcast) |>
      mutate(
        fcast = scaled_fcast * deflator,
        FEBSI = .data[[glue("EBSI_lead_{.fcst_horizon}")]],
        Model = .model_name,
        Partition = .partition_idx
      ) |>
      select(Model, gvkey, fcast, Partition, datadate, FEBSI, MVE)
    
    reg_coeffs <- coef(model_fit)
    if (.estimation == "RQ") {
      bench_model <- quantreg::rq("FEARN ~ 1", tau = .5, data = train_window)
      pseudo_r2 = 1- (as.numeric(model_fit$rho)/as.numeric(bench_model$rho))
    } else {
      pseudo_r2 = as.numeric(summary(model_fit)$adj.r.squared)
    }
    reg_betas_a_stats  <-
      tibble(
        term = as.character(names(reg_coeffs)),
        estimate = as.numeric(reg_coeffs)
      ) |>
      add_row(
        term = c("stat_nobs", "stat_r2"),
        estimate = c(
          as.numeric(length(model_fit$residuals)),
          pseudo_r2
        )
      ) |>
      mutate(
        Model = .model_name,
        Partition = .partition_idx,
        fcst_date = .date_idx
      )
    
    return(list(predictions, reg_betas_a_stats))
  } else {
    return(NULL)
  }
}


ols_trainmodel_and_predict <- function(
    datelist,
    model,
    dta,
    fcst_horizon,
    roll_win_size,
    estimation,
    Deflator,
    min_n_losses) {
  
  # year_range <- min_year:(max_year - fcst_horizon)
  model_name <- names(model[1])
  model_equation <- model[[1]]
  
  # some ols models are fit by industry, etc. Define a dataset partition
  # for those cases
  if (model_name == "EP-LCYCLE") {
    dta_w_partition <- rename(dta, partition = lifecycle)
    dta_w_partition <- filter(dta_w_partition, is.na(partition) == FALSE)
    dta_w_partition$partition <- as.character(dta_w_partition$partition)
  } else if (model_name == "EP-GIC") {
    dta_w_partition <- rename(dta, partition = best_gic)
    dta_w_partition <- filter(dta_w_partition, is.na(partition) == FALSE)
  } else {
    dta_w_partition <- mutate(dta, partition = "1")
  }
  
  dta_w_partition <- filter(dta_w_partition, is.na(partition) == FALSE)
  partition_idxs <- unique(na.omit(dta_w_partition$partition))
  
  data_ids <- expand_grid(date_idx = datelist, partition = partition_idxs)
  
  reg_results <- furrr::future_map2(
    .f = ols_fcst_for_window,
    .x = data_ids$date_idx,
    .y = data_ids$partition,
    # Additional arguments:
    .dta = dta_w_partition,
    .model_name = model_name,
    .model_equation = model_equation,
    .fcst_horizon = fcst_horizon,
    .roll_win_size = roll_win_size,
    .estimation = estimation,
    .min_losses = min_n_losses
  )
  
  model_fcasts <- do.call("rbind", lapply(reg_results, function(x) x[[1]]))
  model_coeffs <- do.call("rbind", lapply(reg_results, function(x) x[[2]]))
  
  write_parquet(
    model_fcasts,
    glue("{data_path}/t{fcst_horizon}-{estimation}-{model_name}-fcasts.parquet")
  )
  write_csv(
    model_coeffs,
    glue("out/t{fcst_horizon}-{estimation}-{model_name}-coeffs.csv")
  )
  
  return(NULL)
}


# Run --------------------------------------------------------------------------

tic()
plan(multisession(workers = min(100, (length(availableWorkers()) - 1))))
for (model_meta in ols_models) {
  for (fhorizon in fcast_horizons) {
    for (est in estimation_type){
      
      ols_trainmodel_and_predict(
        datelist = testing_datelist,
        model = model_meta,
        estimation = est,
        fcst_horizon = fhorizon,
        dta = data_ols,
        roll_win_size = roll_win_size,
        Deflator = deflator,
        min_n_losses = min_n_losses
      )
      
    }
  }
}
plan(sequential) #release the workers
toc()



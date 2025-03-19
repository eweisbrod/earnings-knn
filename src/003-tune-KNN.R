#' In this script, we tune K and N for the PB1 Model


# Setup ------------------------------------------------------------------------

#load libraries
library(glue)
library(tidyverse)
library(tictoc)
library(future.apply)
library(FNN)
library(lubridate)

#load helper scripts
source("src/-Global-Parameters.R")
source("src/utils.R")
source("src/ZZ-matching-functions.R")


#only tune one-year-ahead
fcst_horizon <- 1


# create a grid of all combinations to tune over
mygrid <- data.frame(expand.grid(myDateIdx = 1:length(tuning_datelist), 
                                 myK = seq(10, 500, 10),
                                 myM = 1:5))



# Define tuning wrapper functions ----------------------------------------------

# Define Tune MK Function to tune m and k for each model
tune_mk_function <- function(.grid,
                             .p,
                             .model,
                             .var_idx,
                             .fcstHzn = .Fcst_horizon,
                             datelist=datelist,
                             .tune_dta=Tuning_data) {
  #update the progress bar
  .p(sprintf("DateIdx=%s,M=%s,K=%s,Worker=%s", 
             .grid["myDateIdx"], .grid["myM"], .grid["myK"], 
             Sys.getpid()))
  
  #generate the forecasts
  gen_knn_fcasts(
    model = .model,
    var_idx = .var_idx,
    datelist = datelist,
    dta = .tune_dta,
    .dateidx = as.numeric(.grid["myDateIdx"]),
    .m = as.numeric(.grid["myM"]),
    .k = as.numeric(.grid["myK"]),
    .fcst_hzn = .fcstHzn
  )
}


#Define model-level function to apply the Tune MK function for each model
model_tune_function <- function(.Model,
                                datelist,
                                .Data = data_M5,
                                .Grid = mygrid,
                                .Deflator = deflator,
                                .Fcst_horizon = fcst_horizon,
                                .var_index = model_vars.M5) {
  
  
  #First, filter the data to the model being tuned
  Tuning_data <- .Data %>% 
    filter_at(.vars = unlist(.var_index[[.Model]][5]),all_vars(!is.na(.))) %>%
    mutate(
      FEBSI = .data[[paste0("EBSI_lead_",.Fcst_horizon)]],
      FEARN = .data[[paste0("EARN_lead_",.Fcst_horizon)]],
      Fdatadate = .data[[paste0("datadate_lead_",.Fcst_horizon)]]
    ) %>% 
    select(gvkey,datadate,Scalar,MVE, FEBSI, FEARN,Fdatadate,unlist(.var_index[[.Model]][5]))
  
  #for a given model, run the parallel tuning with the progress bar
  progressr::with_progress({
    #define the progress bar
    p <- progressr::progressor(steps = length(rownames(.Grid)), along = .Grid)
    #apply the tune_mk_function and bind the results
    results <- bind_rows(
      future_apply(
        .Grid, 
        1,
        FUN = tune_mk_function,
        .p=p,
        .model=.Model,
        .var_idx=.var_index,
        datelist=datelist,
        .tune_dta=Tuning_data,
        .fcstHzn = .Fcst_horizon
      )
    )
  }) 
  
  #Save the results to a file and clean up     
  write_parquet(results,glue("{data_path}//{.Model}-tuning-fcasts.parquet"))
  rm(Tuning_data, results)
  
}

#Load Data ---------------------------------------------------------------------

#Always use M5 for tuning
data_M5 <- read_parquet(glue("{data_path}/data_M5.parquet"))

#reserve parallel resources for tuning
# If there are more than 100 cores, set up 100 workers.
# If there are less than 100 cores, use workers = total cores - 1.
plan(multisession(workers = min(100, (length(availableWorkers()) - 1))))
progressr::handlers("progress")

#apply the model tune function to the list of models defined in global params
tic()
lapply(models, model_tune_function,datelist = tuning_datelist)
toc()


plan(sequential) # release the workers





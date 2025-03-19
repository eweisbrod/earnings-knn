#' This script computes peer based model forecasts for a given N = h sample, a given
#' scalar (currently MVE) and forecast horizons (t+1, t+2, t+3). The N (number of years to
#' consider in the PB model) is set via past_N.

# Setup ---------------------------------------------------------------------------------

#load libraries
library(glue)
library(tidyverse)
library(tictoc)
library(future.apply)
library(future.callr)
library(progressr)
library(FNN)
library(lubridate)

#load helper scripts
source("src/-Global-Parameters.R")
source("src/utils.R")
source("src/ZZ-matching-functions.R")

#forcast horizons - applies to all models being generated
fcast_horizons <- c(1,2,3)

# create a grid of all combinations to tune over
mygrid <- data.frame(expand.grid(myDateIdx = 1:length(testing_datelist), 
                                 myFcstHzn = fcast_horizons))







# Define functions -------------------------------------------------------------

# Define function to generate forecasts for a given set of parameters
knn_fcast_function <- function(.grid,
                               .p,
                               .model,
                               .myM,
                               .myK,
                               .var_idx,
                               .helper_vars,
                               datelist=datelist,
                               .fcast_dta) {
  
  #update the progress bar
  .p(sprintf("Model=%s,DateIdx=%s,FcstHzn=%s,Worker=%s", 
             .model, .grid["myDateIdx"], .grid["myFcstHzn"],
             Sys.getpid()))
  
  #set the forecast horizon
  .fcstHzn <- as.numeric(.grid["myFcstHzn"])
  
  #Prepare the data to match the forecast horizon
  .fcast_dta <- .fcast_dta %>%
    mutate(
      FEBSI = .data[[paste0("EBSI_lead_",.fcstHzn)]],
      FEARN = .data[[paste0("EARN_lead_",.fcstHzn)]],
      Fdatadate = .data[[paste0("datadate_lead_",.fcstHzn)]]
    ) %>% 
    select(Scalar,all_of(.helper_vars), FEBSI, FEARN,Fdatadate,unlist(.var_idx[[.model]][.myM]))
  
  #generate the forecasts
  fcasts <- gen_knn_fcasts(
    model = .model,
    var_idx = .var_idx,
    datelist = datelist,
    dta = .fcast_dta,
    .dateidx = as.numeric(.grid["myDateIdx"]),
    .m = .myM,
    .k = .myK,
    .fcst_hzn = .fcstHzn,
    .descriptives = TRUE
  ) 
  
  fcasts[["FcstHzn"]] = .fcstHzn
  
  #return the forecasts
  return(fcasts)
}


#Define model-level function to apply the Tune MK function for each model
model_fcast_function <- function(.Model,
                                 .Grid = mygrid,
                                 .Deflator = deflator,
                                 .var_index = model_vars.M5,
                                 .Helper_vars = helper_vars,
                                 datelist = testing_datelist) {
  #Print a Status Message
  message(
    "begin generating for Model = ", .Model 
  )
  
  #select the model with the lowest training error
  kstar <- read_parquet(glue("{data_path}/{.Model}-kstar-data.parquet")) %>% 
    filter(kstar==TRUE) %>% 
    arrange(MAFE) %>% 
    slice(1)
  
  #use the M and K from that model
  myM <- as.integer(kstar$M)
  myK <- as.integer(kstar$K)
  
  
  #First, read data and filter to the model being forecast
  forecast_data <- read_parquet(glue("{data_path}/data_M{myM}.parquet"))  %>% 
    filter_at(.vars = unlist(.var_index[[.Model]][myM]),all_vars(!is.na(.))) 
  
  #reserve parallel resources for tuning
  # If there are more than 100 cores, set up 100 workers.
  # If there are less than 100 cores, use workers = total cores - 1.
  plan(callr(workers = min(100, (length(availableWorkers()) - 1))))
  progressr::handlers("progress")
  
  #for a given model, run in parallel with the progress bar
  progressr::with_progress({
    #define the progress bar
    p <- progressr::progressor(steps = length(rownames(.Grid)), along = .Grid)
    #apply the forecast function and bind the results
    results <- future_apply(
      .Grid, 
      1,
      FUN = knn_fcast_function,
      .p=p,
      .model=.Model,
      .myM = myM,
      .myK = myK,
      .var_idx=.var_index,
      .helper_vars = .Helper_vars,
      datelist=datelist,
      .fcast_dta=forecast_data
    )
    
  }) 
  
  #Save the results to separate files and clean up
  
  
  lapply(
    unlist(unique(.Grid$myFcstHzn)),
    function(x){
      write_parquet(bind_rows(map(results,1)) %>%
                      filter(FcstHzn==x),
                    glue("{data_path}/t{x}-{.Model}-fcasts.parquet"))
      
      write_parquet(bind_rows(map(results,2)) %>%
                      filter(FcstHzn==x),
                    glue("{data_path}/t{x}-{.Model}-peer-data.parquet"))
      
    }
  )
  
  
  rm(forecast_data, results)
  plan(sequential) #release the workers
  
  #Print a Status Message
  message(
    "end generating for Model = ", .Model 
  )
  
}

#Load Data ---------------------------------------------------------------------



#uncomment this to forecast just one model
#models <- c("KNN")

#apply the forecast function to the list of models
tic()
lapply(models,model_fcast_function)
toc()




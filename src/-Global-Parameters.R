##Define Global Parameters

#run this to set your dropbox path in your .Renviron
usethis::edit_r_environ('project')
#example for .Renviron below
#DATA_PATH = "D:/Dropbox/EKMSW_data"

#load the data path from the project environment
#each person can point to the dropbox folder on their computer
data_path <- Sys.getenv('DATA_PATH')

#Download Dates for WRDS Data
crsp_begin_date <- "1950-01-01"
crsp_end_date   <- "2020-12-31"

#Set Deflator for scaling
deflator <- "MVE"

#Set rolling window size
roll_win_size <- 10L

#Set minimum MVE for testing
min_mve <- 10L


# Define the tuning dates ------------------------------------------------------

#dates corresponding to the minimum and maximum datadates of the subjects
min_tune_date <- lubridate::date("1979-01-01")
max_tune_date <- lubridate::date("1994-12-01")

#list of forecast dates (datadates)
tuning_datelist <- (seq(min_tune_date,max_tune_date, by="month")+months(1)-days(1))

# Define the testing dates -----------------------------------------------------

#dates corresponding to the minimum and maximum datadates of the subjects
min_test_date <- lubridate::date("1998-01-01")
max_test_date <- lubridate::date("2019-12-01")

#list of forecast dates (datadates)
testing_datelist <- (seq(min_test_date,max_test_date, by="month")+months(1)-days(1))


#Define the KNN model specifications -------------------------------------------

#List of models to tune
models <- c("KNN",
            "KNNDUP1",
            "KNNDUP2",
            "KNNDUP3",
            "KNNDUP4",
            "KNNHVZ1",
            "KNNHVZ2",
            "KNNHVZ3",
            "KNNHVZ4")

model_vars <- list(
  KNN = c("EARN"),
  KNNDUP1 = c("PM", "ATO"),
  KNNDUP2 = c("PM", "ATO", "LEV"),
  KNNDUP3 = c("PM", "ATO", "SG"),
  KNNDUP4 = c("PM", "ATO", "LEV", "SG"),
  KNNHVZ1 = c("EARN", "ACC_HVZ"),
  KNNHVZ2 = c("EARN", "ACC_HVZ", "AT_HVZ"),
  KNNHVZ3 = c("EARN", "ACC_HVZ", "AT_HVZ", "D_HVZ"),
  KNNHVZ4 = c("EARN", "ACC_HVZ", "AT_HVZ", "D_HVZ", "LOSS")
)

#top level list of all the unique model variables
unique_model_vars <- unique(unlist(model_vars, use.names = FALSE))


ols_models <- list(  # regression-based models setup
  list("HVZ" = "FEARN ~ EARN + ACC + TA + DIV + DD + LOSS"),
  list("EP-ALL" = "FEARN ~ LOSS*EARN"),
  list("EP-LCYCLE" = "FEARN ~ LOSS*EARN"),
  list("EP-GIC" = "FEARN ~ LOSS*EARN")
)


# create an index of all the variables for each model / M combo ----------------

#bottom-level helper function
lagfunction1 <- function(x,lags){
  append(x,paste0(x,"_lag_",lags))
}

#second-level helper function
lagfunction2 <- function(y,z){
  if(y==1){
    return(z)
  }
  if(y>1){
    unlist(lapply(z, lagfunction1,1:(y-1)))
  }
}

#combine helper functions to get the
model_vars.M5 <- lapply(model_vars,function(w) lapply(1:5,lagfunction2,w))

# define additional vars to save to M-level datasets ---------------------------

#this might be a waste of time, but giving it a try.
helper_vars <- c('gvkey','datadate',
                 'datadate_lead_1','datadate_lead_2','datadate_lead_3',
                 'EBSI_lead_1','EBSI_lead_2','EBSI_lead_3',
                 'EARN_lag_1',
                 'EARN','EARN_lead_1','EARN_lead_2','EARN_lead_3',
                 'MVE','at','sic2','FF12', 
                 'lifecycle','acc', 'age', 'best_gic', 'IBES_covered')

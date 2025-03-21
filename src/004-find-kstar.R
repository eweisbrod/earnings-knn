#load libraries
library(glue)
library(lubridate)
library(broom)
library(fixest)
library(tidyverse)


#load helper scripts
source("src/-Global-Parameters.R")
source("src/utils.R")
source("src/ZZ-matching-functions.R")



# Define Function to run error difference regressions --------------------------

regression_row <- function(k.,
                           model.=.model,
                           m.=.m,
                           tuning_fcasts.=.tuning_fcasts){
  
  #subset the data
  reg_data <-
    tuning_fcasts.  %>% 
    filter(M == m.,
           K == (k. - 10) | K == k.) %>%
    mutate(K = factor(K),
           K = forcats::fct_relevel(K, 
                                    paste("", k., sep = "",
                                          colapse = ""))
    )
  
  #run the regression
  mafe_reg <- tidy(feols(fcast_error ~ K ,
                         vcov = ~ gvkey + year,
                         data = reg_data), se.type = "cluster")
  
  #format the results
  row <- tibble(Model = model.,
                M = m.,
                K = k.,
                MAFE = as.numeric(mafe_reg[1,2]),
                error_diff = -as.numeric(mafe_reg[2,2]),
                pval = as.numeric(mafe_reg[2,5])
  )
  
  #return the results
  return(row)
  
} 

#helper function to find first insignificant step
first_fail = function(diff,pval, plevel=.05,tolerance=1) {
  ((pval > plevel) | (diff >= 0)) & (
    (cumsum(((pval > plevel) | (diff >= 0))) <= tolerance)
  )
}


#function to tune k for a given m ----------------------------------------------

tune_kstar_data <- function(.model = .Model,
                            .m,
                            .klist = klist,
                            .tolerance=1,
                            .tuning_fcasts = tuning_fcasts) {
  
  #output a status message to track progress
  message(
    "finding kstar for M= ", .m 
  )
  
  #apply the regression row function to each row in the klist
  myrows <-  bind_rows(lapply(.klist,regression_row,
                              model.=.model,
                              m.=.m,
                              tuning_fcasts. = .tuning_fcasts))
  
  #bind the regression rows with the initial data for k=10
  k_data <- bind_rows(.tuning_fcasts %>% 
                        filter(M==.m, K==10) %>%
                        group_by(Model,M,K) %>% 
                        summarize(MAFE = mean(fcast_error), .groups="drop") %>% 
                        mutate(error_diff = -.0000000001,
                               pval = 0),
                      myrows) %>% 
    #use the first_fail function to find kstar
    mutate(fail = first_fail(diff=error_diff,pval = pval, tolerance=.tolerance)) %>% 
    group_by(M) %>% 
    arrange(K) %>% 
    mutate(
      kstar = (MAFE==min(MAFE)),
      #tiebreaker if two MAFEs exactly the same, pick lower
      kstar = kstar & cumsum(kstar)==1
    ) %>% 
    ungroup() 
  
  
  #return the kstar dataframe
  return(k_data)
  
  
}

#function to tune all M for a given model --------------------------------------

find_kstars <- function(.Model, .Deflator = deflator, .Tolerance = 1) { 
  
  
  #output a status message to track progress
  message(
    "Model = ", .Model 
  )
  
  #first load the appropriate dataset
  tuning_fcasts <- read_parquet(glue("{data_path}/{.Model}-tuning-fcasts.parquet")) %>%
    #Apply the standard filters (consider parameterizing this maybe?)
    filter(
      is.na(FEBSI) == FALSE,
      #abs(EARN) < 1,
      MVE > 10
    ) %>% 
    #calculate the forecast errors and year (for error clustering)
    mutate(fcast_error = abs(FEBSI - fcast) / MVE,
           year = year(datadate))
  
  #list of m values to tune
  mlist <- unlist(unique(tuning_fcasts$M))
  
  #list of k values to tune
  klist <- seq(20,max(tuning_fcasts$K),10)
  
  
  
  #tune kstar for each m
  mk_data <- bind_rows(
    lapply(mlist,tune_kstar_data,
           .model = .Model,
           .klist = klist,
           .tolerance=.Tolerance,
           .tuning_fcasts = tuning_fcasts)
  )
  
  #Save the results to a file and clean up     
  write_parquet(mk_data,glue("{data_path}/{.Model}-kstar-data.parquet"))
  rm(tuning_fcasts,mlist,klist,mk_data)
  
}

# Apply functions to the model list --------------------------------------------


#apply the functions to the list of models
lapply(models,find_kstars,.Tolerance=3)



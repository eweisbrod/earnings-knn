#' This script creates data0 which is used many times. 
#' It also generates an indicator for analyst coverage.
#' These are needed at the beginning for Table 1B.


# Setup ------------------------------------------------------------------------
library(glue)
library(tidyverse)
library(lubridate)
library(tictoc)


source("src/-Global-Parameters.R")
source("src/utils.R")




# Load the raw WRDS Data
raw_funda <- read_parquet(glue("{data_path}/raw_funda.parquet"))

# create an linking table for a the "has ibes following" dummy
ibes_fcst_exist <-
  read_csv("sample-id/ibes_fcst_exist.csv")

# Create data0 ------------------------------------------------------------------

# make a table to filter duplicates
nodups <- raw_funda %>% 
  group_by(gvkey,gics_year) %>% 
  count() %>% 
  filter(n==1) %>% 
  select(-n)

# Common computations
data0 <-
  raw_funda %>%
  #Filter calendar years with multiple datadates
  inner_join(nodups, by = c("gvkey", "gics_year")) %>% 
  #Rename raw variables to match the paper
  rename(
    EBSI = e,
    B    = ceq,
    MVE  = mve,
    D    = dvc
  ) %>%
  #Sort and group by GVKEY to compute lags
  arrange(gvkey, gics_year) %>%
  group_by(gvkey) %>%
  #Compute lags
  mutate_at(
    vars(act, che, lct, dlc, txp,
         at, B, sale, D, che, 
         lct, dlc, txp, ib, spi, 
         EBSI, MVE, calyear,datadate),
    list(lag_1 = ~ dplyr::lag(., 1))
  ) %>%
  #HVZ variables
  mutate(
    LOSS     = if_else(EBSI < 0,1,0),
    acc_HVZ  = if_else(month(datadate_lag_1) == month(datadate) &
                         (year(datadate_lag_1) == year(datadate) - 1),
                       ((act - che) - (act_lag_1 - che_lag_1)) -
                         ((lct - dlc - txp) - (lct_lag_1 - dlc_lag_1 - txp_lag_1)) - dp,
                       NaN),
    acc = EBSI - oancf,
    DD = D > 0) %>% 
  #DuPont Variables
  mutate(
    SG  = if_else(!is.na(sale_lag_1) &  
                    sale_lag_1 > 0 &
                    month(datadate_lag_1) == month(datadate) &
                    (year(datadate_lag_1) == year(datadate) - 1),
                  (sale / sale_lag_1) - 1,
                  NaN),
    PM  =  if_else(sale>0,EBSI / sale,NaN),
    ATO = if_else(at > 0, sale / at, NaN),
    LEV = if_else(B > 0, at / B, NaN)
  ) %>%
  #Descriptive / other variables
  mutate(
    lifecycle = if_else(!is.na(oancf) & !is.na(ivncf) & !is.na(fincf),
                        case_when(
                          oancf < 0 & ivncf < 0 & fincf > 0 ~ 1,
                          oancf > 0 & ivncf < 0 & fincf > 0 ~ 2,
                          oancf > 0 & ivncf < 0 & fincf < 0 ~ 3,
                          oancf < 0 & ivncf > 0  ~ 5,
                          TRUE ~ 4
                        ), NaN),
    age = row_number(), #not a great way to do age but leave for now.
    sic2 = substr(sic, 1, 2),
    sic2 = as.numeric(sic2),
    FF12 = assign_FF12(as.integer(sic)),
    one  = 1
  )  %>% 
  #leads
  mutate(
    EBSI_lead_1     = dplyr::lead(EBSI,1L),
    EBSI_lead_2     = dplyr::lead(EBSI,2L),
    EBSI_lead_3     = dplyr::lead(EBSI,3L),
    calyear_lead_1  = dplyr::lead(calyear, 1L),
    calyear_lead_2  = dplyr::lead(calyear, 2L),
    calyear_lead_3  = dplyr::lead(calyear, 3L),
    datadate_lead_1  = dplyr::lead(datadate, 1L),
    datadate_lead_2  = dplyr::lead(datadate, 2L),
    datadate_lead_3  = dplyr::lead(datadate, 3L),
  ) %>%
  ungroup() %>% 
  left_join(ibes_fcst_exist, by = c("gvkey", "datadate"))

write_parquet(data0,glue("{data_path}/data0.parquet"))

#Data for M1 Models
data_M1 <- data0 %>%
  #Set deflator dynamically
  mutate(Scalar = .data[[deflator]],) %>% 
  # RW Requirements
  filter(
    !is.na(MVE) & MVE > 0, #Always need MVE to scale MAFE I think?
    !is.na(Scalar) & Scalar > 0,
    !(sic4 %in% (4900:4999)),
    !(sic4 %in% (6000:6999)),
    !is.na(EBSI)
  ) %>%
  #Scaled variables
  mutate(
    EARN = EBSI / Scalar,
    EARN_lag_1 = EBSI_lag_1 / Scalar,
    EARN_lead_1     = EBSI_lead_1 / Scalar,
    EARN_lead_2     = EBSI_lead_2 / Scalar,
    EARN_lead_3     = EBSI_lead_3 / Scalar,
    ACC_HVZ    = acc_HVZ / Scalar,
    AT_HVZ     = at / Scalar, 
    D_HVZ = D / Scalar,
  ) %>% 
  #define required variables in the global settings (maybe?)
  select(Scalar,all_of(helper_vars), all_of(unique_model_vars))


write_parquet(data_M1,glue("{data_path}/data_M1.parquet"))


#pb1n2
data_M2 <- data_M1 %>%
  inner_join(data0 %>%
               arrange(gvkey, gics_year) %>%
               group_by(gvkey) %>%
               mutate_at(
                 vars(EBSI, PM, ATO, LEV, SG, acc_HVZ, at, D, LOSS),
                 list(lag_1 = ~ dplyr::lag(., 1L))
               ) %>%
               ungroup() %>%
               select(gvkey, datadate, ends_with("lag_1")), 
             by = c("gvkey", "datadate")
  ) %>% 
  filter(month(datadate_lag_1) == month(datadate),
         year(datadate_lag_1) == year(datadate) - 1) %>%
  filter(!is.na(EBSI_lag_1)) %>% 
  # Scaling
  mutate(
    ACC_HVZ_lag_1    = acc_HVZ_lag_1 / Scalar,
    AT_HVZ_lag_1     = at_lag_1 / Scalar, 
    D_HVZ_lag_1 = D_lag_1 / Scalar,
  ) %>% 
  select(-EBSI_lag_1,-acc_HVZ_lag_1,-at_lag_1,-D_lag_1)

write_parquet(data_M2,glue("{data_path}/data_M2.parquet"))


#pb1n3
data_M3 <- data_M2 %>%
  inner_join(data0 %>%
               arrange(gvkey, gics_year) %>%
               group_by(gvkey) %>%
               mutate_at(
                 vars(datadate,EBSI, PM, ATO, LEV, SG, acc_HVZ, at, D, LOSS),
                 list(lag_2 = ~ dplyr::lag(., 2L))
               ) %>%
               ungroup() %>%
               select(gvkey, datadate, ends_with("lag_2")),
             by = c("gvkey", "datadate")
  ) %>%
  filter(month(datadate_lag_2) == month(datadate),
         year(datadate_lag_2) == year(datadate) - 2) %>%
  # Scaling
  mutate(
    EARN_lag_2 = EBSI_lag_2 / Scalar,
    ACC_HVZ_lag_2    = acc_HVZ_lag_2 / Scalar,
    AT_HVZ_lag_2     = at_lag_2 / Scalar, 
    D_HVZ_lag_2 = D_lag_2 / Scalar,
  ) %>% 
  select(-EBSI_lag_2,-acc_HVZ_lag_2,-at_lag_2,-D_lag_2)

write_parquet(data_M3,glue("{data_path}/data_M3.parquet"))


#pb1n4
data_M4 <- data_M3 %>%
  inner_join(data0 %>%
               arrange(gvkey, gics_year) %>%
               group_by(gvkey) %>%
               mutate_at(
                 vars(datadate,EBSI, PM, ATO, LEV, SG, acc_HVZ, at, D, LOSS),
                 list(lag_3 = ~ lag(., 3L))
               ) %>%
               ungroup() %>%
               select(gvkey, datadate, ends_with("lag_3")),
             by = c("gvkey", "datadate")
  ) %>%
  filter(month(datadate_lag_3) == month(datadate),
         year(datadate_lag_3) == year(datadate) - 3) %>%
  filter(!is.na(EBSI_lag_3)) %>%
  # Scaling
  mutate(
    EARN_lag_3 = EBSI_lag_3 / Scalar,
    ACC_HVZ_lag_3    = acc_HVZ_lag_3 / Scalar,
    AT_HVZ_lag_3     = at_lag_3 / Scalar, 
    D_HVZ_lag_3 = D_lag_3 / Scalar,
  ) %>% 
  select(-EBSI_lag_3,-acc_HVZ_lag_3,-at_lag_3,-D_lag_3)

write_parquet(data_M4,glue("{data_path}/data_M4.parquet"))



#pb1n5
data_M5 <- 
  data_M4 %>%
  inner_join(data0 %>%
               arrange(gvkey, datadate) %>%
               group_by(gvkey) %>%
               mutate_at(
                 vars(datadate,EBSI, PM, ATO, LEV, SG, acc_HVZ, at, D, LOSS),
                 list(lag_4 = ~ lag(., 4L))
               ) %>%
               ungroup() %>%
               select(gvkey, datadate, ends_with("lag_4")),
             by = c("gvkey", "datadate")
  ) %>%
  filter(month(datadate_lag_4) == month(datadate),
         year(datadate_lag_4) == year(datadate) - 4) %>% 
  filter(!is.na(EBSI_lag_4)) %>%
  # Scaling
  mutate(
    EARN_lag_4 = EBSI_lag_4 / Scalar,
    ACC_HVZ_lag_4    = acc_HVZ_lag_4 / Scalar,
    AT_HVZ_lag_4     = at_lag_4 / Scalar, 
    D_HVZ_lag_4 = D_lag_4 / Scalar,
  ) %>% 
  select(-EBSI_lag_4,-acc_HVZ_lag_4,-at_lag_4,-D_lag_4)

write_parquet(data_M5,glue("{data_path}/data_M5.parquet"))






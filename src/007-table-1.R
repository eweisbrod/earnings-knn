
# Setup ------------------------------------------------------------------------

#load libraries
library(glue)
library(lubridate)
library(tidyverse)

#load helper scripts
source("src/-Global-Parameters.R")
source("src/utils.R")
source("src/ZZ-matching-functions.R")

#
fcst_horizon <- 1


# Base data --------------------------------------------------------------------

raw_funda <- read_parquet(glue("{data_path}/raw_funda.parquet"))



# Sample Selection -------------------------------------------------------------

#Staring point (step 0): load data0 and filter to testing window dates
data_0 <- read_parquet(glue("{data_path}/data0.parquet")) |>
  filter(
    datadate >= testing_datelist[[1]],
    datadate <= testing_datelist[[(length(testing_datelist) - 12*fcst_horizon)]]
  )

#start Table 1 with this number of obs -----------------------------------------
nrows_table <- bind_cols(
  Sample = "Total Compustat Observations 1998 - 2018",
  data_0 |> count(name = "Sample_Selection")
)


# Step 1: non-missing EBSI -----------------------------------------------------
data_1 <-
  data_0 |>
  filter(!is.na(EBSI)) 


ndata_01 <- data_1 |> nrow() - data_0 |> nrow()

nrows_table <- nrows_table |> 
  add_row(
    Sample = "Less missing EBSI",
    Sample_Selection = ndata_01
  )

# Step 2: non-missing MVE ------------------------------------------------------
data_2 <-
  data_1 |>
  filter(!is.na(MVE) & MVE > 0)


ndata_0201 <- data_2 |> nrow() - data_1 |> nrow()

nrows_table <- nrows_table |> 
  add_row(
    Sample = "Less missing and non-positive MVE",
    Sample_Selection = ndata_0201
  )

#Step 3: Minimum MVE -----------------------------------------------------------

data_3 <- data_2 |> 
  filter(!is.na(MVE) & (MVE > min_mve))

ndata_0302 <- data_3 |> nrow() - data_2 |> nrow()


nrows_table <- nrows_table |> 
  add_row(
    Sample = "Less MVE < $10M",
    Sample_Selection = ndata_0302
  )

#Step 4: Absolute EARN < 1  ----------------------------------------------------

data_4 <- data_3 |> 
  mutate(EARN = EBSI/MVE) |> 
  filter(abs(EARN) < 1)

ndata_0403 <- data_4 |> nrow() - data_3 |> nrow()


nrows_table <- nrows_table |> 
  add_row(
    Sample = "Less absolute scaled earnings greater than one",
    Sample_Selection = ndata_0403
  )

#Step 5: Future EBSI -----------------------------------------------------------

data_5 <- data_4 |> 
  filter(!is.na(EBSI_lead_1),  
         month(datadate_lead_1) == month(datadate),
         year(datadate_lead_1) == year(datadate) + 1)

ndata_0504 <- data_5 |> nrow() - data_4 |> nrow()


nrows_table <- nrows_table |> 
  add_row(
    Sample = "Less missing future EBSI",
    Sample_Selection = ndata_0504
  )


# Step 6: exclude regulated industries -----------------------------------------
data_6 <-
  data_5 |>  
  filter(
    !(sic4 %in% (4900:4999)),
    !(sic4 %in% (6000:6999))
  ) 

ndata_0605 <- data_6 |> nrow() - data_5 |> nrow()

nrows_table <- nrows_table |> 
  add_row(
    Sample = "Less financial firms and regulated firms",
    Sample_Selection = ndata_0605
  )


#sum steps 1-6
nrows_table <- nrows_table |> 
  bind_rows(bind_cols(
    Sample = "Random walk forecast sample",
    data_6 |> count(name = "Sample_Selection")
  ))


#Step 7: lagged EBSI -----------------------------------------------------------

data_7 <-
  data_6 |>
  filter(month(datadate_lag_1) == month(datadate),
         year(datadate_lag_1) == year(datadate) - 1) |>
  filter(!is.na(EBSI_lag_1),
         !is.na(.data[[deflator]]),
         .data[[deflator]] > 0) 

ndata_0706 <- data_7 |> nrow() - data_6 |> nrow()

nrows_table <- nrows_table |> 
  add_row(
    Sample = "Less missing lagged EBSI",
    Sample_Selection = ndata_0706
  )

#sum steps 6 - 7
nrows_table <- nrows_table |> 
  bind_rows(bind_cols(
    Sample = "Random Walk Comparison Sample (KNN (M=2) forecast sample)",
    data_7 |> count(name = "Sample_Selection")
  ))



#Step 8: BCG ------------------------------------------------------------------

#add a spacer
nrows_table <- nrows_table |> 
  add_row(
    Sample = "",
    Sample_Selection = NaN
  )

#back to RW
nrows_table <- nrows_table |> 
  bind_rows(bind_cols(
    Sample = "Random Walk Comparison Sample",
    data_7 |> count(name = "Sample_Selection")
  ))


bcg_fcasts <- read_parquet(glue("{data_path}/t{fcst_horizon}-BCG-fcasts.parquet"))


data_11 <-
  data_7 |>
  inner_join(bcg_fcasts, by = c("gvkey","datadate"))

ndata_1107 <- data_11 |> nrow() - data_7 |> nrow()

nrows_table <- nrows_table |> 
  add_row(
    Sample = "Less missing BCG Data",
    Sample_Selection = ndata_1107
  )

nrows_table <- nrows_table |> 
  bind_rows(bind_cols(
    Sample = "BCG Comparison Sample",
    data_11 |> count(name = "Sample_Selection")
  )
  )



#Step 9: HVZ -------------------------------------------------------------------

#add a spacer
nrows_table <- nrows_table |> 
  add_row(
    Sample = "",
    Sample_Selection = NaN
  )

#back to RW
nrows_table <- nrows_table |> 
  bind_rows(bind_cols(
    Sample = "Random Walk Comparison Sample",
    data_7 |> count(name = "Sample_Selection")
  ))

data_9 <-
  data_7 |>
  filter(!is.na(at),
         !is.na(D),
         !is.na(acc_HVZ))


ndata_0907 <- data_9 |> nrow() - data_7 |> nrow()

nrows_table <- nrows_table |> 
  add_row(
    Sample = "Less missing HVZ Data",
    Sample_Selection = ndata_0907
  )

nrows_table <- nrows_table |> 
  bind_rows(bind_cols(
    Sample = "HVZ Comparison Sample",
    data_9 |> count(name = "Sample_Selection")
  )
  )



#Step 10: Life-cycle ------------------------------------------------------------

#add a spacer
nrows_table <- nrows_table |> 
  add_row(
    Sample = "",
    Sample_Selection = NaN
  )

#sum steps 6 - 7
nrows_table <- nrows_table |> 
  bind_rows(bind_cols(
    Sample = "Random Walk Comparison Sample",
    data_7 |> count(name = "Sample_Selection")
  ))

data_8 <-
  data_7 |>
  filter(!is.na(lifecycle))


ndata_0807 <- data_8 |> nrow() - data_7 |> nrow()

nrows_table <- nrows_table |> 
  add_row(
    Sample = "Less missing Lifecycle Data",
    Sample_Selection = ndata_0807
  )

nrows_table <- nrows_table |> 
  bind_rows(bind_cols(
    Sample = "EP-LIFE Comparison Sample",
    data_8 |> count(name = "Sample_Selection")
  )
  )




#Step 11: Industry -------------------------------------------------------------

#add a spacer
nrows_table <- nrows_table |> 
  add_row(
    Sample = "",
    Sample_Selection = NaN
  )

#back to RW
nrows_table <- nrows_table |> 
  bind_rows(bind_cols(
    Sample = "Random Walk Comparison Sample",
    data_7 |> count(name = "Sample_Selection")
  ))

gic_fcasts <- read_parquet(glue("{data_path}/t{fcst_horizon}-OLS-EP-GIC-fcasts.parquet")) 


data_10 <-
  data_7 |>
  inner_join(gic_fcasts, by = c("gvkey","datadate"))

ndata_1007 <- data_10 |> nrow() - data_7 |> nrow()

nrows_table <- nrows_table |> 
  add_row(
    Sample = "Less missing industry-level Data",
    Sample_Selection = ndata_1007
  )

nrows_table <- nrows_table |> 
  bind_rows(bind_cols(
    Sample = "EP-GIC Comparison Sample",
    data_10 |> count(name = "Sample_Selection")
  )
  )



write_csv(nrows_table, glue("out/T1-Sample-Selection.csv"), na = '')



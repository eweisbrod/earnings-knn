# Run all the matches from the same dataset
## load libraries
library(glue)
library(tidyverse)
library(tictoc)
library(lubridate)

# load helper scripts
source("src/-Global-Parameters.R")
source("src/utils.R")

# parameter for replicating scaling by average assets
scalar <- ifelse(deflator == "AT", "at_avg", deflator)
fcst_horizon <- 1



# Data Wrangling ---------------------------------------------------------------

data0 <- read_parquet(glue('{data_path}/data0.parquet'))

# max_year <- 2019L - fcst_horizon
max_year <- year(max(testing_datelist))
min_year <- year(min(testing_datelist))
 
bcg_data <-
  data0 |>  
  arrange(gvkey, calyear) |>
  group_by(gvkey) |>
  mutate(
    avg_at = if_else(((at + at_lag_1) / 2) > 0,
      ((at + at_lag_1) / 2), NA_real_
    ),
    Scalar = if_else((.data[[scalar]] > 0 & !is.na(.data[[scalar]])), 
                     .data[[scalar]], NA_real_),
    ROA = EBSI / Scalar,
    ROA = EBSI / Scalar,
    LOSS = EBSI < 0,
    match_date = calyear - fcst_horizon - 1
  ) |>
  arrange(gvkey, calyear) |>
  group_by(gvkey) |>
  mutate(
    FEBSI       = lead(EBSI, fcst_horizon),
    FScalar     = lead(Scalar, fcst_horizon),
    FROA        = FEBSI / FScalar,
    Fcalyear    = lead(calyear, fcst_horizon),
  ) |>
  ungroup() |>
  filter(
    !is.na(MVE) & MVE > 0,
    !(sic4 %in% (4900:4999)),
    !(sic4 %in% (6000:6999))
  ) |>
  filter(is.na(EBSI) == FALSE) |>
  filter(
    !is.na(Fcalyear) | Fcalyear == calyear + fcst_horizon,
    calyear_lag_1 == calyear - 1
  ) |>
  filter(
    year(datadate) >= min_year - 3 & year(datadate) <= max_year
  ) |>
  filter(
    !is.na(ROA),
    !is.na(FROA)
  ) |> 
  select(gvkey, calyear, datadate, MVE, avg_at, LOSS,
         ROA, FROA, FEBSI, EBSI, match_date, FScalar, at, B, csho, sale) |>
  mutate(
    peer_ROA_growth = FROA - ROA,
    peer_asset_growth = FScalar / .data[[scalar]],
    dd_year  = lubridate::year(datadate),
    dd_month = lubridate::month(datadate)
  )

set.seed(123456)


lag_date_funs <- Map(function(x) {function(dat) {dat %m-% months(x)}}, 24:35)
bcg_peer_windows <- 
  bcg_data |> 
  select(datadate) |> 
  distinct() |> 
  arrange(datadate) |> 
  mutate(across(datadate, lag_date_funs)) |> 
  pivot_longer(cols = c(-datadate), values_to = "peer_bin_window") |> 
  select(-name) |> 
  # account for leap year - just do not match on day as well, month and year suffices
  mutate(pb_window_year  = lubridate::year(peer_bin_window),
         pb_window_month = lubridate::month(peer_bin_window))


# Create bins for matching and prepping the base peer data
bcg_peer_bins <-
  bcg_peer_windows |>
  inner_join(
    select(bcg_data, 
           peer_gvkey = gvkey, ROA, peer_ROA_growth, 
           peer_asset_growth, .data[[scalar]], LOSS,
           dd_year, dd_month),
    by = c("pb_window_year" = "dd_year", "pb_window_month" = "dd_month")
    #by = c("peer_bin_window" = "datadate")
  ) |> 
  group_by(datadate, LOSS) |>  # adding ROA cuts 
  mutate(
    ROA_q25 = quantile(ROA, probs = 0.25),
    ROA_q50 = quantile(ROA, probs = 0.50),
    ROA_q75 = quantile(ROA, probs = 0.75)
  ) |>
  ungroup() |>
  rowwise() |>
  mutate(ROA_bins_peer = if_else(
    LOSS == TRUE,
    cut(ROA, breaks = c(-Inf, ROA_q50, Inf),labels = FALSE),
    cut(ROA, breaks = c(-Inf, ROA_q25, ROA_q50, ROA_q75, Inf), labels = FALSE)
    )
  ) |>
  ungroup() |>
  # adding size cuts (quintiles) within ROA cuts
  group_by(datadate, LOSS, ROA_bins_peer) |> 
  mutate(
    size_q20 = quantile(.data[[scalar]], probs = 0.2),
    size_q40 = quantile(.data[[scalar]], probs = 0.4),
    size_q60 = quantile(.data[[scalar]], probs = 0.6),
    size_q80 = quantile(.data[[scalar]], probs = 0.8)
  ) |>
  ungroup() |>
  rowwise() |>
  mutate(
    size_bins_peer = cut(
      .data[[scalar]],
      breaks = c(-Inf, size_q20, size_q40, size_q60, size_q80, Inf),
      labels = FALSE
    )
  ) |>
  ungroup() 


# extracting cutoffs for the subject sample
bcg_cuts <- 
  bcg_peer_bins |> 
  select(datadate, LOSS, 
         size_q20, size_q40, size_q60, size_q80,
         ROA_q25, ROA_q50, ROA_q75, ROA_bins_peer
   ) |> 
  distinct()

# Assign bins to subject firms (based on the 2 year old cutoffs)
bcg_subject_bins <- 
  bcg_data |>
  inner_join(bcg_cuts, by = c("datadate" = "datadate", "LOSS" = "LOSS")) |>
  rowwise() |>
  mutate(ROA_bins = if_else(
    LOSS,
    cut(ROA, breaks = c(-Inf, ROA_q50, Inf),labels = FALSE),
    cut(ROA, breaks = c(-Inf, ROA_q25, ROA_q50, ROA_q75, Inf), labels = FALSE)
    )
  ) |>
  ungroup() |> 
  filter(ROA_bins == ROA_bins_peer) |> 
  rowwise() |>
  mutate(
    size_bins = cut(
      .data[[scalar]],
      breaks = c(-Inf, size_q20, size_q40, size_q60, size_q80, Inf),
      labels = FALSE
    )
  ) |>
  ungroup() |> 
  select(-ROA_bins_peer)


# Match 50 random draws to each subject 
bcg_fcasts <- 
  bcg_subject_bins |>
  select(gvkey, datadate, LOSS, ROA_bins, size_bins, 
         ROA, avg_at, at, B, csho, sale, MVE, EBSI, FEBSI) |>
  inner_join(
    select(bcg_peer_bins, 
           peer_gvkey, datadate, LOSS, ROA_bins_peer, size_bins_peer,
           peer_ROA_growth, peer_asset_growth),
    by = c("datadate" = "datadate", "LOSS" = "LOSS", 
           "ROA_bins" = "ROA_bins_peer",
           "size_bins" = "size_bins_peer")
  ) |>
  group_by(gvkey, datadate, MVE, EBSI, FEBSI) |>
  sample_n(50, replace = TRUE) |>    # draw sample of 50 firms
  # asset growth is 1 + growth, therefore multiplication
  mutate(fcast = 
           (ROA + peer_ROA_growth) * (.data[[scalar]] * peer_asset_growth)) |>
  summarize(
    n_peers = n(),
    median_roa_growth = median(peer_ROA_growth),
    median_asset_growth = median(peer_asset_growth),
    fcast = median(fcast),
    .groups = "drop"
  ) |>
  ungroup() |> 
  mutate(Model = "BCG")


write_parquet(bcg_fcasts,
              glue("{data_path}/t{fcst_horizon}-BCG-fcasts.parquet"))




# Setup -------------------------------------------------------------------
library(dbplyr)
library(RPostgres)
library(DBI)
library(tidyverse)

write_parquet <- arrow::write_parquet
write_parquet <- function(x, p) {
  arrow::write_parquet(x, p, compression='gzip', compression_level=5)
}

source("src/-Global-Parameters.R")


# Log into wrds -----------------------------------------------------------

if(exists("wrds")){
  dbDisconnect(wrds)  # because otherwise WRDS might time out
}

wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  user=rstudioapi::askForSecret("WRDS user"),
                  password=rstudioapi::askForSecret("WRDS pw"),
                  sslmode='require',
                  dbname='wrds')
wrds  # checking if connection exists



# Computstat --------------------------------------------------------------
comp.funda <- tbl(wrds,in_schema("comp", "funda"))
comp.company <- tbl(wrds,in_schema("comp", "company"))
ccm.comphist <- tbl(wrds,in_schema("crsp", "comphist"))
comp.hgic <- tbl(wrds,in_schema("comp", "co_hgic"))

# hgind <- ccm.comphist %>%
#   select(gvkey, hgind, hchgdt, hchgenddt) %>%
#   collect()


hgic <- comp.hgic %>%
  #select(gvkey, hgind, hchgdt, hchgenddt) %>%
  collect()


raw_funda <-
  comp.funda %>%
  filter(indfmt=='INDL', datafmt=='STD', popsrc=='D' ,consol=='C') %>%
  select(gvkey, datadate, conm, fyear, fyr, cstat_cusip=cusip, cik,
         cstat_ticker= tic, sich, ib, ibc, spi, at, dvc, act, che, lct, dlc, txp,
         xrd, dp, ceq, sale,csho, prcc_f, ajex, ni,
         epsfi, epsfx, epspi, epspx, opeps, cshfd, cshpri,
         oancf, ivncf, fincf
  ) %>%
  inner_join(select(comp.company, gvkey, sic, fic, gind), by="gvkey") %>%
  mutate(sic4 = case_when( is.null(sich) ~ as.numeric(sic), TRUE ~ sich)) %>%
  mutate(sic2 = floor(sic4/100)) %>%
  # replace missings with 0 for defined vars
  mutate_at(vars(spi, dvc, che, lct, dlc, txp, dp, xrd),
            .funs=funs(coalesce(., 0))) %>%
  mutate(
    # align the data in calendar time following HVZ
    # they use June of each year and assume a 3 month reporting lag
    # so effectively this is coded as aligning as of March each year
    # See HVZ (2012) figure 1
    calyear = if_else( fyr > 3,
                       sql("extract(year from datadate)")+1,
                       sql("extract(year from datadate)")),
    mve = csho * prcc_f,
    e= ib-spi,
    gics_year = sql("extract(year from datadate)"),
    gics_month = sql("extract(month from datadate)")
                    ) %>%
  filter(1955 < fyear) %>%
  filter(fic=="USA") %>%
  # download to local machine
  collect()





# two ways to check the calendar year alignment:
raw_funda %>%
  filter(calyear==2017) %>%
  distinct(fyr,fyear,calyear,datadate) %>%
  arrange(datadate)

raw_funda %>%
  filter(fyear==2016) %>%
  distinct(fyr,fyear,calyear,datadate) %>%
  arrange(datadate)

# check for dups
raw_funda %>%
  group_by(gvkey,calyear) %>%
  count() %>%
  arrange(-n)
# there are some dups due to companies changing their fiscal end

# how many dups?
raw_funda %>%
  group_by(gvkey,calyear) %>%
  count() %>%
  ungroup() %>%
  filter(n > 1) %>%
  count()


# Not that many, so lets just delete both records when this happens
# this will spill over later to also delete any fiscal years that use
# these years as leads or lags, which I think is reasonable
raw_funda <- raw_funda %>%
  semi_join(raw_funda %>%
              group_by(gvkey,calyear) %>%
              count() %>%
              ungroup() %>%
              filter(n ==1),
            by=c('gvkey'='gvkey', 'calyear'='calyear'))
#433,931 after deleting dups


#check for dups
raw_funda %>%
  count(gvkey, calyear) %>%
  filter(n > 1) %>%
  arrange(-n)
#no dups now

#check n obs by calyear:
raw_funda %>%
  count(calyear) %>%
  print(n=Inf)


#add hgind
cs_with_hgic <- raw_funda %>%
  select(gvkey,datadate) %>% 
left_join(hgic, by = "gvkey") %>%
  filter(datadate >= indfrom & (datadate <= indthru | is.na(indthru))) %>%
  arrange(gvkey, datadate) %>%
  # select(-hchgdt, -hchgenddt) %>%  # checking whether there was weird matching.
  distinct()  # sometimes the join does weird things, so clean up using distinct

#take the historical gind if avail, otherwise take the current gind
funda_best_gic <- raw_funda %>% 
left_join(cs_with_hgic %>% select(gvkey,datadate,gindh=gind)) %>% 
  mutate(best_gic = if_else(is.na(gindh),gind,gindh),
         check_gic = best_gic == gind)
#same nobs as raw_funda so join is ok.

funda_best_gic %>% count(check_gic)

write_parquet(funda_best_gic, 'data/raw_funda.parquet')


no_dividends <-
  comp.funda %>%
  filter(indfmt=='INDL', datafmt=='STD', popsrc=='D' ,consol=='C') %>%
  select(gvkey, datadate, conm, fyear, fyr, cstat_cusip=cusip, cik, fic,
         cstat_ticker= tic, dvc) %>%
  mutate(
    # align the data in calendar time following HVZ
    # they use June of each year and assume a 3 month reporting lag
    # so effectively this is coded as aligning as of March each year
    # See HVZ (2012) figure 1
    calyear = if_else( fyr > 3,
                       sql("extract(year from datadate)")+1,
                       sql("extract(year from datadate)")),
    no_dividends = if_else(is.na(dvc), 1, 0)
    ) %>%
  filter(1955 < fyear) %>%
  filter(fic=="USA") %>%
  # download to local machine
  collect()

no_dividends <-
  no_dividends %>%
  select(gvkey, calyear, no_dividends) %>%
  distinct()

write_parquet(no_dividends, 'data/no_div_indic.parquet')






capex <-
  comp.funda %>%
  filter(indfmt=='INDL', datafmt=='STD', popsrc=='D' ,consol=='C') %>%
  select(gvkey, datadate, conm, fyear, fyr, cstat_cusip=cusip, cik, fic,
         cstat_ticker= tic, capx, capxv) %>%
  mutate(
    # align the data in calendar time following HVZ
    # they use June of each year and assume a 3 month reporting lag
    # so effectively this is coded as aligning as of March each year
    # See HVZ (2012) figure 1
    calyear = if_else( fyr > 3,
                       sql("extract(year from datadate)")+1,
                       sql("extract(year from datadate)"))
  ) %>%
  filter(1955 < fyear) %>%
  filter(fic=="USA") %>%
  # download to local machine
  collect()



capex2 <-
  capex %>%
  select(gvkey, calyear, datadate, capx, capxv) %>%
  mutate(capx = if_else(is.na(capx), capxv, capx)) %>%
  select(-capxv) %>%
  distinct() %>%
  drop_na() %>%
  distinct() %>%
  arrange(gvkey, calyear, datadate) %>%
  group_by(gvkey, calyear) %>%
  slice(n()) %>%
  ungroup() %>%
  select(-datadate)

write_parquet(capex2, 'data/capex.parquet')





# IBES ------------------------------------------------------------------------------

compustat_ibes_linktable <-
  tbl(wrds, in_schema("comp", "security")) %>%
  filter(is.na(ibtic) == FALSE) %>%
  select(gvkey, iid, ibtic)

compustat_ibes_linktable_local <-
  tbl(wrds, in_schema("comp", "security")) %>%
  filter(is.na(ibtic) == FALSE) %>%
  select(gvkey, iid, ibtic) %>%
  collect()

write_parquet(compustat_ibes_linktable_local, 'data/compustat_ibes_linktable.parquet')

raw_street_actuals <-
  tbl(wrds, in_schema("ibes", "actu_epsus")) %>%
  filter(
    measure == "EPS",
    is.na(value) == FALSE,
    curr_act == "USD",
    pdicity == "ANN"
    ) %>%
  collect()

write_parquet(raw_street_actuals, 'data/street-actuals.parquet')


analyst_consensus <-
  tbl(wrds,in_schema('ibes','statsumu_epsus')) %>%
  filter(
    fiscalp == "ANN",
    fpi %in% c("1", "2","3"),
    #curr_act == 'USD',
    curcode == 'USD'
    )

analyst_consensus_local <-
  tbl(wrds,in_schema('ibes','statsumu_epsus')) %>%
  filter(
    fiscalp == "ANN",
    fpi %in% c("1", "2","3"),
    #curr_act == 'USD',
    curcode == 'USD'
  ) %>%
  collect()

write_parquet(analyst_consensus_local, 'data/analyst-consensus.parquet')

adjsum <-
  tbl(wrds, in_schema("ibes", "adjsum")) %>%
  arrange(ticker,statpers) %>%
  group_by(ticker) %>%
  mutate(nextpers = coalesce(lead(statpers,1L),as.Date("2050-12-31"))
  ) %>%
  ungroup() %>%
  select(ibtic=ticker,bpers=statpers,epers=nextpers,adjspf)

adj <-
  tbl(wrds, in_schema("ibes", "adj")) %>%
  arrange(ticker,spdates) %>%
  group_by(ticker) %>%
  mutate(nextsp = coalesce(lead(spdates,1L),as.Date("2050-12-31"))
  ) %>%
  ungroup() %>%
  select(ibtic=ticker,bdate=spdates,edate=nextsp,adj)

analyst_consensus_adj <- analyst_consensus %>%
  inner_join(adjsum, sql_on = '"LHS".ticker="RHS".ibtic AND
             ("RHS".bpers <= "LHS".statpers) AND
             ("LHS".statpers < "RHS".epers)'
  ) %>%
  select(-ibtic) %>%
  collect()

write_parquet(analyst_consensus_adj, 'data/analyst_consensus_adj.parquet')


raw_street_actuals_adj <-
  tbl(wrds, in_schema("ibes", "actu_epsus")) %>%
  filter(
    measure == "EPS",
    is.na(value) == FALSE,
    curr_act == "USD",
    pdicity == "ANN"
  ) %>%
  #assume that if the announcement date is missing we can use the period end date
  mutate(linkdate = coalesce(anndats,pends)) %>%
  inner_join(adj, sql_on = '"LHS".ticker="RHS".ibtic AND
             ("RHS".bdate <= "LHS".linkdate) AND
             ("LHS".linkdate < "RHS".edate)'
  ) %>%
  select(-ibtic) %>%
  collect()


write_parquet(raw_street_actuals_adj, 'data/street_actuals_adj.parquet')

#make a temporary view of raw funda just to get prices and calyears, etc.
ptemp <- comp.funda %>%
  filter(indfmt=='INDL', datafmt=='STD', popsrc=='D' ,consol=='C') %>%
  select(gvkey, datadate, fyr, prcc_f
  ) %>%
  mutate(
    # align the data in calendar time following HVZ
    # they use June of each year and assume a 3 month reporting lag
    # so effectively this is coded as aligning as of March each year
    # See HVZ (2012) figure 1
    calyear = if_else( fyr > 3,
                       sql("extract(year from datadate)")+1,
                       sql("extract(year from datadate)"))) %>%
  filter(!is.na(prcc_f))

#make a file linking compustat price to the ibes data
cstat_ibes_prccf <- adj %>%
  inner_join(  tbl(wrds, in_schema("comp", "security")) %>%
                 filter(is.na(ibtic) == FALSE) %>%
                 select(gvkey, iid, ibtic) , by=c('ibtic')) %>%
  filter(iid == "01")  %>%
  rename(lgvkey=gvkey) %>%
  inner_join(ptemp, sql_on = '"LHS".lgvkey="RHS".gvkey AND
             ("LHS".bdate <= "RHS".datadate) AND
             ("RHS".datadate < "LHS".edate)'
  ) %>%
  select(-lgvkey) %>%
  collect()

write_parquet(cstat_ibes_prccf, 'data/cstat_ibes_prccf.parquet')



# Crsp ------------------------------------------------------------------------------
ccm_link <-
  tbl(wrds, in_schema("crsp", "ccmxpf_linktable")) %>%
  filter(
    usedflag %in% c(0, 1),
    linktype %in% c('LU', 'LC'),
    linkprim %in% c("P", "C")
    ) %>%
  mutate(
    linkenddt = if_else(is.na(linkenddt) == T, date("2030-06-30"), linkenddt)
    ) %>%
  collect()

chosen_permno <- unique(ccm_link$lpermno)

monthly_delisting_returns <-
  tbl(wrds, in_schema("crsp", "msedelist")) %>%
  select(permno, dlstdt, dlret, dlstcd)

monthly_returns <-
  tbl(wrds, in_schema("crsp", "msf")) %>%
  filter(
    permno %in% chosen_permno,
    between(date, crsp_begin_date, crsp_end_date)
    ) %>%
  mutate(begdt = sql("date - interval '1 month'")) %>%
  group_by(permno) %>%
  mutate(
    Price  = abs(prc),
    MVE    = (Price * shrout) / 1000, # divide by 1000 to adjust to Compu
  ) %>%
  ungroup() %>%
  select(permno, date, ret, Price, MVE, begdt)

monthly_with_delistings <-
  monthly_returns %>%
  left_join(monthly_delisting_returns,
    sql_on = paste0(
      '("LHS".permno = "RHS".permno) AND ',
      '("LHS".date   >= "RHS".dlstdt) AND ',
      '("LHS".begdt  <= "RHS".dlstdt) '
      )
    ) %>%
  rename(permno = permno.x) %>%
  select(-permno.y) %>%
  collect()

monthly_compinfo <-
  tbl(wrds, in_schema("crsp", "msenames")) %>%
  filter(
    shrcd %in% c(10, 11),
    exchcd %in% c(1, 2, 3),
    primexch %in% c("N", "A", "Q")
    ) %>%
  select(permno, namedt, nameendt, exchcd) %>%
  collect()

mcrsp <-
  monthly_with_delistings %>%
  inner_join(monthly_compinfo, by="permno") %>%
  arrange(permno, date, dlstcd) %>%
  group_by(permno, date, dlstcd) %>%
  mutate(max_nameendt = max(nameendt)) %>%
  ungroup() %>%
  filter((date >= namedt & date <= nameendt) |
         (date >= nameendt & nameendt == max_nameendt & !is.na(dlstcd))) %>%
  mutate(delret = case_when(
    is.na(dlret) == T & (dlstcd==500 | (dlstcd>=520 & dlstcd<=584)) & (exchcd==1 | exchcd==2) ~ -0.3,
    is.na(dlret) == T & (dlstcd==500 | (dlstcd>=520 & dlstcd<=584)) & exchcd==3 ~ -0.55,
    TRUE ~ dlret)
    ) %>%
  select(-namedt, -nameendt, -max_nameendt, -begdt) %>%
  collect()

rm(monthly_with_delistings, monthly_compinfo)

monthly_indices <-
  tbl(wrds, in_schema("crsp", "msi")) %>%
  select(date, MktRet=vwretd, MktEWRet=ewretd) %>%
  filter(between(date, crsp_begin_date, crsp_end_date)) %>%
  collect()

write_parquet(monthly_indices, "data/monthly_indices.parquet")
write_parquet(mcrsp, "data/mcrsp.parquet")
write_parquet(ccm_link, "data/ccm_link.parquet")

dbDisconnect(wrds)

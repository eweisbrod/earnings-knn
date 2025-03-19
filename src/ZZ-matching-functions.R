#' This script holds the matching machinery used in all scripts that estimate matching
#' models.

# Define matching functions ----------------------------------------------------

#Generate the standardized data lists ------------------------------------------
gen_windowed_dta <- function(dta,
                             datelist,
                             j,
                             window_type="fixed",
                             rws=roll_win_size,
                             match_vars,
                             standardize=T,
                             fcast_hzn
) {
  # Computes fixed or expanding rolling windows anchored on year j. Returns a list of peer
  # and subject data frames
  if (window_type == "fixed") {
    peers_a_subjs <-
      dta %>%
      select(gvkey, datadate, all_of(match_vars), FEARN, Fdatadate) %>%
      filter(datadate == datelist[[j]] | 
               (datadate <= (datelist[[j]]  %m-% months(fcast_hzn*12)) 
                & (datadate > (datelist[[j]] %m-% months((fcast_hzn*12) + 12*roll_win_size)))) 
      ) 
  } else if (window_type == "expanding") {
    peers_a_subjs <-
      dta %>%
      select(gvkey, datadate, all_of(match_vars), FEARN, Fdatadate) %>%
      filter(datadate == datelist[[j]] | 
               (datadate <= (datelist[[j]] %m-% months(fcast_hzn*12)) 
               ) 
      ) 
  } else {
    stop("Error: window_type must be either 'fixed' or 'expanding'")
  }
  
  if (standardize == T) {
    peers_a_subjs <- mutate_at(peers_a_subjs, match_vars, ~ scale(.)[, 1])
  }
  
  peers <- peers_a_subjs %>%
    filter(
      datadate <= (datelist[[j]] %m-% months(fcast_hzn*12)),
      is.na(FEARN) == FALSE,
      month(Fdatadate) == month(datadate),
      year(Fdatadate) == year(datadate) + fcast_hzn
    )
  
  subjs <- filter(peers_a_subjs, datadate == datelist[[j]]) %>%
    select(-FEARN)
  
  return(list('xpeers'=peers %>% select(all_of(match_vars)),
              'xsubjs'=subjs %>% select(all_of(match_vars)),
              'id_peers'=peers %>% select(gvkey, datadate),
              'id_subjs'=subjs %>% select(gvkey, datadate)
  ))
}

#Compute the peers, given K ----------------------------------------------------

compute_knn_peers <- function(win_dta, K=myK) {
  # takes a list of 4 data frames (2 for peers and 2 for subjects) and computes the K
  # nearest neighbors out of peers for all subjects. Returns a linking table for subject
  # and peer gvkey and datadates
  nnout <- get.knnx(data=win_dta[['xpeers']],
                    query=win_dta[['xsubjs']],
                    k=K, algo="kd_tree")
  
  if (K == 1) {
    query_results <-
      cbind(win_dta[['id_subjs']], index=nnout$nn.index, dist=nnout$nn.dist)
  } else {
    query_results <-
      cbind(win_dta[['id_subjs']], index=nnout$nn.index, dist=nnout$nn.dist) %>%
      pivot_longer(contains("."),
                   names_to = c(".value", "rank"),
                   names_pattern = "(.*)\\.(.*)") %>%
      select(-rank)
  }
  
  peer_data <-
    win_dta[['id_peers']][query_results$index, ] %>%
    rename(gvkey_peer = gvkey,
           datadate_peer = datadate)
  
  subj_peer_match <-
    bind_cols(query_results, peer_data) %>%
    rename(gvkey_subj = gvkey,
           datadate_subj = datadate) %>%
    select(-index)
  
  return(subj_peer_match)
}

#find the median earnings of the peers to use as forecast ----------------------

get_med_earn <- function(full_dta, peer_dta) {
  # Takes the original matching data and a peer-subj linking table and computes the
  # median t-year forward EARN per subj
  peer_earn <-
    full_dta %>%
    mutate(peer_FEARN = FEARN) %>%
    select(gvkey, datadate, peer_FEARN) %>%
    inner_join(peer_dta,
               by=c(gvkey="gvkey_peer",
                    datadate="datadate_peer"))
  
  match_forecast <-
    peer_earn %>%
    inner_join(full_dta %>% select(gvkey, datadate,
                                   subject_Scalar=Scalar, FEARN),
               by=c(gvkey_subj="gvkey",
                    datadate_subj="datadate")) %>%
    mutate(fcast = peer_FEARN * subject_Scalar) %>%
    group_by(gvkey_subj, datadate_subj) %>%
    summarize(.groups = 'drop', 
              fcast = median(fcast)
    ) %>% 
    ungroup() %>%
    inner_join(full_dta %>% 
                 select(gvkey, datadate,
                        Scalar, FEBSI, MVE),
               by=c(gvkey_subj="gvkey",
                    datadate_subj="datadate")) %>% 
    rename(gvkey = gvkey_subj,
           datadate = datadate_subj)
  
  return(match_forecast)
}


#Generate Descriptive Statistics about the Peers -------------------------------

gen_peer_stats <- function(peer_dta, full_dta) {
  
  #select variables for each peer
  peer_stats <-
    full_dta %>%
    mutate(ACC = acc/MVE) %>% 
    select(Peer.gvkey = gvkey, 
           Peer.datadate= datadate, 
           Peer.EARN=EARN,
           Peer.EARN_lag_1 = EARN_lag_1,
           Peer.ACC = ACC,
           Peer.sic2=sic2,
           Peer.FF12=FF12,
           Peer.gics=best_gic,
           Peer.MVE=MVE,
           Peer.age=age,
           Peer.lifecycle = lifecycle,
           Peer.IBES_covered = IBES_covered
    ) %>%
    inner_join(peer_dta,
               by=c("Peer.gvkey"="gvkey_peer",
                    "Peer.datadate"="datadate_peer"))
  
  #join peer stats with subject stats
  peer_subject_stats <-
    full_dta %>%
    mutate(ACC = acc/MVE) %>%
    select(Subj.gvkey = gvkey, 
           Subj.datadate= datadate, 
           Subj.EARN=EARN,
           Subj.EARN_lag_1 = EARN_lag_1,
           Subj.ACC = ACC,
           Subj.sic2=sic2,
           Subj.FF12=FF12,
           Subj.gics=best_gic,
           Subj.MVE=MVE,
           Subj.age=age,
           Subj.lifecycle = lifecycle,
           Subj.IBES_covered = IBES_covered
    ) %>% 
    inner_join(peer_stats,
               by=c("Subj.gvkey"="gvkey_subj",
                    "Subj.datadate"="datadate_subj")) %>% 
    arrange(Subj.gvkey,Subj.datadate)
  
  
  peer_subject_stats %>% distinct(Subj.gvkey,Subj.datadate)
  
  #return the results
  return(peer_subject_stats)
}

#find the IQR of the peers -----------------------------------------------------



get_std_earn <- function(full_dta, peer_dta) {
  # Takes the original matching data and the a peer-subj linking table and computes the
  # standard deviation of t-year forward EARN per subj
  peer_earn <-
    full_dta %>%
    mutate(peer_FEARN = FEARN) %>%
    select(gvkey, datadate, peer_FEARN) %>%
    inner_join(peer_dta,
               by=c(gvkey="gvkey_peer",
                    datadate="datadate_peer"))
  
  match_forecast <-
    peer_earn %>%
    inner_join(full_dta %>% select(gvkey, datadate, subject_Scalar=Scalar),
               by=c(gvkey_subj="gvkey",
                    datadate_subj="datadate")) %>%
    #I don't think it should be unscaled
    #mutate(fcast = peer_FEARN * subject_Scalar) %>%
    group_by(gvkey_subj, datadate_subj) %>%
    summarize(.groups = 'drop', spread = sd(peer_FEARN),
              p75 = quantile(peer_FEARN, probs=.75,na.rm=T),
              median = median(peer_FEARN, na.rm = T),
              p25 = quantile(peer_FEARN, probs=.25),
              d2 = if_else(abs(p75 + p25) < .1, 
                           .1, 
                           abs(p75 + p25)
              ),
              d3 = if_else(abs(median(peer_FEARN, na.rm=T)) < .1, 
                           .1, 
                           (abs(median(peer_FEARN,na.rm=T)))
              ),
              IQR = p75 - p25,
              MAD = median(abs(peer_FEARN - median),na.rm = T),
              QCV = IQR / d2,
              IQR3 = IQR / d3,
              MADM = MAD / d3
    ) %>%
    ungroup() %>%
    rename(gvkey = gvkey_subj,
           datadate = datadate_subj)
  
  return(match_forecast)
}


#wrapper to do the full forecast process given input data ----------------------
gen_knn_fcasts <- function(model, 
                           var_idx,
                           datelist,
                           dta,
                           .win_type = "fixed",
                           .rws = 10L,
                           .dateidx,
                           .m,
                           .k,
                           .fcst_hzn,
                           .descriptives = F) {
  #' Wrapper function to generate forecasts given a window type and 
  vars_used <- unlist(var_idx[[model]][.m])
  windowed_dta <- gen_windowed_dta(dta, 
                                   datelist = datelist,
                                   j = .dateidx, 
                                   window_type = .win_type,
                                   rws = .rws,
                                   match_vars = vars_used,
                                   fcast_hzn = .fcst_hzn)
  peers <- compute_knn_peers(windowed_dta, K = .k)
  fcasts <- get_med_earn(dta, peers) %>%
    mutate(Model = model, M = .m, K = .k, FcstHzn = .fcst_hzn)
  
  if (.descriptives == T) {
    #generate the peer data
    descriptives <- gen_peer_stats(peers,dta) %>% 
      mutate(Model = model, M = .m, K = .k, FcstHzn = .fcst_hzn)
    
    #calculate IQR
    IQR <- get_std_earn(dta,peers)
    
    #append IQR to forecasts
    fcasts <- fcasts %>% 
      inner_join(IQR, by=c("gvkey", "datadate"))
    
    #output both datasets in a list
    results <- list(fcasts=fcasts,peer_data=descriptives)
    
    return(results)
    
  } else if (.descriptives == "IQR_only") {
    
    IQR <- get_std_earn(dta,peers)
    fcasts <- fcasts %>% 
      inner_join(IQR, by=c("gvkey", "datadate"))
    return(fcasts) 
    
  } else {
    
    return(fcasts) 
  }
  
}

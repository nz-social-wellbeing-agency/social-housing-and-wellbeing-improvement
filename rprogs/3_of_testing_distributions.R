# ================================================================================================ #
# Description: Testing if the recalibrated weights produced after linking to the spine have 
#   a distribution of outcomes/descriptive variabes that are consistent with the original 
#   distribution.
#
# Input: 
# of_rewt_gss_preson_replicates.R (or similar script) to produce datasets for consistency checking.
#
# Output: 
# results.csv providing p-values that the corresponding distributions are the same.
#
# Author: S Anastasiadis
#
# Dependencies:
# NA
#
# Notes:
# Output csv file is titled "comparison_pvals.csv". This has been inspected, with additional 
# comments and description in "validity of reweighting checks.xls".
#
# History (reverse order): 
# 22 Sep 2017 SA v1
# ================================================================================================ #

############### prepare data ###################

# build combined dataset
compare_data <- left_join(dataset,all_weights_df,by = 'snz_uid', suffix=c(".ds",".new")) %>%
  # select("snz_uid",
  #        contains('.new'),
  #        contains('HH_'),
  #        contains('qual_'),
  #        contains('health_'),
  #        contains('birth_year'),
  #        contains('born_nz'),
  #        contains('descent_'),
  #        contains('paid_work'),
  #        contains('dvage_'),
  #        contains('Region_'),
  #        contains('Eth_'),
  #        contains('AOD_'),
  #        contains('feel_life'),
  #        contains('house_'),
  #        contains('Wgt')) %>%
  dplyr::mutate(
    gss_pq_house_condition_code = ifelse( is.na(get("gss_pq_house_condition_code")), -1, gss_pq_house_condition_code )
         ,housing_satisfaction =  ifelse(is.na(get("housing_satisfaction")), -1, housing_satisfaction)
         ,gss_pq_safe_day_hood_code =  ifelse(is.na(get("gss_pq_safe_day_hood_code")), -1, gss_pq_safe_day_hood_code)
         ,lbr_force_status = factor(ifelse(is.na(as.character(get("lbr_force_status"))), "UNK", as.character(lbr_force_status)))
         )
# compare_data$gss_pq_house_condition_code[which(is.na(compare_data$gss_pq_house_condition_code))] <- -1
# compare_data$housing_satisfaction[which(is.na(compare_data$housing_satisfaction))] <- -1
# compare_data$gss_pq_safe_day_hood_code[which(is.na(compare_data$gss_pq_safe_day_hood_code))] <- -1
# compare_data$lbr_force_status <- as.character(compare_data$lbr_force_status)
# compare_data$lbr_force_status[which(is.na(compare_data$lbr_force_status))] <- "UNK"
# compare_data$lbr_force_status <- as.factor(compare_data$lbr_force_status)



# list of columns containing weights
wgt_prefix <- c("link_","gss_pq_person_")
wgt_list   <- c(  'FinalWgt', paste('FinalWgt',1:100,sep=''))

# list of columns containing parameters whose distribution we care about
param_list <- c("gss_hq_sex_dev"
                ,"gss_hq_house_trust"
                ,"gss_hq_house_own"
                ,"gss_hq_house_pay_mort_code"
                ,"gss_hq_house_pay_rent_code"
                ,"gss_hq_house_who_owns_code"
                ,"gss_pq_HH_tenure_code"
                ,"gss_pq_HH_crowd_code"
                ,"gss_pq_house_mold_code"
                ,"gss_pq_house_cold_code"
                ,"gss_pq_house_condition_code"
                ,"housing_satisfaction"
                ,"gss_pq_prob_hood_noisy_ind"
                ,"gss_pq_safe_night_pub_trans_code"
                ,"gss_pq_safe_night_hood_code"
                ,"gss_pq_safe_day_hood_code"
                ,"gss_pq_discrim_rent_ind"
                ,"gss_pq_crimes_against_ind"
                ,"gss_pq_cult_identity_code"
                ,"gss_pq_lfs_dev"
                ,"gss_pq_highest_qual_dev"
                ,"gss_pq_feel_life_code"
                # ,"gss_pq_voting"
                ,"gss_pq_time_lonely_code"
                ,"gss_hq_household_inc1_dev"
                ,"gss_pq_dvage_code"
                ,"P_EURO"
                ,"P_MAORI"
                ,"P_ASIAN"
                ,"P_PACIFIC"
                ,"P_OTHER"
                ,"gss_pq_HH_comp_code"
                ,"gss_hq_regcouncil_dev"
                ,"adult_ct"
                ,"pub_trpt_safety_ind"
                ,"safety_ind"
                ,"house_crowding_ind"
                ,"crime_exp_ind"
                ,"ct_house_pblms"
                ,"health_nzsf12_physical"
                ,"health_nzsf12_mental"
                # ,"lbr_force_status"
                ,"time_lonely_ind"
                #"voting_ind"
                # ,"hh_gss_income"
                ,"life_satisfaction_ind"
                )

############### run checks ###################

for(i in levels(dataset$gss_id_collection_code) ){
  
  print(i)
  # Filter the required GSS wave for analysis
  test.ds <- compare_data %>% filter(gss_id_collection_code.ds == i)
  
  # storage for Mann-Whitney test results
  sink(paste0('../output/comp_',i,'.csv'))
  
  # Mann-Whitney U Tests
  cat('Mann-Whitney U Tests\n')
  # column headers
  cat('SIA wgt,GSS wgt,',paste(param_list,collapse=','),'\n')
  # iterate through weights
  for(this_wgt in wgt_list){
    
    # current weight
    cat(this_wgt)
    # weight codes
    this_sia_wgt <- paste0(wgt_prefix[1],this_wgt)
    this_snz_wgt <- paste0(wgt_prefix[2],this_wgt)
    print(this_sia_wgt)
    print(this_snz_wgt)
    # test distribution of weights
    test <- distribution_compare(pop1 = test.ds[[this_sia_wgt]], pop2 = test.ds[[this_snz_wgt]], standardize = TRUE)
    cat(',',test$MW_test)
    # test distribution of outcomes
    for(param in param_list){
      # weighted sampling of parameter
      ## Some Error in the code below. Need to debug. 
      tmp_samples <- make_samples(test.ds[[param]], wgt1=test.ds[[this_sia_wgt]], wgt2=test.ds[[this_snz_wgt]])
      # compare
      test <- distribution_compare(pop1 = tmp_samples$pop1, pop2 = tmp_samples$pop2)
      cat(',',test$MW_test)
    }
    # end line
    cat('\n')
  }
  
  # Kolmogorov-Smirnov Tests
  cat('Kolmogorov-Smirnov Tests\n')
  # column headers
  cat('SIA wgt,GSS wgt,',paste(param_list,collapse=','),'\n')
  # iterate through weights
  for(this_wgt in wgt_list){
    # current weight
    cat(this_wgt)
    # weight codes
    this_sia_wgt <- paste0(wgt_prefix[1],this_wgt)
    this_snz_wgt <- paste0(wgt_prefix[2],this_wgt)
    # test distribution of weights
    test <- distribution_compare(pop1 = test.ds[[this_sia_wgt]], pop2 = test.ds[[this_snz_wgt]], standardize = TRUE)
    cat(',',test$KS_test)
    # test distribution of outcomes
    for(param in param_list){
      print(param)
      # weighted sampling of parameter
      tmp_samples <- make_samples( test.ds[[param]] , wgt1=test.ds[[this_sia_wgt]], wgt2=test.ds[[this_snz_wgt]])
      # compare
      test <- distribution_compare(pop1 = tmp_samples$pop1, pop2 = tmp_samples$pop2)
      cat(',',test$KS_test)
    }
    # end line
    cat('\n')
  }
  
  sink()
  
  # plot standardized distribution of weights
  tmp <- test.ds %>%
    # select(gss_pq_person_FinalWgt_nbr,link_FinalWgt_nbr) %>%
    mutate(SNZ_wgt = (gss_pq_person_FinalWgt - mean(gss_pq_person_FinalWgt))/sd(gss_pq_person_FinalWgt)) %>%
    mutate(SIA_wgt = (link_FinalWgt-mean(link_FinalWgt,na.rm=TRUE))/sd(link_FinalWgt,na.rm=TRUE))
  ggplot() +
    geom_density(data=tmp,aes(x=SNZ_wgt),col='red') +
    geom_density(data=tmp %>% filter(!is.na(SIA_wgt)),aes(x=SIA_wgt))
}



##Other checks and outputing plots
##Simple output of the weights vs after reweighting distribution plots

##Juct checking for overall population by snz_uid weights
gss_prefix <- 'gss_pq_person_FinalWgt'
sia_prefix <- 'link_FinalWgt'
n <- ''


plot_df <- compare_data %>% 
  dplyr::select_(paste0(gss_prefix, n), paste0(sia_prefix, n), 'snz_uid', 'gss_id_collection_code.ds') %>%
  melt(id.vars=c('snz_uid', 'gss_id_collection_code.ds'))


p <- ggplot(data=plot_df) +
  geom_density(aes(x = value, fill=variable), alpha=0.6) +
  facet_grid(gss_id_collection_code.ds~.) +
  scale_x_log10()

jpeg('~/Network-Shares/DataLabNas/MAA/MAA2016-15 Supporting the Social Investment Unit/outcomes_framework/sh3/output/weight.png',width=1000,height=900)
p
dev.off()



compare_data <- compare_data %>% dplyr::mutate(mult_change = link_FinalWgt / gss_pq_person_FinalWgt)
compare_data %>%
  group_by(gss_id_collection_code.ds) %>%
  summarise(link_prob = sum(!is.na(link_FinalWgt)) / length(gss_pq_person_FinalWgt),
            link_mult_factor = 1/link_prob,
            mean_change = mean(mult_change, na.rm=TRUE))

jpeg('~/Network-Shares/DataLabNas/MAA/MAA2016-15 Supporting the Social Investment Unit/outcomes_framework/sh3/output/rateofchange.png',width=1000,height=900)
ggplot(compare_data) + geom_histogram(aes(x = mult_change, fill = gss_id_collection_code.ds)) + facet_grid(gss_id_collection_code.ds ~.)
dev.off()


## Now looking for each variable numerical first

data_levels <- compare_data %>% 
  tidyr::gather(., 'column', 'value', param_list) %>% 
  group_by(column) %>% 
  summarise(levels = n_distinct(value))

# Categorise into data type by number of levels
continuous <- data_levels %>% filter(levels > 40) %>% pull(., column)
ordinal <- data_levels %>% filter(levels > 4 & levels <= 40) %>% pull(., column)
categorical <- data_levels %>% filter(levels >= 2 & levels <= 4) %>% pull(., column)


for(i in seq(1,length(continuous))){
tmp_samples <- make_samples(compare_data[[continuous[i]]] , wgt1=compare_data[[gss_prefix]], wgt2=compare_data[[sia_prefix]])
tmp_samples<-data.frame(tmp_samples)
p <- ggplot(data=tmp_samples) +
  geom_density(aes(x = pop1,fill=gss_prefix,colour=gss_prefix), alpha=0.6) +
    geom_density(aes(x = pop2,fill=sia_prefix, colour=sia_prefix), alpha=0.6) +
    ggtitle(continuous[i])

jpeg(paste('~/Network-Shares/DataLabNas/MAA/MAA2016-15 Supporting the Social Investment Unit/outcomes_framework/sh3/output/',continuous[i],'.png'),width=1000,height=900)
print(p)
dev.off()
  }


for(i in seq(1,length(ordinal))){
  tmp_samples <- make_samples(compare_data[[ordinal[i]]] , wgt1=compare_data[[gss_prefix]], wgt2=compare_data[[sia_prefix]])
  tmp_samples<-data.frame(tmp_samples)
  p <- ggplot(data=tmp_samples) +
    geom_histogram(aes(x = pop1,fill=gss_prefix,colour=gss_prefix), alpha=0.6) +
    geom_histogram(aes(x = pop2,fill=sia_prefix, colour=sia_prefix), alpha=0.6) +
    ggtitle(ordinal[i])
  
  jpeg(paste('~/Network-Shares/DataLabNas/MAA/MAA2016-15 Supporting the Social Investment Unit/outcomes_framework/sh3/output/',ordinal[i],'.png'),width=1000,height=900)
  print(p)
  dev.off()
}

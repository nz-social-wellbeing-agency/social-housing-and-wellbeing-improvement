# ================================================================================================ #
# Description: Creating descriptive statistics with new GSS weights.
#
# Input: 
# [DL-MAA2016-15].of_gss_calibrated_weights
# [DL-MAA2016-15].of_gss_ind_variables
#
# Output: 
#
# Author: C MacCormick and V Benny
#
# Dependencies:
#
# Notes:
#
# History (reverse order): 
# 4 October 2017 CM v1
#16 October 2018 WJ Added in variables of interest regarding family type and tax credit measures
# ================================================================================================ #

# rm(list = ls())
# List of variables on which stats are required


measure_list <- c("health_nzsf12_physical","health_nzsf12_mental","life_satisfaction_ind","total_equi_income_net_b4","total_equi_income_net_af","total_income_monthly_before_hld")

##,econ_material_well_being_idx2

####################################################################################################
# Functions to generate the statistics


# Bivariate stats generation
bivariate_means_sh <- function(measure){
  
  # enquo_input <- enquo(inputVar)
  print(measure)
  
  tmp1 <- gss_person_final_svy %>%
    mutate(var = factor(housing_groups_pq)
           ,var2 = factor(counter)
           ,meas = as.numeric(get(measure[1]))) %>%
    filter(!is.na(var) & !is.na(var2)) %>%
    group_by(var, var2) %>%
    summarise(
      wtmean = survey_mean(meas, na.rm = TRUE, vartype = c("ci", "se"))
      # ,wttotal = survey_total(na.rm = TRUE, vartype = c("ci", "se"))
      # ,unwttotal = unweighted(n())
    ) %>%
    mutate(var_name = "housing_groups_pq"
           ,var_name2 = "mean"
           ,measure_name = measure[1]
    )
  
  tmp2 <- gss_person_final_svy %>%
    mutate(var = factor(housing_groups_pq)
           ,var2 = factor(counter)
           ,meas = as.numeric(get(measure[1]))) %>%
    filter(!is.na(var) & !is.na(var2)) %>%
    group_by(var, var2) %>%
    summarise(
      unwttotal = unweighted(n())
      # ,wttotal = survey_total(na.rm = TRUE, vartype = c("ci", "se"))
      # ,unwttotal = unweighted(n())
    ) %>%
    mutate(var_name = "housing_groups_pq"
           ,var_name2 = "mean"
           ,measure_name = measure[1]
    )
  
  tmp <- left_join(tmp1, tmp2, by = c("var", "var_name", "var2", "var_name2", "measure_name"))
  return(tmp)
  
}


univariate_descr <- function(measure){
  
  
  # if weighted is TRUE, then run the below aggregation
    tmp <- gss_person_final_svy %>%
      mutate(var = factor(counter), meas = as.numeric(get(measure[1]))) %>%
      filter(!is.na(var)) %>% # Filter NAs
      group_by(var) %>%
      summarise(wtmean = survey_mean(meas,na.rm = TRUE, vartype = c("ci", "se"))
                ,unwttotal = unweighted(n())
      )  %>%
  mutate(var_name = "mean"
         ,measure_name = measure[1]
  )
                
  
  return(tmp)
  
}


####################################################################################################

# Declare lists and an iterator for storing intermediate datasets from each wave
univariate_list <- list()
bivariate_list <- c()
listcounter <- 1

for(wave in c("GSS2016","GSS2014","GSS2012","GSS2010","GSS2008") ) {
  
  # Create the survey object for generating stats
  gss_person_final_svy <- svrepdesign(id = ~1
                                      , weights = ~link_FinalWgt
                                      , repweights = "link_FinalWgt[0-9]"
                                      # , repweights = "pq_person_FinalWgt[0-9]"
                                      , data = gss_person_final %>% filter(gss_id_collection_code == wave)
                                      , type = "JK1"
                                      , scale = 0.99
  ) %>%
    as_survey_rep()

  measurevars <- measure_list
  univariate_tbl <- data.frame()
  
  for(i in 1:length(measure_list)){
    print(measurevars[[i]])
    univariate_tbl <- rbind(univariate_tbl, univariate_descr(measurevars[[i]]))
  }
  
  # Save the summary table for later use
  univariate_list[[listcounter]] <- univariate_tbl

  ######## Bivariate Statistics at housing groups level- Weighted ########
  bivariate_tbl <- data.frame()
      for(k in 1:length(measurevars)){
          bivariate_tbl <- rbind(bivariate_tbl, bivariate_means_sh(measurevars[[k]] ))
      }
    

  # Save the summary table for later use
  bivariate_list[[listcounter]] <- bivariate_tbl
  
  # Increment counter for list
  listcounter <- listcounter + 1
  
}

names(bivariate_list) <- c("GSS2016","GSS2014","GSS2012","GSS2010","GSS2008")
names(univariate_list) <- c("GSS2016","GSS2014","GSS2012","GSS2010","GSS2008")

# Calculate aggregate statistics for bivariate case
bivar_agg <- bivariate_list$GSS2016 %>% dplyr::select(var, var_name, measure_name, var_name2, wtmean, wtmean_se, wtmean_low, wtmean_upp, unwttotal) %>%  
  dplyr::rename(wtmean16=wtmean,wtmean_se16=wtmean_se, wtmean_low16=wtmean_low, wtmean_up16=wtmean_upp,unwttotal16=unwttotal)%>%  
  full_join(bivariate_list$GSS2014 %>% dplyr::select(var, var_name, measure_name, var_name2, wtmean, wtmean_se, wtmean_low, wtmean_upp, unwttotal)%>%
              dplyr::rename(wtmean14=wtmean,wtmean_se14=wtmean_se, wtmean_low14=wtmean_low, wtmean_up14=wtmean_upp,unwttotal14=unwttotal) , 
            by = c("var", "var_name", "measure_name", "var_name2")) %>%
  full_join(bivariate_list$GSS2012 %>% dplyr::select(var, var_name, measure_name, var_name2, wtmean, wtmean_se, wtmean_low, wtmean_upp, unwttotal)%>%
              dplyr::rename(wtmean12=wtmean,wtmean_se12=wtmean_se, wtmean_low12=wtmean_low, wtmean_up12=wtmean_upp,unwttotal12=unwttotal) , 
            by = c("var", "var_name", "measure_name", "var_name2")) %>%
  full_join(bivariate_list$GSS2010 %>% dplyr::select(var, var_name, measure_name, var_name2, wtmean, wtmean_se, wtmean_low, wtmean_upp, unwttotal)%>%
              dplyr::rename(wtmean10=wtmean,wtmean_se10=wtmean_se, wtmean_low10=wtmean_low, wtmean_up10=wtmean_upp,unwttotal10=unwttotal) , 
            by = c("var", "var_name", "measure_name", "var_name2")) %>%
  full_join(bivariate_list$GSS2008 %>% dplyr::select(var, var_name, measure_name, var_name2, wtmean, wtmean_se, wtmean_low, wtmean_upp, unwttotal)%>%
              dplyr::rename(wtmean08=wtmean,wtmean_se08=wtmean_se, wtmean_low08=wtmean_low, wtmean_up08=wtmean_upp,unwttotal08=unwttotal) , 
            by = c("var", "var_name", "measure_name", "var_name2"))%>%
  mutate(overallse = sqrt( ( ifelse(is.na(wtmean_se16), 0, wtmean_se16^2) + 
                               ifelse(is.na(wtmean_se14), 0, wtmean_se14^2) + 
                               ifelse(is.na(wtmean_se12), 0, wtmean_se12^2) + 
                               ifelse(is.na(wtmean_se10), 0, wtmean_se10^2)  + 
                               ifelse(is.na(wtmean_se08), 0, wtmean_se08^2) ) /
                             (ifelse(is.na(wtmean_se16), 0, 1) + 
                                ifelse(is.na(wtmean_se14), 0, 1) + 
                                ifelse(is.na(wtmean_se12), 0, 1) + 
                                ifelse(is.na(wtmean_se10), 0, 1) + 
                                ifelse(is.na(wtmean_se08), 0, 1))^2 
  )
  )

bivar_agg$overallmean <-  rowMeans(bivar_agg[,grepl("mean[0-9]", names(bivar_agg))], na.rm = TRUE)
bivar_agg$overall_low <- bivar_agg$overallmean - 1.96*bivar_agg$overallse
bivar_agg$overall_upp <- bivar_agg$overallmean + 1.96*bivar_agg$overallse
write.xlsx(as.data.frame(bivar_agg), file = "../output/bivariate_agg_means.xlsx", sheetName = "Data", row.names = FALSE)



# Calculate aggregate statistics for bivariate case
univar_agg <- univariate_list$GSS2016 %>% dplyr::select(var, var_name, measure_name, wtmean, wtmean_se, wtmean_low, wtmean_upp, unwttotal) %>%  
  dplyr::rename(wtmean16=wtmean,wtmean_se16=wtmean_se, wtmean_low16=wtmean_low, wtmean_up16=wtmean_upp,unwttotal16=unwttotal)%>%  
  full_join(univariate_list$GSS2014 %>% dplyr::select(var, var_name, measure_name, wtmean, wtmean_se, wtmean_low, wtmean_upp, unwttotal)%>%
              dplyr::rename(wtmean14=wtmean,wtmean_se14=wtmean_se, wtmean_low14=wtmean_low, wtmean_up14=wtmean_upp,unwttotal14=unwttotal) , 
            by = c("var", "var_name", "measure_name")) %>%
  full_join(univariate_list$GSS2012 %>% dplyr::select(var, var_name, measure_name, wtmean, wtmean_se, wtmean_low, wtmean_upp, unwttotal)%>%
              dplyr::rename(wtmean12=wtmean,wtmean_se12=wtmean_se, wtmean_low12=wtmean_low, wtmean_up12=wtmean_upp,unwttotal12=unwttotal) , 
            by = c("var", "var_name", "measure_name")) %>%
  full_join(univariate_list$GSS2010 %>% dplyr::select(var, var_name, measure_name, wtmean, wtmean_se, wtmean_low, wtmean_upp, unwttotal)%>%
              dplyr::rename(wtmean10=wtmean,wtmean_se10=wtmean_se, wtmean_low10=wtmean_low, wtmean_up10=wtmean_upp,unwttotal10=unwttotal) , 
            by = c("var", "var_name", "measure_name")) %>%
  full_join(univariate_list$GSS2008 %>% dplyr::select(var, var_name, measure_name, wtmean, wtmean_se, wtmean_low, wtmean_upp, unwttotal)%>%
              dplyr::rename(wtmean08=wtmean,wtmean_se08=wtmean_se, wtmean_low08=wtmean_low, wtmean_up08=wtmean_upp,unwttotal08=unwttotal) , 
            by = c("var", "var_name", "measure_name"))%>%
  mutate(overallse = sqrt( ( ifelse(is.na(wtmean_se16), 0, wtmean_se16^2) + 
                               ifelse(is.na(wtmean_se14), 0, wtmean_se14^2) + 
                               ifelse(is.na(wtmean_se12), 0, wtmean_se12^2) + 
                               ifelse(is.na(wtmean_se10), 0, wtmean_se10^2)  + 
                               ifelse(is.na(wtmean_se08), 0, wtmean_se08^2) ) /
                             (ifelse(is.na(wtmean_se16), 0, 1) + 
                                ifelse(is.na(wtmean_se14), 0, 1) + 
                                ifelse(is.na(wtmean_se12), 0, 1) + 
                                ifelse(is.na(wtmean_se10), 0, 1) + 
                                ifelse(is.na(wtmean_se08), 0, 1))^2 
  )
  )

univar_agg$overallmean <-  rowMeans(univar_agg[,grepl("mean[0-9]", names(univar_agg))], na.rm = TRUE)
univar_agg$overall_low <- univar_agg$overallmean - 1.96*univar_agg$overallse
univar_agg$overall_upp <- univar_agg$overallmean + 1.96*univar_agg$overallse
write.xlsx(as.data.frame(univar_agg), file = "../output/univar_agg_means2.xlsx", sheetName = "Data", row.names = FALSE)

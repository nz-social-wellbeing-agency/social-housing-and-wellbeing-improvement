/*********************************************************************************************************
DESCRIPTION: 

Creates a final table which has all the variables required for all gss people for the next bit for the GSS descriptive stats

Creates a wrapper of combining for all people who have answered the questionaire -all info from the houshold variables,
from the housing new zealand population and 

INPUT:
sand.of_gss_hh_variables_sh3_admin
sand.of_hnz_gss_population_sh3
sand.of_gss_ind_variables_sh3

OUTPUT:dep
sand.of_gss_ind_variables_sh3_trt = dataset with person variables for GSS

AUTHOR: 
WJ LEE

DEPENDENCIES:
NA

NOTES:   
1. All GSS waves are available only from IDI_Clean_20171027 onwards.


HISTORY: 

***********************************************************************************************************/



proc sql;

	connect to odbc (dsn=&si_idi_dsnname.);

	create table work._temp_of_gss_ind_variables as
	select *
	from connection to odbc(
select z.*

/*Housing groups logic created by conal - these housing groups are using in conals report at table 5 and 7*/
	,case 
					when housing_status in ('OWN', 'TRUST','PRIVATE, TRUST OR BUSINESS-UNKNOWN','UNKNOWN-NO RENT', 'PRIVATE, TRUST OR BUSINESS-NO RENT') 
							and accom_sup_1yr_b4_intvw = 0
						then 'Non Subsidised-Own Home'
					when housing_status in ('OWN', 'TRUST','PRIVATE, TRUST OR BUSINESS-UNKNOWN','UNKNOWN-NO RENT', 'PRIVATE, TRUST OR BUSINESS-NO RENT') 
							and accom_sup_1yr_b4_intvw > 0
						then 'Accommodation Supplement-Own Home'
					when housing_status = 'HOUSING NZ' 
						then 'IRR'
					when housing_status = 'OTHER SOCIAL HOUSING' 
						then 'Other Social Housing'
					when housing_status in ('PRIVATE, TRUST OR BUSINESS-PAY RENT', 'UNKNOWN-PAY RENT') and accom_sup_1yr_b4_intvw = 0
						then 'Non Subsidised-Renting'
					when housing_status in ('PRIVATE, TRUST OR BUSINESS-PAY RENT', 'UNKNOWN-PAY RENT') and accom_sup_1yr_b4_intvw > 0
						then 'Accommodation Supplement-Renting'
					end as housing_groups_pq


/*Self reported income that has been equivilized using OECD methodology and been CPI adjusted */
/*					Median income*/
		,case when gss_id_collection_code='GSS2008' then (hh_gss_income_median / hh_size_eq) * (1228.0/1063.5)
					when gss_id_collection_code='GSS2010' then (hh_gss_income_median / hh_size_eq)* (1228.0/1111.0)
					when gss_id_collection_code='GSS2012' then (hh_gss_income_median / hh_size_eq) * (1228.0/1168.0)
					when gss_id_collection_code='GSS2014' then (hh_gss_income_median / hh_size_eq) * (1228.0/1195.75)
					when gss_id_collection_code='GSS2016' then (hh_gss_income_median / hh_size_eq)* (1228.0/1207.0)
		end as hh_gss_equi_income_median_inf,
/*					Lower bound income based on income bands*/
		case when gss_id_collection_code='GSS2008' then (hh_gss_income_lower / hh_size_eq) * (1228.0/1063.5)
					when gss_id_collection_code='GSS2010' then (hh_gss_income_lower / hh_size_eq)* (1228.0/1111.0)
					when gss_id_collection_code='GSS2012' then (hh_gss_income_lower / hh_size_eq) * (1228.0/1168.0)
					when gss_id_collection_code='GSS2014' then (hh_gss_income_lower / hh_size_eq) * (1228.0/1195.75)
					when gss_id_collection_code='GSS2016' then (hh_gss_income_lower / hh_size_eq)* (1228.0/1207.0)
		end as hh_gss_equi_income_median_lw_inf,
/*					Upper bound income based on income bands*/
			case when gss_id_collection_code='GSS2008' then (hh_gss_income_upper / hh_size_eq) * (1228.0/1063.5)
					when gss_id_collection_code='GSS2010' then (hh_gss_income_upper / hh_size_eq)* (1228.0/1111.0)
					when gss_id_collection_code='GSS2012' then (hh_gss_income_upper / hh_size_eq) * (1228.0/1168.0)
					when gss_id_collection_code='GSS2014' then (hh_gss_income_upper / hh_size_eq) * (1228.0/1195.75)
					when gss_id_collection_code='GSS2016' then (hh_gss_income_upper / hh_size_eq)* (1228.0/1207.0)
				end as hh_gss_equi_income_median_up_inf

from (	
	select 
					p.snz_uid
					,p.snz_spine_ind
					,p.snz_gss_hhld_uid
					,p.gss_id_collection_code
					,p.gss_pq_interview_date
					,gss_hq_interview_date
					,gss_hq_sex_dev
					,gss_hq_birth_month_nbr
					,gss_hq_birth_year_nbr
					,gss_pq_dvage_code
					,P_ASIAN
					,P_EURO
					,P_MAORI
					,P_MELAA
					,P_OTHER
					,P_PACIFIC
					,maori_desc
					,case when nz_born in (77, 88, 99) then NULL else nz_born end as nz_born
					,nz_arrive_year
					,nz_arrive_month
					,gss_hq_regcouncil_dev
					,married
					,gss_pq_HH_comp_code
					,gss_hq_dep_child_dev
					,fam_type
					,family_nuclei_ct
					,nonfamily_nuclei_ct
					,family_size_adult
					,family_size_child
					,family_size_depchild
					,family_size_indepchild
					,gss_hq_house_trust
					,gss_hq_house_own
					,gss_hq_house_pay_mort_code
					,gss_hq_house_pay_rent_code
					,gss_hq_house_who_owns_code
					,case when gss_pq_HH_tenure_code in (77, 88, 99) then NULL else gss_pq_HH_tenure_code end as gss_pq_HH_tenure_code
					,case when gss_pq_HH_crowd_code in (77, 88, 99) then NULL else gss_pq_HH_crowd_code end as gss_pq_HH_crowd_code
					,gss_pq_house_mold_code
					,gss_pq_house_cold_code
					,case when gss_pq_house_condition_code20146 in (77, 88, 99) then NULL else gss_pq_house_condition_code20146 end as gss_pq_house_condition_code20146
					,gss_pq_house_condition_code
					,case when housing_satisfaction in (77, 88, 99) then NULL else housing_satisfaction end as housing_satisfaction
					,gss_pq_prob_hood_noisy_ind
					,case when gss_pq_safe_night_pub_trans_code in (77, 88, 99) then NULL else gss_pq_safe_night_pub_trans_code end as gss_pq_safe_night_pub_trans_code
					,case when gss_pq_safe_night_hood_code in (77, 88, 99) then NULL else gss_pq_safe_night_hood_code end as gss_pq_safe_night_hood_code
					,case when gss_pq_safe_day_hood_code in (77, 88, 99) then NULL else gss_pq_safe_day_hood_code end as gss_pq_safe_day_hood_code
					,case when gss_pq_crimes_against_ind in (77, 88, 99) then NULL else gss_pq_crimes_against_ind end as gss_pq_crimes_against_ind
					,gss_hq_household_inc1_dev
					,case when gss_hq_household_inc1_dev = '01' then '-Loss'
					when gss_hq_household_inc1_dev = '02' then '0'
					when gss_hq_household_inc1_dev = '03' then '1-5000'
					when gss_hq_household_inc1_dev = '04' then '5001-10000'
					when gss_hq_household_inc1_dev = '05' then '10001-15000'
					when gss_hq_household_inc1_dev = '06' then '15001-20000'
					when gss_hq_household_inc1_dev = '07' then '20001-25000'
					when gss_hq_household_inc1_dev = '08' then '25001-30000'
					when gss_hq_household_inc1_dev = '09' then '30001-35000'
					when gss_hq_household_inc1_dev = '10' then '35001-40000'
					when gss_hq_household_inc1_dev in ('11', '12', '13') then '40001-70000'
					when gss_hq_household_inc1_dev = '14' then '70001-100000'
					when gss_hq_household_inc1_dev = '15' then '100001-150000'
					when gss_hq_household_inc1_dev = '16' then '150001-Inf'
				end as hh_gss_income

				,case when gss_hq_household_inc1_dev = '01' then 0
					when gss_hq_household_inc1_dev = '02' then 0
					when gss_hq_household_inc1_dev = '03' then 2500
					when gss_hq_household_inc1_dev = '04' then 7500
					when gss_hq_household_inc1_dev = '05' then 12500
					when gss_hq_household_inc1_dev = '06' then 17500
					when gss_hq_household_inc1_dev = '07' then 22500
					when gss_hq_household_inc1_dev = '08' then 27500
					when gss_hq_household_inc1_dev = '09' then 32500
					when gss_hq_household_inc1_dev = '10' then 37500
					when gss_hq_household_inc1_dev in ('11', '12', '13') then 55000
					when gss_hq_household_inc1_dev = '14' then 85000
					when gss_hq_household_inc1_dev = '15' then 125000
					when gss_hq_household_inc1_dev = '16' then 175000
				end as hh_gss_income_median

				,case when gss_hq_household_inc1_dev = '01' then 0
					when gss_hq_household_inc1_dev = '02' then 0
					when gss_hq_household_inc1_dev = '03' then 1
					when gss_hq_household_inc1_dev = '04' then 5001
					when gss_hq_household_inc1_dev = '05' then 10001
					when gss_hq_household_inc1_dev = '06' then 15001
					when gss_hq_household_inc1_dev = '07' then 20001
					when gss_hq_household_inc1_dev = '08' then 25001
					when gss_hq_household_inc1_dev = '09' then 30001
					when gss_hq_household_inc1_dev = '10' then 35001
					when gss_hq_household_inc1_dev in ('11', '12', '13') then 40001
					when gss_hq_household_inc1_dev = '14' then 70001
					when gss_hq_household_inc1_dev = '15' then 100001
					when gss_hq_household_inc1_dev = '16' then 150001
				end as hh_gss_income_lower

				,case when gss_hq_household_inc1_dev = '01' then 0
					when gss_hq_household_inc1_dev = '02' then 0
					when gss_hq_household_inc1_dev = '03' then 5000
					when gss_hq_household_inc1_dev = '04' then 10000
					when gss_hq_household_inc1_dev = '05' then 15000
					when gss_hq_household_inc1_dev = '06' then 20000
					when gss_hq_household_inc1_dev = '07' then 25000
					when gss_hq_household_inc1_dev = '08' then 30000
					when gss_hq_household_inc1_dev = '09' then 35000
					when gss_hq_household_inc1_dev = '10' then 40000
					when gss_hq_household_inc1_dev in ('11', '12', '13') then 70000
					when gss_hq_household_inc1_dev = '14' then 100000
					when gss_hq_household_inc1_dev = '15' then 150000
					when gss_hq_household_inc1_dev = '16' then 200000
					end as hh_gss_income_upper
					,case when gss_pq_lfs_dev in (77, 88, 99) then NULL else gss_pq_lfs_dev end as gss_pq_lfs_dev
					,gss_unemp_jobseek
					,gss_sickness
					,gss_invalid_support
					,gss_soleprnt_domestic
					,gss_oth_ben
					,gss_no_income
					,gss_pq_material_wellbeing_code
					,gss_pq_ELSIDV1
					,case when gss_pq_cost_down_vege_code in (77, 88, 99) then NULL else gss_pq_cost_down_vege_code end as gss_pq_cost_down_vege_code
					,case when gss_pq_cost_down_dr_code in (77, 88, 99) then NULL else gss_pq_cost_down_dr_code end as gss_pq_cost_down_dr_code
					,case when gss_pq_cost_down_shop_code in (77, 88, 99) then NULL else gss_pq_cost_down_shop_code end as gss_pq_cost_down_shop_code
					,case when gss_pq_cost_down_hobby_code in (77, 88, 99) then NULL else gss_pq_cost_down_hobby_code end as gss_pq_cost_down_hobby_code
					,case when gss_pq_cost_down_cold_code in (77, 88, 99) then NULL else gss_pq_cost_down_cold_code end as gss_pq_cost_down_cold_code
					,case when gss_pq_cost_down_appliance_code in (77, 88, 99) then NULL else gss_pq_cost_down_appliance_code end as gss_pq_cost_down_appliance_code
					,case when gss_pq_buy_shoes_limit_code in (77, 88, 99) then NULL else gss_pq_buy_shoes_limit_code end as gss_pq_buy_shoes_limit_code
					,case when gss_pq_item_300_limit_code in (77, 88, 99) then NULL else gss_pq_item_300_limit_code end as gss_pq_item_300_limit_code
					,case when gss_pq_not_pay_bills_time_code in (77, 88, 99) then NULL else gss_pq_not_pay_bills_time_code end as gss_pq_not_pay_bills_time_code
					,case when gss_pq_enough_inc_code in (77, 88, 99) then NULL else gss_pq_enough_inc_code end as gss_pq_enough_inc_code
					,work_hrs
					,work_jobs_no
					,case when work_satisfaction in (77, 88, 99) then NULL else work_satisfaction end as work_satisfaction
					,case when work_ft_pt in (77, 88, 99) then NULL else work_ft_pt end as work_ft_pt
					,case when work_now_ind in (77, 88, 99) then NULL else work_now_ind end as work_now_ind
					,work_start_ind
					,work_look_ind
					,work_could_start
					,case when school_highest in (77, 88, 99) then NULL else school_highest end as school_highest
					,gss_pq_highest_qual_dev
					,qual_yr
					,health_status
					,health_limit_activ
					,health_limit_stair
					,case when health_accomplish_phys in (77, 88, 99) then NULL else health_accomplish_phys end as health_accomplish_phys
					,case when health_work_phys in (77, 88, 99) then NULL else health_work_phys end as health_work_phys
					,case when health_accomplish_emo in (77, 88, 99) then NULL else health_accomplish_emo end as health_accomplish_emo
					,case when health_work_emo in (77, 88, 99) then NULL else health_work_emo end as health_work_emo
					,case when health_pain in (77, 88, 99) then NULL else health_pain end as health_pain
					,case when health_calm in (77, 88, 99) then NULL else health_calm end as health_calm
					,case when health_energy in (77, 88, 99) then NULL else health_energy end as health_energy
					,case when health_depressed in (77, 88, 99) then NULL else health_depressed end as health_depressed
					,case when health_social in (77, 88, 99) then NULL else health_social end as health_social
					,case when gss_pq_ment_health_code in (777) then NULL else gss_pq_ment_health_code end as gss_pq_ment_health_code
					,case when gss_pq_phys_health_code in (777) then NULL else gss_pq_phys_health_code end as gss_pq_phys_health_code
					,case when gss_pq_cult_identity_code in (77, 88, 99) then NULL else gss_pq_cult_identity_code end as gss_pq_cult_identity_code
					,case when belong in (77, 88, 99) then NULL else belong end as belong
					,case when belong_2016 in (77, 88, 99) then NULL else belong_2016 end as belong_2016
					,case when discrim_status in (77, 88, 99) then NULL else discrim_status end as discrim_status
					,discrim_shop
					,discrim_work
					,discrim_public
					,discrim_school
					,discrim_job
					,discrim_police
					,discrim_medic
					,discrim_other
					,discrim_dk
					,discrim_nr
					,gss_pq_discrim_rent_ind
					,case when leisure_time in (77, 88, 99) then NULL else leisure_time end as leisure_time
					,case when gss_pq_voting in (77, 88, 99) then NULL else gss_pq_voting end as gss_pq_voting
					,case when volunteer in (77, 88, 99) then NULL else volunteer end as volunteer
					,case when trust_police in (77, 88, 99) then NULL else trust_police end as trust_police
					,case when trust_education in (77, 88, 99) then NULL else trust_education end as trust_education
					,case when trust_media in (77, 88, 99) then NULL else trust_media end as trust_media
					,case when trust_courts in (77, 88, 99) then NULL else trust_courts end as trust_courts
					,case when trust_parliament in (77, 88, 99) then NULL else trust_parliament end as trust_parliament
					,case when trust_health in (77, 88, 99) then NULL else trust_health end as trust_health
					,case when generalised_trust in (77, 88, 99) then NULL else generalised_trust end as generalised_trust
					,case when gss_pq_time_lonely_code in (77, 88, 99) then NULL else gss_pq_time_lonely_code end as gss_pq_time_lonely_code
					,case when env_water_bodies in (77, 88, 99) then NULL else env_water_bodies end as env_water_bodies
					,case when env_access_bush in (77, 88, 99) then NULL else env_access_bush end as env_access_bush
					,case when env_state_bush in (77, 88, 99) then NULL else env_state_bush end as env_state_bush
					,case when purpose_sense in (77, 88, 99) then NULL else purpose_sense end as purpose_sense
					,case when gss_pq_feel_life_code in (77, 88, 99) then NULL else gss_pq_feel_life_code end as gss_pq_feel_life_code
					,house_brm
					,inc_self  
					,gss_pq_person_SeInWgt
					,gss_pq_person_FinalWgt
					,gss_pq_person_FinalWgt1
					,gss_pq_person_FinalWgt2
					,gss_pq_person_FinalWgt3
					,gss_pq_person_FinalWgt4
					,gss_pq_person_FinalWgt5
					,gss_pq_person_FinalWgt6
					,gss_pq_person_FinalWgt7
					,gss_pq_person_FinalWgt8
					,gss_pq_person_FinalWgt9
					,gss_pq_person_FinalWgt10
					,gss_pq_person_FinalWgt11
					,gss_pq_person_FinalWgt12
					,gss_pq_person_FinalWgt13
					,gss_pq_person_FinalWgt14
					,gss_pq_person_FinalWgt15
					,gss_pq_person_FinalWgt16
					,gss_pq_person_FinalWgt17
					,gss_pq_person_FinalWgt18
					,gss_pq_person_FinalWgt19
					,gss_pq_person_FinalWgt20
					,gss_pq_person_FinalWgt21
					,gss_pq_person_FinalWgt22
					,gss_pq_person_FinalWgt23
					,gss_pq_person_FinalWgt24
					,gss_pq_person_FinalWgt25
					,gss_pq_person_FinalWgt26
					,gss_pq_person_FinalWgt27
					,gss_pq_person_FinalWgt28
					,gss_pq_person_FinalWgt29
					,gss_pq_person_FinalWgt30
					,gss_pq_person_FinalWgt31
					,gss_pq_person_FinalWgt32
					,gss_pq_person_FinalWgt33
					,gss_pq_person_FinalWgt34
					,gss_pq_person_FinalWgt35
					,gss_pq_person_FinalWgt36
					,gss_pq_person_FinalWgt37
					,gss_pq_person_FinalWgt38
					,gss_pq_person_FinalWgt39
					,gss_pq_person_FinalWgt40
					,gss_pq_person_FinalWgt41
					,gss_pq_person_FinalWgt42
					,gss_pq_person_FinalWgt43
					,gss_pq_person_FinalWgt44
					,gss_pq_person_FinalWgt45
					,gss_pq_person_FinalWgt46
					,gss_pq_person_FinalWgt47
					,gss_pq_person_FinalWgt48
					,gss_pq_person_FinalWgt49
					,gss_pq_person_FinalWgt50
					,gss_pq_person_FinalWgt51
					,gss_pq_person_FinalWgt52
					,gss_pq_person_FinalWgt53
					,gss_pq_person_FinalWgt54
					,gss_pq_person_FinalWgt55
					,gss_pq_person_FinalWgt56
					,gss_pq_person_FinalWgt57
					,gss_pq_person_FinalWgt58
					,gss_pq_person_FinalWgt59
					,gss_pq_person_FinalWgt60
					,gss_pq_person_FinalWgt61
					,gss_pq_person_FinalWgt62
					,gss_pq_person_FinalWgt63
					,gss_pq_person_FinalWgt64
					,gss_pq_person_FinalWgt65
					,gss_pq_person_FinalWgt66
					,gss_pq_person_FinalWgt67
					,gss_pq_person_FinalWgt68
					,gss_pq_person_FinalWgt69
					,gss_pq_person_FinalWgt70
					,gss_pq_person_FinalWgt71
					,gss_pq_person_FinalWgt72
					,gss_pq_person_FinalWgt73
					,gss_pq_person_FinalWgt74
					,gss_pq_person_FinalWgt75
					,gss_pq_person_FinalWgt76
					,gss_pq_person_FinalWgt77
					,gss_pq_person_FinalWgt78
					,gss_pq_person_FinalWgt79
					,gss_pq_person_FinalWgt80
					,gss_pq_person_FinalWgt81
					,gss_pq_person_FinalWgt82
					,gss_pq_person_FinalWgt83
					,gss_pq_person_FinalWgt84
					,gss_pq_person_FinalWgt85
					,gss_pq_person_FinalWgt86
					,gss_pq_person_FinalWgt87
					,gss_pq_person_FinalWgt88
					,gss_pq_person_FinalWgt89
					,gss_pq_person_FinalWgt90
					,gss_pq_person_FinalWgt91
					,gss_pq_person_FinalWgt92
					,gss_pq_person_FinalWgt93
					,gss_pq_person_FinalWgt94
					,gss_pq_person_FinalWgt95
					,gss_pq_person_FinalWgt96
					,gss_pq_person_FinalWgt97
					,gss_pq_person_FinalWgt98
					,gss_pq_person_FinalWgt99
					,gss_pq_person_FinalWgt100
					/* "11-Very Safe" and "12-Safe" are treated as being "Safe" and everyone else is in the "No/Unknown" category.
					It is to be noted that this is a 4-point scale in 2010 & 2012, but 5-point in 2008 and 2014. In case of 
					the 5-point scales, the "13-Neither Safe nor Unsafe" is counted with "No/Unknown" here. Also note that 
					gss_pq_safe_day_hood_code variable does not exist for 2014.	*/
					,case when gss_pq_safe_night_pub_trans_code in ('11', '12') then 1 else 0 end as pub_trpt_safety_ind
					,case when gss_pq_safe_night_hood_code in ('11', '12') then 1 else 0 end as safety_ind
					,case when gss_pq_safe_day_hood_code in ('11', '12') then 1 else 0 end as safety_day_ind

					/* "1-Two or more bedrooms needed" & "2-One bedroom needed" are combined into 1-Crowded. "77-Not stated" is included with 
						Uncrowded/Unknown */
					,case when gss_pq_HH_crowd_code in (1, 2) then 1 else 0 end as house_crowding_ind

					/* For crimes experience, "1-Yes" coded as 1. Everything else decoded as No/Unknown. */
					,case when gss_pq_crimes_against_ind = 1 then 1 else 0 end as crime_exp_ind
					,case when gss_pq_HH_crowd_code in (1, 2) then 1 else 0 end + 
							gss_pq_house_mold_code + 
							gss_pq_house_cold_code as ct_house_pblms
					
					,case when gss_pq_lfs_dev = '01' then 'Employed'
						when gss_pq_lfs_dev = '02' then 'Unemployed'
						when gss_pq_lfs_dev = '03' then 'Not_in_Labour_Force'
						else NULL end as lbr_force_status

					/* "11-All of the time", "12-Most of the time", & "13-Some of the time" has been coded as "Lonely"
						The scales for 2014 were flipped in the original data, but the inner query standardizes it
						to the same scales as 2008, 2010 and 2014.				*/
					,case when gss_pq_time_lonely_code in ('14', '15') then 0 
						when gss_pq_time_lonely_code in ('88', '99') or gss_pq_time_lonely_code is null then NULL
						else 1 end as time_lonely_ind 

					/* No voting indicator exists for 2014, and is all decoded as -1, 1-"Yes" and "2-No". */
					, case when gss_pq_voting = 1 then 1 
						when gss_pq_voting = 2 then 0
						else NULL end as voting_ind

					/* Life satisfaction is a 10-point scale for 2014, and 5 point scales for all previous waves.
						For 2014, the range is 00 to 10 and for previous waves it is flipped from 15 to 11.
						Here we convert the 10-point scales into a 5-point scale and make it numeric in the range 1 to 5.*/
					,case when gss_pq_feel_life_code in ('00', '01', '15') then 1
						when gss_pq_feel_life_code in ('02','03','04','14') then 2
						when gss_pq_feel_life_code in ('05','13') then 3
						when gss_pq_feel_life_code in ('06','07','08','12') then 4
						when gss_pq_feel_life_code in ('09','10','11') then 5
					else NULL end as life_satisfaction_ind

						/* David Rea only wants owned, rented from private landlord, renting from public landlord, unknown; */
						,case when gss_pq_hh_tenure_code ='77' or gss_pq_hh_tenure_code IS NULL then 'UNKNOWN'
								when gss_pq_hh_tenure_code in ('10','11','12','30','31','32') then 'OWNED(or living rent free)'
								when gss_pq_hh_tenure_code in ('20','21','22') then
											case when gss_hq_house_who_owns_code = '11' then 'RENTING FROM PRIVATE LANDLORD'
												when gss_hq_house_who_owns_code in ('12','13', '14') then 'RENTING FROM PUBLIC LANDLORD'
												when gss_hq_house_who_owns_code in ('88', '99') then 'UNKNOWN'
												when gss_hq_house_who_owns_code is NULL then 'UNKNOWN'
											end
						end as housing_status_simplified


					,case when gss_hq_house_trust='99' then 'UNKNOWN'
							when gss_hq_house_trust in ('88', '02') then 
								case when gss_hq_house_own = '01' then 'OWN'
									when gss_hq_house_own = '99' then 'UNKNOWN'
									when gss_hq_house_own in ('02','88') then 
										case when gss_hq_house_who_owns_code = '11' and gss_hq_house_pay_rent_code = '01' then 'PRIVATE, TRUST OR BUSINESS-PAY RENT'
											when gss_hq_house_who_owns_code = '11' and gss_hq_house_pay_rent_code = '02' then 'PRIVATE, TRUST OR BUSINESS-NO RENT'
											when gss_hq_house_who_owns_code = '11' and gss_hq_house_pay_rent_code in ('88', '99') then 'PRIVATE, TRUST OR BUSINESS-UNKNOWN'
											when gss_hq_house_who_owns_code in ('12', '14') then 'OTHER SOCIAL HOUSING'
											when gss_hq_house_who_owns_code ='13' then 'HOUSING NZ'
											when gss_hq_house_who_owns_code in ('88', '99') and gss_hq_house_pay_rent_code = '01' then 'UNKNOWN-PAY RENT'
											when gss_hq_house_who_owns_code in ('88', '99') and gss_hq_house_pay_rent_code = '02' then 'UNKNOWN-NO RENT'
											when gss_hq_house_who_owns_code in ('88', '99') and gss_hq_house_pay_rent_code in ('88', '99') then 'UNKNOWN'
										end
								end
							when gss_hq_house_trust = '01' then 'TRUST'
						end as housing_status

					/* For cultural identity, "11- Very Easy" and "12-Easy" is decoded into 1-Easy. Nulls, 88, 99 are kept as Nulls, and others 
					into "0-Not easy"*/
					,case when gss_pq_cult_identity_code in ('11', '12') then 1 
						when gss_pq_cult_identity_code in ('88', '99') then NULL 
						when gss_pq_cult_identity_code is NULL then NULL
						else 0 end as cult_identity_ind
					/* If Housing satisfacion is "11-Very Satisfied" and "12-Satisfied", then we decode "1-Satisfied". If it is "13-Neither Satisfied or Unsatisfied",
					"14-Unsatisfied","15-Very Unsatisfied" then we use 0. Else we use NULL.*/
					,case when housing_satisfaction in (11, 12) then 1 when housing_satisfaction in (13, 14, 15) then 0 else NULL end as housing_sat_ind
					/* Household composition variables*/
/*					,hhcomp.adult_ct*/
/*					,hhcomp.child_ct*/
/*					,hhcomp.hh_size*/
/*					,hhcomp.hh_size_eq*/
					/* OECD Qual Level attempt - see ISCED*/
					,case when gss_pq_highest_qual_dev in (0,1) then 'Less Upper Secondary'
						when gss_pq_highest_qual_dev in (2,3,4,11) then 'Upper Secondary'
						when gss_pq_highest_qual_dev in (5,6,7,8,9,10) then 'Tertiary'
						when gss_pq_highest_qual_dev =NULL then 'NULL'
						else 'Other'
						end as gss_oecd_quals
					/*family type breakdown by type of family */
						,Case when fam_type in ('11') THEN 'Couple without Children'		
								when fam_type in ('21') THEN 'Couple with dependent child(ren) under 18 only'
								when fam_type in ('22') THEN 'Couple with adult child(ren) only'
								when fam_type in ('23') THEN 'Couple with adult child(ren) and dependent child(ren) under 18'
								when fam_type in ('31') THEN 'One parent with dependent child(ren) under 18 only'
								when fam_type in ('32') THEN 'One parent with adult child(ren) only'
								when fam_type in ('33') THEN 'One parent with adult child(ren) and dependent child(ren) under 18'
								when fam_type in ('41') THEN 'Not in a family nucleus'
							else 'Family type unidentifiable' end as fam_typem
	
				,case when work_hrs = 777 then NULL else work_hrs end as work_hrs_clean
				,case when purpose_sense between 0 and 10 then purpose_sense else NULL end as purpose_sense_clean
				,case when generalised_trust between 0 and 10 then generalised_trust else NULL end as generalised_trust_clean
				,case when volunteer = 1 then volunteer else 0 end as volunteer_clean/*1-Yes, 0- Don't know or Refused or NULL */
				,case when trust_police between 0 and 10 then trust_police else NULL end as trust_police_clean
				,case when trust_education between 0 and 10 then trust_education else NULL end as trust_education_clean
				,case when trust_media between 0 and 10 then trust_media else NULL end as trust_media_clean
				,case when trust_courts between 0 and 10 then trust_courts else NULL end as trust_courts_clean
				,case when trust_parliament between 0 and 10 then trust_parliament else NULL end as trust_parliament_clean
				,case when trust_health between 0 and 10 then trust_health else NULL end as trust_health_clean

				/* access to green space */			
				,case when env_access_bush in ('14','15') then 1 
					  when env_access_bush in ('88', '99') then NULL 
					  when env_access_bush is NULL then NULL
						else 0 end as no_access_natural_space

					

				/* free time */
				,case when leisure_time =13 then 1 else 0 end as no_free_time


				/* Binary groupings for Life Satisfaction scores*/
			,case when gss_pq_feel_life_code in ('00', '01', '15','02','03','04','14','05','13') then 0 
				when gss_pq_feel_life_code in ('06','07','08','12','09','10','11') then 1 
				else NULL end as life_satisfaction_bin

				,hhcomp.adult_ct
				,hhcomp.child_ct
				,hhcomp.hh_size
				,hhcomp.hh_size_eq

				,supp_benefit_monthly_before_hld
				,supp_benefit_monthly_after_hld
				,tax_credit_monthly_before_hld
				,tax_credit_monthly_after_hld
				,total_income_monthly_before_hld
				,total_income_monthly_after_hld
				,other_income_monthly_before_hld
				,other_income_monthly_after_hld
				,transfer_income_monthly_before_h
				,transfer_income_monthly_after_h
				,income_monthly_net_before_hld
				,income_monthly_net_after_hld

				,supp_benefit_monthly_before_per
				,supp_benefit_monthly_after_per
				,tax_credit_monthly_before_per
				,tax_credit_monthly_after_per
				,total_income_monthly_before_per
				,total_income_monthly_after_per
				,other_income_monthly_before_per
				,other_income_monthly_after_per
				,transfer_income_monthly_before_p
				,transfer_income_monthly_after_p
				,income_monthly_net_before_per
				,income_monthly_net_after_per
				,accom_sup_1yr_b4_intvw_hld
				,accom_sup_1yr_b4_intvw

				 ,coalesce(hnzmain.treat_control, 'IGNORE') as treat_control_main
				,coalesce(hnzsen.treat_control, 'IGNORE') as treat_control_sen
/*				Admin variables added below*/
/*				,case when accom.snz_gss_hhld_uid is NULL then 0 else 1 end as acc_sup_ind*/

				,income_monthly_net_before_hld / hh_size_eq as hh_equi_income_net_b4
				,income_monthly_net_after_hld / hh_size_eq as hh_equi_income_net_af
				,total_income_monthly_before_hld/ hh_size_eq as total_equi_income_net_b4
				,total_income_monthly_after_hld/ hh_size_eq as total_equi_income_net_af

				,health_nzsf12_physical
				,health_nzsf12_mental

				,hnzmain.hnz_prim
				,hnzmain.snz_uid_hnz_prim


			

			from [IDI_Sandpit].[DL-MAA2016-15].of_gss_ind_variables_sh3 p
			left join (

			select 
						snz_gss_hhld_uid
						,gss_id_collection_code
						,adult_ct
						,child_ct
						,hh_size
						,supp_benefit_monthly_before_hld
						,supp_benefit_monthly_after_hld
						,tax_credit_monthly_before_hld
						,tax_credit_monthly_after_hld
						,total_income_monthly_before_hld
						,total_income_monthly_after_hld
						,other_income_monthly_before_hld
						,other_income_monthly_after_hld
						,transfer_income_monthly_before_h
						,transfer_income_monthly_after_h
						,income_monthly_net_before_hld
						,income_monthly_net_after_hld
						,accom_sup_1yr_b4_intvw_hld
						,case when adult_ct > 0 then 1 else 0 end +
							case when adult_ct > 1 then 0.5 * (adult_ct - 1)  else 0 end +
							(0.3*child_ct) as hh_size_eq

					from (
						select 
							snz_gss_hhld_uid
							,gss_id_collection_code
							,sum(case when gss_hq_age_dev > 18 then 1 else 0 end) as adult_ct
							,sum(case when gss_hq_age_dev <= 18 then 1 else 0 end) as child_ct
							,sum(supp_benefit_monthly_before) as supp_benefit_monthly_before_hld
							,sum(supp_benefit_monthly_after) as supp_benefit_monthly_after_hld
							,sum(tax_credit_monthly_before) as tax_credit_monthly_before_hld
							,sum(tax_credit_monthly_after) as tax_credit_monthly_after_hld
							,sum(total_income_monthly_before) as total_income_monthly_before_hld
							,sum(total_income_monthly_after) as total_income_monthly_after_hld
							,sum(other_income_monthly_before) as other_income_monthly_before_hld
							,sum(other_income_monthly_after) as other_income_monthly_after_hld
							,sum(transfer_income_monthly_before) as transfer_income_monthly_before_h
							,sum(transfer_income_monthly_after) as transfer_income_monthly_after_h
							,sum(income_monthly_net_before) as income_monthly_net_before_hld
							,sum(income_monthly_net_after) as income_monthly_net_after_hld
							,sum(accom_sup_1yr_b4_intvw) as accom_sup_1yr_b4_intvw_hld
							,count(*) as hh_size
						from IDI_Sandpit.[DL-MAA2016-15].[of_gss_hh_variables_sh3_admin]
						group by snz_gss_hhld_uid, gss_id_collection_code
						) hhsize
					) as hhcomp
				on (p.snz_gss_hhld_uid = hhcomp.snz_gss_hhld_uid and p.gss_id_collection_code=hhcomp.gss_id_collection_code)


					left join (
			select 
						snz_uid
						,gss_id_collection_code
						,supp_benefit_monthly_before as supp_benefit_monthly_before_per
						,supp_benefit_monthly_after as supp_benefit_monthly_after_per
						,tax_credit_monthly_before as tax_credit_monthly_before_per
						,tax_credit_monthly_after as tax_credit_monthly_after_per
						,total_income_monthly_before as total_income_monthly_before_per
						,total_income_monthly_after as total_income_monthly_after_per
						,other_income_monthly_before as other_income_monthly_before_per
						,other_income_monthly_after as other_income_monthly_after_per
						,transfer_income_monthly_before as transfer_income_monthly_before_p
						,transfer_income_monthly_after as transfer_income_monthly_after_p
						,income_monthly_net_before as income_monthly_net_before_per
						,income_monthly_net_after as income_monthly_net_after_per
						,accom_sup_1yr_b4_intvw as accom_sup_1yr_b4_intvw 
					from (
						select snz_uid
						,gss_id_collection_code
						,supp_benefit_monthly_before
						,supp_benefit_monthly_after
						,tax_credit_monthly_before
						,tax_credit_monthly_after
						,total_income_monthly_before
						,total_income_monthly_after
						,other_income_monthly_before
						,other_income_monthly_after
						,transfer_income_monthly_before
						,transfer_income_monthly_after
						,income_monthly_net_before
						,income_monthly_net_after
						,accom_sup_1yr_b4_intvw
						from IDI_Sandpit.[DL-MAA2016-15].[of_gss_hh_variables_sh3_admin] ) indv				
					 ) as ppcomp
				on (p.snz_uid = ppcomp.snz_uid and p.gss_id_collection_code=ppcomp.gss_id_collection_code)
		left join ( select * from [IDI_Sandpit].[DL-MAA2016-15].[of_hnz_gss_population_sh3] 
					where 
						hnz_apply_type = 'new application' 
						and months_interview_to_entry between -12 and 15
					) hnzmain on (p.snz_uid = hnzmain.snz_uid_pq and p.gss_id_collection_code = hnzmain.gss_id_collection_code)
		left join ( select * from [IDI_Sandpit].[DL-MAA2016-15].[of_hnz_gss_population_sh3] 
					where 
						hnz_apply_type = 'new application' 
						and months_interview_to_entry between -12 and 12
					) hnzsen on (p.snz_uid = hnzsen.snz_uid_pq and p.gss_id_collection_code = hnzsen.gss_id_collection_code)
) z

);

	
	disconnect from odbc;
quit;


/* Push the cohort into the database*/
%si_write_to_db(si_write_table_in=work._temp_of_gss_ind_variables,
	si_write_table_out=&si_sandpit_libname..of_gss_ind_variables_sh3_trt
	,si_cluster_index_flag=True,si_index_cols=%bquote(&si_id_col.)
	);


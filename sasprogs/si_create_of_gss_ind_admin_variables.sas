/*******************************************************************************************************
TITLE: si_create_of_gss_ind_admin_variables.sas

DESCRIPTION: This script creates all the covariates required for the SH3 project from the admin datasets
	in the IDI.

INPUT: 
	SAS variable "si_pop_table_out" 
	SIAL tables


OUTPUT:
	SQL Table named by "si_pop_table_out" 

KNOWN ISSUES:
	NA

DEPENDENCIES: 
	1. SIAL tables should be available in the project schema.
	2. The SI Data Foundation macros should be available for use.
	3. Marc De Boer's SAS progs for income and tax credit calculation

NOTES: 
	NA


AUTHOR: V Benny

DATE: 03 Apr 2018

HISTORY: 
	03 Apr 2018	VB	First version
	07 Sep 2018	VB	Temporary addition that calculates income from admin datasets using an alternative 
					method developed by Marc DeBoer, to compare against our existing income measure
					and ensure that the differences are minimal.
	04 Dec 2018	BV	QA for SH3 version

*******************************************************************************************************/

/*********************************IRD Admin variables**************************************************/

/* Here we invoke Marc DeBoer's macros to construct a measure of income and tax credits, to function as an
	alternative to the existing measures of income we have. Please note the logic used for this measure of 
income is still pending review */
/*%include "&si_source_path.\sasprogs\si_alt_income_calculation.sas";*/
%include "&si_source_path./sasprogs/si_alt_income_calculation_rev.sas";


/*Temp sandpit tables for data foundation rollup on each wave (for duplicates handling)*/
proc delete data=sand.temp_hh_variables_GSS2016; run;
proc delete data=sand.temp_hh_variables_GSS2014; run;
proc delete data=sand.temp_hh_variables_GSS2012; run;
proc delete data=sand.temp_hh_variables_GSS2010; run;
proc delete data=sand.temp_hh_variables_GSS2008; run;

%macro massive_round_up(wave);


proc sql;
create table sand.temp_hh_variables_&wave. as select * from sand.&si_pop_table_out. where gss_id_collection_code="&wave.";
run;
 
%si_align_sialevents_to_periods(
			si_table_in=[IDI_Sandpit].[&si_proj_schema.].temp_hh_variables_&wave.,
			si_sial_table=[IDI_Sandpit].[&si_proj_schema.].SIAL_MSD_t1_events , 						
			si_id_col = &si_id_col. , 
			si_as_at_date = &si_asat_date. ,
			si_amount_type = NA , 
			si_amount_col = NA , 
			noofperiodsbefore = -1 , 
			noofperiodsafter = 0 , 
			period_duration = Year , 
			si_out_table = MSD_t1_events_aln,
			period_aligned_to_calendar = False, 
			si_pi_type = NA , 
			si_pi_qtr = NA );

%si_create_rollup_vars(
	si_table_in = sand.temp_hh_variables_&wave., 
	si_sial_table = MSD_t1_events_aln,	
	si_out_table = MSD_t1_events_rlp,	
	si_agg_cols= %str(datamart),	
	si_id_col = &si_id_col. ,
	si_as_at_date = &si_asat_date.,
	si_amount_col = NA ,
	cost = False, 
	duration = True, 
	count = False, 
	count_startdate = False, 
	dayssince = False,
	si_rollup_ouput_type = Both
);

	/*********************************COR Admin variables**************************************************/
	/* Time spent incarcerated or in contact with the corrections system. This is defined by the following 
		Corrections codes:
			PRISON	Prison sentenced
			REMAND	Remanded in custody
			HD_SENT	Home detention sentenced
			HD_REL	Released to HD
			ESO	Extended supervision order
			PAROLE	Paroled
			ROC	Released with conditions
			PDC	Post detention conditions
			PERIODIC	Periodic detention
			COM_DET	Community detention
			CW	Community work
			COM_PROG	Community programme
			COM_SERV	Community service
			OTH_COM	Other community
			INT_SUPER	Intensive supervision
			SUPER	Supervision
	*/
%si_align_sialevents_to_periods(
			si_table_in=[IDI_Sandpit].[&si_proj_schema.].temp_hh_variables_&wave.,
			si_sial_table=[IDI_Sandpit].[&si_proj_schema.].SIAL_COR_sentence_events , 						
			si_id_col = &si_id_col. , 
			si_as_at_date = &si_asat_date. ,
			si_amount_type = NA , 
			si_amount_col = NA , 
			noofperiodsbefore = -1 , 
			noofperiodsafter = 0, 
			period_duration = Year , 
			si_out_table = COR_sentence_events_aln,
			period_aligned_to_calendar = False, 
			si_pi_type = NA , 
			si_pi_qtr = NA );

%si_create_rollup_vars(
	si_table_in = sand.temp_hh_variables_&wave., 
	si_sial_table = COR_sentence_events_aln,	
	si_out_table = COR_sentence_events_rlp,	
	si_agg_cols= %str(department datamart subject_area),	
	si_id_col = &si_id_col. ,
	si_as_at_date = &si_asat_date. ,
	si_amount_col = NA ,
	cost = False, 
	duration = True, 
	count = False, 
	count_startdate = False, 
	dayssince = False,
	si_rollup_ouput_type = Both
);





/*********************************IRD Admin variables**************************************************/

/* Gross Income in the month of interview. */
%si_align_sialevents_to_periods(
			si_table_in=[IDI_Sandpit].[&si_proj_schema.].temp_hh_variables_&wave.,
			si_sial_table=[IDI_Sandpit].[&si_proj_schema.].SIAL_IRD_income_events , 						
			si_id_col = &si_id_col. , 
			si_as_at_date = &si_asat_date. ,
			si_amount_type = &si_amount_type. , 
			si_amount_col = &si_sial_amount_col. ,  
			noofperiodsbefore = -1 , 
			noofperiodsafter = 1 , 
			period_duration = 30 , 
			si_out_table = IRD_income_events_30d_aln,
			period_aligned_to_calendar = False, 
			si_pi_type = &si_price_index_type. , 
			si_pi_qtr = &si_price_index_qtr. );
%si_create_rollup_vars(
	si_table_in = sand.temp_hh_variables_&wave., 
	si_sial_table = IRD_income_events_30d_aln,	
	si_out_table = IRD_income_events_30d_rlp,	
	si_agg_cols= %str(department datamart subject_area),	
	si_id_col = &si_id_col. ,
	si_as_at_date = &si_asat_date. ,
	si_amount_col = &si_sial_amount_col._&si_price_index_type._&si_price_index_qtr. ,
	cost = True, 
	duration = False, 
	count = False, 
	count_startdate = False, 
	dayssince = False,
	si_rollup_ouput_type = Long
);

/* Get total T2 benefits in the month of interview */
%si_align_sialevents_to_periods(
			si_table_in=[IDI_Sandpit].[&si_proj_schema.].temp_hh_variables_&wave.,
			si_sial_table=[IDI_Sandpit].[&si_proj_schema.].SIAL_MSD_t2_events , 						
			si_id_col = &si_id_col. , 
			si_as_at_date = &si_asat_date. ,
			si_amount_type = &si_amount_type. , 
			si_amount_col = &si_sial_amount_col. ,  
			noofperiodsbefore = -1 , 
			noofperiodsafter = 1 , /* Since we would like to look at accomodation supplement 1 month after the interview as well.*/
			period_duration = 30 , 
			si_out_table = MSD_t2_events_30d_aln,
			period_aligned_to_calendar = False, 
			si_pi_type = &si_price_index_type. , 
			si_pi_qtr = &si_price_index_qtr. );
%si_create_rollup_vars(
	si_table_in = sand.temp_hh_variables_&wave., 
	si_sial_table = MSD_t2_events_30d_aln,	
	si_out_table = MSD_t2_events_30d_rlp,	
	si_agg_cols= %str(department datamart subject_area event_type),	
	si_id_col = &si_id_col. ,
	si_as_at_date = &si_asat_date. ,
	si_amount_col = &si_sial_amount_col._&si_price_index_type._&si_price_index_qtr. ,
	cost = True, 
	duration = False, 
	count = False, 
	count_startdate = False, 
	dayssince = False,
	si_rollup_ouput_type = Long
);

/* Interview month's income from tax credits*/
%si_align_sialevents_to_periods(
			si_table_in=[IDI_Sandpit].[&si_proj_schema.].temp_hh_variables_&wave.,
			si_sial_table=[IDI_Sandpit].[&si_proj_schema.].of_gss_admin_taxcreds , 						
			si_id_col = &si_id_col. , 
			si_as_at_date = &si_asat_date. ,
			si_amount_type = &si_amount_type. , 
			si_amount_col = &si_sial_amount_col. ,  
			noofperiodsbefore = -1 , 
			noofperiodsafter = 1 , 
			period_duration = 30 , 
			si_out_table = ALT_taxcred_events_30d_aln,
			period_aligned_to_calendar = False, 
			si_pi_type = &si_price_index_type. , 
			si_pi_qtr = &si_price_index_qtr. );
%si_create_rollup_vars(
	si_table_in = sand.temp_hh_variables_&wave., 
	si_sial_table = ALT_taxcred_events_30d_aln,	
	si_out_table = ALT_taxcred_events_30d_rlp,	
	si_agg_cols= %str(department datamart subject_area),	
	si_id_col = &si_id_col. ,
	si_as_at_date = &si_asat_date. ,
	si_amount_col = &si_sial_amount_col._&si_price_index_type._&si_price_index_qtr. ,
	cost = True, 
	duration = False, 
	count = False, 
	count_startdate = False, 
	dayssince = False,
	si_rollup_ouput_type = Long
);


/*********************************IRD Admin variables / 1year for net monthly income *********************/
/* IRD income - 1 year before interview, monthly */
%si_align_sialevents_to_periods(
			si_table_in=[IDI_Sandpit].[&si_proj_schema.].temp_hh_variables_&wave.,
			si_sial_table=[IDI_Sandpit].[&si_proj_schema.].SIAL_IRD_income_events , 						
			si_id_col = &si_id_col. , 
			si_as_at_date = &si_asat_date. ,
			si_amount_type = &si_amount_type. , 
			si_amount_col = &si_sial_amount_col. ,  
			noofperiodsbefore = -1 , 
			noofperiodsafter = 1 , 
			period_duration = Year , 
			si_out_table = IRD_income_events_1yr_aln,
			period_aligned_to_calendar = False, 
			si_pi_type = &si_price_index_type. , 
			si_pi_qtr = &si_price_index_qtr. );

%si_create_rollup_vars(
	si_table_in = sand.temp_hh_variables_&wave., 
	si_sial_table = IRD_income_events_1yr_aln,	
	si_out_table = IRD_income_events_1yr_rlp,	
	si_agg_cols= %str(department datamart subject_area),	
	si_id_col = &si_id_col. ,
	si_as_at_date = &si_asat_date. ,
	si_amount_col = &si_sial_amount_col._&si_price_index_type._&si_price_index_qtr. ,
	cost = True, 
	duration = False, 
	count = False, 
	count_startdate = False, 
	dayssince = False,
	si_rollup_ouput_type = Both
);

/*MSD T2 income - 1 year before interview, monthly */
%si_align_sialevents_to_periods(
			si_table_in=[IDI_Sandpit].[&si_proj_schema.].temp_hh_variables_&wave.,
			si_sial_table=[IDI_Sandpit].[&si_proj_schema.].SIAL_MSD_t2_events , 						
			si_id_col = &si_id_col. , 
			si_as_at_date = &si_asat_date. ,
			si_amount_type = &si_amount_type. , 
			si_amount_col = &si_sial_amount_col. ,  
			noofperiodsbefore = -1 , 
			noofperiodsafter = 1 ,  
			period_duration = Year , 
			si_out_table = MSD_t2_events_1yr_aln,
			period_aligned_to_calendar = False, 
			si_pi_type = &si_price_index_type. , 
			si_pi_qtr = &si_price_index_qtr. );
%si_create_rollup_vars(
	si_table_in = sand.temp_hh_variables_&wave., 
	si_sial_table = MSD_t2_events_1yr_aln,	
	si_out_table = MSD_t2_events_1yr_rlp,	
	si_agg_cols= %str(department datamart subject_area event_type),	
	si_id_col = &si_id_col. ,
	si_as_at_date = &si_asat_date. ,
	si_amount_col = &si_sial_amount_col._&si_price_index_type._&si_price_index_qtr. ,
	cost = True, 
	duration = False, 
	count = False, 
	count_startdate = False, 
	dayssince = False,
	si_rollup_ouput_type = Both
);

/* Tax credits - 1 year before interview, monthly */
%si_align_sialevents_to_periods(
			si_table_in=[IDI_Sandpit].[&si_proj_schema.].temp_hh_variables_&wave.,
			si_sial_table=[IDI_Sandpit].[&si_proj_schema.].of_gss_admin_taxcreds , 						
			si_id_col = &si_id_col. , 
			si_as_at_date = &si_asat_date. ,
			si_amount_type = &si_amount_type. , 
			si_amount_col = &si_sial_amount_col. ,  
			noofperiodsbefore = -1 , 
			noofperiodsafter = 1 , 
			period_duration = Year , 
			si_out_table = ALT_taxcred_events_1yr_aln,
			period_aligned_to_calendar = False, 
			si_pi_type = &si_price_index_type. , 
			si_pi_qtr = &si_price_index_qtr. );
%si_create_rollup_vars(
	si_table_in = sand.temp_hh_variables_&wave., 
	si_sial_table = ALT_taxcred_events_1yr_aln,	
	si_out_table = ALT_taxcred_events_1yr_rlp,	
	si_agg_cols= %str(department datamart subject_area event_type),	
	si_id_col = &si_id_col. ,
	si_as_at_date = &si_asat_date. ,
	si_amount_col = &si_sial_amount_col._&si_price_index_type._&si_price_index_qtr. ,
	cost = True, 
	duration = False, 
	count = False, 
	count_startdate = False, 
	dayssince = False,
	si_rollup_ouput_type = Both
);





/******** End Temporary addition****************/



/*******************************************************************************************************/
/* Consolidate all admin variables and create custom variables as required 

Yearly Net Income is defined as follows: 
 Income 
	P_IRD_EMP_C00_CST
	P_IRD_EMP_C01_CST
	P_IRD_EMP_C02_CST
	P_IRD_EMP_P00_CST
	P_IRD_EMP_P01_CST
	P_IRD_EMP_PPL_CST
	P_IRD_EMP_S00_CST
	P_IRD_EMP_S01_CST
	P_IRD_EMP_S02_CST
	P_IRD_EMP_WAS_CST
	P_IRD_INS_BEN_CST
	P_IRD_INS_CLM_CST
	P_IRD_INS_PEN_CST
	P_IRD_RNT_S03_CST
	P_IRD_STS_STU_CST
	P_MSD_BEN_T2_065_CST
	P_MSD_BEN_T2_340_CST
	P_MSD_BEN_T2_344_CST
	P_MSD_BEN_T2_425_CST
	P_MSD_BEN_T2_450_CST
	P_MSD_BEN_T2_460_CST
	P_MSD_BEN_T2_471_CST
	P_MSD_BEN_T2_500_CST
	P_MSD_BEN_T2_833_CST
	P_MSD_BEN_T2_835_CST
	P_MSD_BEN_T2_838_CST
	P_INC_INC_INC_emstpm_CST
	P_INC_INC_INC_fdrtpi_CST
	P_INC_INC_INC_frdtir_CST
	P_INC_INC_INC_frdtwi_CST
 Tax 
	P_IRD_PYE_BEN_CST
	P_IRD_PYE_CLM_CST
	P_IRD_PYE_PEN_CST
	P_IRD_PYE_PPL_CST
	P_IRD_PYE_STU_CST
	P_IRD_PYE_WAS_CST
	P_IRD_WHT_WHP_CST
*/



proc sql;

	create table final_adminvars_&wave. as 
	select 
		ind.snz_uid
		,"&wave." as wave
		,t1.P_BEN_DUR/365.24 as P_BEN_DUR_prop
		,coalesce(ird.inc_intvwmnth, 0.00) as inc_intvwmnth_gross /* Gross Income in the specific month before interview*/
		,coalesce(irdf.inc_intvwmnth_after, 0.00) as inc_intvwmnth_gross_after /* Gross Income in the specific month after interview*/
		,coalesce(paye.incpaye_intvwmnth, 0.00) as incpaye_intvwmnth_gross /* PAYE Income in the specific month before interview*/
		,coalesce(payef.incpaye_intvwmnth_after, 0.00) as incpaye_intvwmnth_gross_after /* PAYE Income in the specific month after interview*/
		,coalesce(yearlynet.income_monthly_net_before, 0.00) as income_monthly_net_before /* Average monthly net income for 1 year before interview*/
		,coalesce(yearlynetf.income_monthly_net_after, 0.00) as income_monthly_net_after /* Average monthly net income for 1 year before interview*/

		,coalesce(tim.transfer_income_monthly_b4, 0.00) as transfer_income_monthly_before /* Average monthly net income for 1 year before interview*/
		,coalesce(timf.transfer_income_monthly_af, 0.00) as transfer_income_monthly_after /* Average monthly net income for 1 year before interview*/

		,coalesce(oiw.other_income_weekly_b4, 0.00) as other_income_monthly_before /* Average monthly net income for 1 year before interview*/
		,coalesce(oiwf.other_income_weekly_af, 0.00) as other_income_monthly_after /* Average monthly net income for 1 year before interview*/

		,coalesce(tib.total_income_before, 0.00) as total_income_monthly_before /* Average monthly net income for 1 year before interview*/
		,coalesce(tia.total_income_after, 0.00) as total_income_monthly_after /* Average monthly net income for 1 year before interview*/

		,coalesce(mbb.main_benefit_b4, 0.00) as main_benefit_monthly_before /* Average monthly net income for 1 year before interview*/
		,coalesce(mba.main_benefit_af, 0.00) as main_benefit_monthly_after /* Average monthly net income for 1 year before interview*/

		,coalesce(tcb.tax_cred_b4, 0.00) as tax_credit_monthly_before /* Average monthly net income for 1 year before interview*/
		,coalesce(tca.tax_cred_af, 0.00) as tax_credit_monthly_after /* Average monthly net income for 1 year before interview*/

		,coalesce(sbb.supp_benefit_b4, 0.00) as supp_benefit_monthly_before /* Average monthly net income for 1 year before interview*/
		,coalesce(sba.supp_benefit_af, 0.00) as supp_benefit_monthly_after /* Average monthly net income for 1 year before interview*/

		,case when cor.snz_uid is null then 0 else 1 end as P_COR_ind
		,accom_b4.value as accom_sup_1mnth_b4_intvw
		,accom_af.value as accom_sup_1mnth_af_intvw
		,accom_b41.value as accom_sup_1yr_b4_intvw
		,accom_af1.value as accom_sup_1yr_af_intvw
	from 
	sand.temp_hh_variables_&wave. ind
	left join MSD_t1_events_rlpw t1 on (ind.snz_uid = t1.snz_uid)
	left join COR_sentence_events_rlpw cor on (ind.snz_uid = cor.snz_uid)

	/* Gross Income in the month of interview*/
	left join (select snz_uid, sum(value) as inc_intvwmnth from 
				(	select * from IRD_income_events_30d_rlpl 
					where 
						(vartype like 'P_IRD_EMP%CST' 
							or vartype like 'P_IRD_INS%CST'
							or vartype like 'P_IRD_STS%CST'
							or vartype like 'P_IRD_RNT%CST')
						and vartype not in ('P_IRD_INS_FTCb_CST','P_IRD_INS_FTCn_CST') /*Exclude Family Tax credits as it is accounted from external source*/
					union all 
					select * from MSD_t2_events_30d_rlpl 
					where vartype like 'P_MSD_%' 
						and vartype not like 'P_MSD_BEN_T2_064_CST' /*Exclude Family Tax credits as it is accounted  from external source*/
					union all 
					select * from ALT_taxcred_events_30d_rlpl where vartype like 'P_INC_%' /*Tax credits, from IR3, PTS, FRD and FDR combined*/
				)
				group by snz_uid) ird on (ind.snz_uid = ird.snz_uid)

		/* Gross Income in the month after the interview*/
	left join (select snz_uid, sum(value) as inc_intvwmnth_after from 
				(	select * from IRD_income_events_30d_rlpl 
					where 
						(vartype like 'F_IRD_EMP%CST' 
							or vartype like 'F_IRD_INS%CST'
							or vartype like 'F_IRD_STS%CST'
							or vartype like 'F_IRD_RNT%CST')
						and vartype not in ('F_IRD_INS_FTCb_CST','F_IRD_INS_FTCn_CST') /*Exclude Family Tax credits as it is accounted from external source*/
					union all 
					select * from MSD_t2_events_30d_rlpl 
					where vartype like 'F_MSD_%' 
						and vartype not like 'F_MSD_BEN_T2_064_CST' /*Exclude Family Tax credits as it is accounted  from external source*/
					union all 
					select * from ALT_taxcred_events_30d_rlpl where vartype like 'F_INC_%' /*Tax credits, from IR3, PTS, FRD and FDR combined*/
				)
				group by snz_uid) irdf on (ind.snz_uid = irdf.snz_uid)

	/* Gross PAYE income for individual from month of interview*/
	left join (select snz_uid, sum(value) as incpaye_intvwmnth from 
				(	select * from IRD_income_events_30d_rlpl 
					where vartype = 'P_IRD_EMP_WAS_CST' )
				group by snz_uid) paye on (ind.snz_uid = paye.snz_uid)

	
	/* Gross PAYE income for individual from month of interview*/
	left join (select snz_uid, sum(value) as incpaye_intvwmnth_after from 
				(	select * from IRD_income_events_30d_rlpl 
					where vartype = 'F_IRD_EMP_WAS_CST' )
				group by snz_uid) payef on (ind.snz_uid = payef.snz_uid)

	/* Yearly Net Income :  tax credits edited 07 August 2018*/
	left join (select 
					snz_uid, sum(value)/12.0 as income_monthly_net_before
				from 
					(/*Exclude Student Loans & tax credit components, add in the tax components and withheld payments*/
					select * from IRD_income_events_1yr_rlpl where vartype not like 'P_IRD_STL%' and vartype like 'P_IRD_%' 
						and vartype not in ('P_IRD_INS_FTCb_CST','P_IRD_INS_FTCn_CST')
					union all 
					/*Exclude Family Tax credits as it is accounted from IRD table*/
					select * from MSD_t2_events_1yr_rlpl where vartype like 'P_MSD_%' and vartype not like 'P_MSD_BEN_T2_064_CST'
					union all 
					/*Add in Family Tax credits*/
					select * from ALT_taxcred_events_1yr_rlpl where vartype like 'P_INC_%' )x
				group by snz_uid) yearlynet on (ind.snz_uid = yearlynet.snz_uid)


	/* Yearly Net Income, tax credits edited 07 August 2018*/
	left join (select 
					snz_uid, sum(value)/12.0 as income_monthly_net_after
				from 
					(/*Exclude Student Loans & tax credit components, add in the tax components and withheld payments*/
					select * from IRD_income_events_1yr_rlpl where vartype not like 'F_IRD_STL%' and vartype like 'F_IRD_%' 
						and vartype not in ('F_IRD_INS_FTCb_CST','F_IRD_INS_FTCn_CST')
					union all 
					/*Exclude Family Tax credits as it is accounted from IRD table*/
					select * from MSD_t2_events_1yr_rlpl where vartype like 'F_MSD_%' and vartype not like 'F_MSD_BEN_T2_064_CST'
					union all 
					/*Add in Family Tax credits*/
					select * from ALT_taxcred_events_1yr_rlpl where vartype like 'F_INC_%' )x
				group by snz_uid) yearlynetf on (ind.snz_uid = yearlynetf.snz_uid)


	/* Income broken down into components*/
	/*		Transfer income before/after itw*/
		left join(
		select snz_uid,sum(value)/12 as transfer_income_monthly_b4 from (
			select * from IRD_income_events_1yr_rlpl 
				union all 
				select * from MSD_t2_events_1yr_rlpl
				union all 
				select * from ALT_taxcred_events_1yr_rlpl)
			where vartype in ("P_MSD_BEN_T2_065_CST" 						
						,"P_MSD_BEN_T2_340_CST" 
						,"P_MSD_BEN_T2_344_CST" 
						,"P_MSD_BEN_T2_425_CST" 
						,"P_MSD_BEN_T2_450_CST" 
						,"P_MSD_BEN_T2_460_CST" 
						,"P_MSD_BEN_T2_471_CST" 
						,"P_MSD_BEN_T2_500_CST" 
						,"P_MSD_BEN_T2_833_CST" 
						,"P_MSD_BEN_T2_835_CST"
						,"P_MSD_BEN_T2_838_CST"
						,"P_IRD_INS_BEN_CST"
						,"P_IRD_PYE_BEN_CST"
						,"P_INC_INC_INC_emstpm_CST"
						,"P_INC_INC_INC_fdrtpi_CST" 
						,"P_INC_INC_INC_frdtir_CST"
						,"P_INC_INC_INC_frdtwi_CST") group by snz_uid ) tim
			on (ind.snz_uid=tim.snz_uid)

		left join(
		select snz_uid,sum(value)/12 as transfer_income_monthly_af from (
			select * from IRD_income_events_1yr_rlpl 
				union all 
				select * from MSD_t2_events_1yr_rlpl
				union all 
				select * from ALT_taxcred_events_1yr_rlpl)
			where vartype in ("F_MSD_BEN_T2_065_CST" 						
						,"F_MSD_BEN_T2_340_CST" 
						,"F_MSD_BEN_T2_344_CST" 
						,"F_MSD_BEN_T2_425_CST" 
						,"F_MSD_BEN_T2_450_CST" 
						,"F_MSD_BEN_T2_460_CST" 
						,"F_MSD_BEN_T2_471_CST" 
						,"F_MSD_BEN_T2_500_CST" 
						,"F_MSD_BEN_T2_833_CST" 
						,"F_MSD_BEN_T2_835_CST"
						,"F_MSD_BEN_T2_838_CST"
						,"F_IRD_INS_BEN_CST"
						,"F_IRD_PYE_BEN_CST"
						,"F_INC_INC_INC_emstpm_CST"
						,"F_INC_INC_INC_fdrtpi_CST" 
						,"F_INC_INC_INC_frdtir_CST"
						,"F_INC_INC_INC_frdtwi_CST") group by snz_uid ) timf
			on (ind.snz_uid=timf.snz_uid)

	/*		Other income before/after itw*/
		left join(
		select snz_uid,sum(value)/12 as other_income_weekly_b4 from (
			select * from IRD_income_events_1yr_rlpl 
				union all 
				select * from MSD_t2_events_1yr_rlpl
				union all 
				select * from ALT_taxcred_events_1yr_rlpl)
			where vartype in ("P_IRD_EMP_C00_CST"
					,"P_IRD_EMP_C01_CST"
					,"P_IRD_EMP_C02_CST"
					,"P_IRD_EMP_P00_CST"
					,"P_IRD_EMP_P01_CST"
					,"P_IRD_EMP_PPL_CST"
					,"P_IRD_EMP_S00_CST"
					,"P_IRD_EMP_S01_CST"
					,"P_IRD_EMP_S02_CST"
					,"P_IRD_EMP_WAS_CST"
					,"P_IRD_INS_CLM_CST"
					,"P_IRD_INS_PEN_CST" 
					,"P_IRD_RNT_S03_CST"
					,"P_IRD_STS_STU_CST"
					,"P_IRD_PYE_CLM_CST" 
					,"P_IRD_PYE_PEN_CST" 
					,"P_IRD_PYE_PPL_CST" 
					,"P_IRD_PYE_STU_CST" 
					,"P_IRD_PYE_WAS_CST"
					,"P_IRD_WHT_WHP_CST") group by snz_uid ) oiw
			on (ind.snz_uid=oiw.snz_uid)

		left join(
		select snz_uid,sum(value)/12 as other_income_weekly_af from (
			select * from IRD_income_events_1yr_rlpl 
				union all 
				select * from MSD_t2_events_1yr_rlpl
				union all 
				select * from ALT_taxcred_events_1yr_rlpl)
			where vartype in ("F_IRD_EMP_C00_CST"
					,"F_IRD_EMP_C01_CST"
					,"F_IRD_EMP_C02_CST"
					,"F_IRD_EMP_P00_CST"
					,"F_IRD_EMP_P01_CST"
					,"F_IRD_EMP_PPL_CST"
					,"F_IRD_EMP_S00_CST"
					,"F_IRD_EMP_S01_CST"
					,"F_IRD_EMP_S02_CST"
					,"F_IRD_EMP_WAS_CST"
					,"F_IRD_INS_CLM_CST"
					,"F_IRD_INS_PEN_CST" 
					,"F_IRD_RNT_S03_CST"
					,"F_IRD_STS_STU_CST"
					,"F_IRD_PYE_CLM_CST" 
					,"F_IRD_PYE_PEN_CST" 
					,"F_IRD_PYE_PPL_CST" 
					,"F_IRD_PYE_STU_CST" 
					,"F_IRD_PYE_WAS_CST"
					,"F_IRD_WHT_WHP_CST") group by snz_uid ) oiwf
			on (ind.snz_uid=oiwf.snz_uid)

	/*		Total income before/after itw*/
			left join(
		select snz_uid,sum(value)/12 as total_income_before from (
			select * from IRD_income_events_1yr_rlpl 
				union all 
				select * from MSD_t2_events_1yr_rlpl
				union all 
				select * from ALT_taxcred_events_1yr_rlpl)
			where vartype in ("P_MSD_BEN_T2_065_CST" 					
						,"P_MSD_BEN_T2_340_CST" 
						,"P_MSD_BEN_T2_344_CST" 
						,"P_MSD_BEN_T2_425_CST" 
						,"P_MSD_BEN_T2_450_CST" 
						,"P_MSD_BEN_T2_460_CST" 
						,"P_MSD_BEN_T2_471_CST" 
						,"P_MSD_BEN_T2_500_CST" 
						,"P_MSD_BEN_T2_833_CST" 
						,"P_MSD_BEN_T2_835_CST"
						,"P_MSD_BEN_T2_838_CST"
						,"P_IRD_INS_BEN_CST"
						,"P_IRD_PYE_BEN_CST"
						,"P_INC_INC_INC_emstpm_CST"
						,"P_INC_INC_INC_fdrtpi_CST" 
						,"P_INC_INC_INC_frdtir_CST"
						,"P_INC_INC_INC_frdtwi_CST"
						,"P_IRD_EMP_C00_CST"
						,"P_IRD_EMP_C01_CST"
						,"P_IRD_EMP_C02_CST"
						,"P_IRD_EMP_P00_CST"
						,"P_IRD_EMP_P01_CST"
						,"P_IRD_EMP_PPL_CST"
						,"P_IRD_EMP_S00_CST"
						,"P_IRD_EMP_S01_CST"
						,"P_IRD_EMP_S02_CST"
						,"P_IRD_EMP_WAS_CST"
						,"P_IRD_INS_CLM_CST"
						,"P_IRD_INS_PEN_CST" 
						,"P_IRD_RNT_S03_CST"
						,"P_IRD_STS_STU_CST"
						,"P_IRD_PYE_CLM_CST" 
						,"P_IRD_PYE_PEN_CST" 
						,"P_IRD_PYE_PPL_CST" 
						,"P_IRD_PYE_STU_CST" 
						,"P_IRD_PYE_WAS_CST"
						,"P_IRD_WHT_WHP_CST" ) group by snz_uid ) tib
			on (ind.snz_uid=tib.snz_uid)


						left join(
		select snz_uid,sum(value)/12 as total_income_after from (
			select * from IRD_income_events_1yr_rlpl 
				union all 
				select * from MSD_t2_events_1yr_rlpl
				union all 
				select * from ALT_taxcred_events_1yr_rlpl)
			where vartype in ("F_MSD_BEN_T2_065_CST" 					
					,"F_MSD_BEN_T2_340_CST" 
					,"F_MSD_BEN_T2_344_CST" 
					,"F_MSD_BEN_T2_425_CST" 
					,"F_MSD_BEN_T2_450_CST" 
					,"F_MSD_BEN_T2_460_CST" 
					,"F_MSD_BEN_T2_471_CST" 
					,"F_MSD_BEN_T2_500_CST" 
					,"F_MSD_BEN_T2_833_CST" 
					,"F_MSD_BEN_T2_835_CST"
					,"F_MSD_BEN_T2_838_CST"
					,"F_IRD_INS_BEN_CST"
					,"F_IRD_PYE_BEN_CST"
					,"F_INC_INC_INC_emstpm_CST"
					,"F_INC_INC_INC_fdrtpi_CST" 
					,"F_INC_INC_INC_frdtir_CST"
					,"F_INC_INC_INC_frdtwi_CST"
					,"F_IRD_EMP_C00_CST"
					,"F_IRD_EMP_C01_CST"
					,"F_IRD_EMP_C02_CST"
					,"F_IRD_EMP_P00_CST"
					,"F_IRD_EMP_P01_CST"
					,"F_IRD_EMP_PPL_CST"
					,"F_IRD_EMP_S00_CST"
					,"F_IRD_EMP_S01_CST"
					,"F_IRD_EMP_S02_CST"
					,"F_IRD_EMP_WAS_CST"
					,"F_IRD_INS_CLM_CST"
					,"F_IRD_INS_PEN_CST" 
					,"F_IRD_RNT_S03_CST"
					,"F_IRD_STS_STU_CST"
					,"F_IRD_PYE_CLM_CST" 
					,"F_IRD_PYE_PEN_CST" 
					,"F_IRD_PYE_PPL_CST" 
					,"F_IRD_PYE_STU_CST" 
					,"F_IRD_PYE_WAS_CST"
					,"F_IRD_WHT_WHP_CST") group by snz_uid ) tia
			on (ind.snz_uid=tia.snz_uid)

		/*		Main benefit income before/after itw*/	
						left join(
		select snz_uid,sum(value)/12 as main_benefit_b4 from (
			select * from IRD_income_events_1yr_rlpl 
				union all 
				select * from MSD_t2_events_1yr_rlpl
				union all 
				select * from ALT_taxcred_events_1yr_rlpl)
			where vartype in (
					"P_IRD_INS_BEN_CST"
						,"P_IRD_PYE_BEN_CST") group by snz_uid ) mbb
			on (ind.snz_uid=mbb.snz_uid)

				left join(
		select snz_uid,sum(value)/12 as main_benefit_af from (
			select * from IRD_income_events_1yr_rlpl 
				union all 
				select * from MSD_t2_events_1yr_rlpl
				union all 
				select * from ALT_taxcred_events_1yr_rlpl)
			where vartype in (
					"F_IRD_INS_BEN_CST"
						,"F_IRD_PYE_BEN_CST") group by snz_uid ) mba
			on (ind.snz_uid=mba.snz_uid)

	/*		Tax credit income before/after itw*/
	left join(
		select snz_uid,sum(value)/12 as tax_cred_b4 from (
			select * from IRD_income_events_1yr_rlpl 
				union all 
				select * from MSD_t2_events_1yr_rlpl
				union all 
				select * from ALT_taxcred_events_1yr_rlpl)
			where vartype in (
					"P_INC_INC_INC_emstpm_CST"
						,"P_INC_INC_INC_fdrtpi_CST"
						,"P_INC_INC_INC_frdtir_CST"
						,"P_INC_INC_INC_frdtwi_CST") group by snz_uid ) tcb
			on (ind.snz_uid=tcb.snz_uid)

		left join(
		select snz_uid,sum(value)/12 as tax_cred_af from (
			select * from IRD_income_events_1yr_rlpl 
				union all 
				select * from MSD_t2_events_1yr_rlpl
				union all 
				select * from ALT_taxcred_events_1yr_rlpl)
			where vartype in (
					"F_INC_INC_INC_emstpm_CST"
						,"F_INC_INC_INC_fdrtpi_CST"
						,"F_INC_INC_INC_frdtir_CST"
						,"F_INC_INC_INC_frdtwi_CST") group by snz_uid ) tca
			on (ind.snz_uid=tca.snz_uid)

		/*		Supp benefit before/after itw*/
		left join(
		select snz_uid,sum(value)/12 as supp_benefit_b4 from (
			select * from IRD_income_events_1yr_rlpl 
				union all 
				select * from MSD_t2_events_1yr_rlpl
				union all 
				select * from ALT_taxcred_events_1yr_rlpl)
			where vartype in (
			"P_MSD_BEN_T2_065_CST" 						
						,"P_MSD_BEN_T2_340_CST" 
						,"P_MSD_BEN_T2_344_CST" 
						,"P_MSD_BEN_T2_425_CST" 
						,"P_MSD_BEN_T2_450_CST" 
						,"P_MSD_BEN_T2_460_CST" 
						,"P_MSD_BEN_T2_471_CST" 
						,"P_MSD_BEN_T2_500_CST" 
						,"P_MSD_BEN_T2_833_CST" 
						,"P_MSD_BEN_T2_835_CST"
						,"P_MSD_BEN_T2_838_CST") group by snz_uid ) sbb
			on (ind.snz_uid=sbb.snz_uid)

				left join(
		select snz_uid,sum(value)/12 as supp_benefit_af from (
			select * from IRD_income_events_1yr_rlpl 
				union all 
				select * from MSD_t2_events_1yr_rlpl
				union all 
				select * from ALT_taxcred_events_1yr_rlpl)
			where vartype in (
			"F_MSD_BEN_T2_065_CST" 						
						,"F_MSD_BEN_T2_340_CST" 
						,"F_MSD_BEN_T2_344_CST" 
						,"F_MSD_BEN_T2_425_CST" 
						,"F_MSD_BEN_T2_450_CST" 
						,"F_MSD_BEN_T2_460_CST" 
						,"F_MSD_BEN_T2_471_CST" 
						,"F_MSD_BEN_T2_500_CST" 
						,"F_MSD_BEN_T2_833_CST" 
						,"F_MSD_BEN_T2_835_CST"
						,"F_MSD_BEN_T2_838_CST") group by snz_uid ) sba
			on (ind.snz_uid=sba.snz_uid)

	left join (select * from MSD_t2_events_30d_rlpl where vartype = 'P_MSD_BEN_T2_471_CST') accom_b4 
		on (ind.snz_uid = accom_b4.snz_uid)
	left join (select * from MSD_t2_events_30d_rlpl where vartype = 'F_MSD_BEN_T2_471_CST') accom_af 
		on (ind.snz_uid = accom_af.snz_uid)
	left join (select * from MSD_t2_events_1yr_rlpl where vartype = 'P_MSD_BEN_T2_471_CST') accom_b41 
		on (ind.snz_uid = accom_b41.snz_uid)
	left join (select * from MSD_t2_events_1yr_rlpl where vartype = 'F_MSD_BEN_T2_471_CST') accom_af1 
		on (ind.snz_uid = accom_af1.snz_uid);




quit;


proc datasets lib=work;
	delete MSD_: ALT_: COR_: IRD_: TMP_: EMS_: IR_: INCOME:;
run;


%mend;

%massive_round_up(GSS2016);
%massive_round_up(GSS2014);
%massive_round_up(GSS2012);
%massive_round_up(GSS2010);
%massive_round_up(GSS2008);
/**/
/*Combining all the data together*/
proc sql;
	create table _temp_adminvars as 
	select * from final_adminvars_gss2008
	union all 
	select * from final_adminvars_gss2010
	union all 
	select * from final_adminvars_gss2012
	union all 
	select * from final_adminvars_gss2014
	union all 
	select * from final_adminvars_gss2016;
QUIT;



/* Replace NULLs in admin variables with 0 */
proc stdize data=_temp_adminvars out=_temp_adminvars reponly missing=0;run;
proc sort data=_temp_adminvars out=_temp_adminvars_f nodupkey; by snz_uid wave; run;

/* Add in the admin variables into the "si_pop_table_out" table and write to database*/
proc sql;

	create table _temp_adminvars_comp as 
	select 
		ind.*
		,temp.*
	from 
	sand.&si_pop_table_out. ind
	left join _temp_adminvars_f temp on (ind.snz_uid = temp.snz_uid and ind.gss_id_collection_code=temp.wave);

quit;

%si_write_to_db(si_write_table_in=work._temp_adminvars_comp,
	si_write_table_out=&si_sandpit_libname..of_gss_hh_variables_sh3_admin
	,si_cluster_index_flag=True,si_index_cols=%bquote(&si_id_col.)
	);

/*Delete temp*/
proc datasets lib=work;
	delete _temp_: final:;
run;
proc delete data=sand.temp_hh_variables_GSS2016; run;
proc delete data=sand.temp_hh_variables_GSS2014; run;
proc delete data=sand.temp_hh_variables_GSS2012; run;
proc delete data=sand.temp_hh_variables_GSS2010; run;
proc delete data=sand.temp_hh_variables_GSS2008; run;
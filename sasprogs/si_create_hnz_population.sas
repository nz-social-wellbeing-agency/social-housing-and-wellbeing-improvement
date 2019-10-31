/*********************************************************************************************************
DESCRIPTION: 
Creates the HNZ population subset to the GSS population.

INPUT:
[&idi_version.].[gss_clean].[gss_household] = 2014 GSS household table
[&idi_version.].[gss_clean].[gss_household_2012] = 2012 GSS household table
[&idi_version.].[gss_clean].[gss_household_2010] = 2010 GSS household table
[&idi_version.].[gss_clean].[gss_household_2008] = 2008 GSS household table
[&idi_version.].[hnz_clean].[tenancy_snapshot]
[&idi_version.].[hnz_clean].[tenancy_household_snapshot]
[&idi_version.].[hnz_clean].[tenancy_exit]


OUTPUT:
sand.assembled_tenancy_HNZ = dataset with HNZ tenancy details for individuals in GSS08-14
sand.assembled_registry_HNZ = dataset with HNZ register details for individuals in GSS08-14
sand.of_hnz_gss_population = dataset with household variables for GSS

AUTHOR: 
S Anastasiadis

DEPENDENCIES:
NA

NOTES:   
1. Individuals in the GSS households are not linked to the spine at the time of writing this code, except
	for those individuals who also answer the personal questionnaire. 
2. All GSS waves are available only from &idi_version._20171027 onwards.


HISTORY: 
22 Nov 2017 VB Converted the SQL version into SAS.
21 Nov 2018 BV adapted for SH3 (+GSS2016 + improved linkage rate)

*******************************************************************************************

/* GSS PQ is part of the HNZ application (mandatory) + HNZ primary applicant is part of the GSS household (indicator) */

proc sql;
	connect to odbc (dsn=&idi_version._srvprd);
	create table _temp_of_hnz_gss_population as 
		select * from connection to odbc (

		with trim_nah as (
				SELECT [snz_uid]
						,[snz_application_uid]
						,[snz_legacy_application_uid]
						,[snz_msd_application_uid]
						,[hnz_nah_app_relship_text]
				FROM [IDI_Clean_20181020].[hnz_clean].[new_applications_household]
			)
/*			HNZ new applications*/
			,trim_na as (
				SELECT [snz_uid]
						,[hnz_na_date_of_application_date]
						,[snz_application_uid]
						,[snz_legacy_application_uid]
						,[snz_msd_application_uid]
						,[hnz_na_analy_score_afford_text]
						,[hnz_na_analy_score_adeq_text]
						,[hnz_na_analy_score_suitably_text]
						,[hnz_na_analy_score_sustain_text]
						,[hnz_na_analy_score_access_text]
						,[hnz_na_analysis_total_score_text]
						,[hnz_na_main_reason_app_text]
						,[hnz_na_hshd_size_nbr]
						,'new application' AS hnz_apply_type
				FROM [IDI_Clean_20181020].[hnz_clean].[new_applications]
			)
/*			HNZ tenancy snapshot (duplicates, different entry dates by house)*/
			,trim_ts as (
				SELECT distinct [snz_uid]
				  ,[snz_household_uid]
				  ,[snz_legacy_household_uid]
				  ,[snz_msd_application_uid]
				  ,coalesce([hnz_ts_house_entry_date],[msd_tenancy_start_date]) as house_entry_date
				  ,[snz_hnz_ts_house_uid]
				  ,[snz_hnz_ts_legacy_house_uid]
				  ,[snz_msd_house_uid]
				FROM [IDI_Clean_20181020].[hnz_clean].[tenancy_snapshot]
			)
/*		HNZ register exit, HOUSED or OTHER EXIT */
		    ,trim_re as (
				SELECT distinct [hnz_re_exit_date]
				,[snz_application_uid]
				,[snz_legacy_application_uid]
				,[snz_msd_application_uid]
				,[hnz_re_exit_status_text]
				,[hnz_re_exit_reason_text]
				,[snz_house_uid]
				,[snz_legacy_house_uid]
				,[snz_msd_house_uid]
				FROM [IDI_Clean_20181020].[hnz_clean].[register_exit]
/*				WHERE [hnz_re_exit_status_text] = 'HOUSED'*/
			)
/* Get the exit dates for each tenancy*/
			,trim_te as (
				SELECT distinct [snz_household_uid]
				  ,[snz_legacy_household_uid]
				  ,[snz_msd_application_uid]
				  ,[snz_msd_house_uid]
				  ,[hnz_te_exit_date]
				  ,[hnz_te_exit_status_text]
				FROM [IDI_Clean_20181020].[hnz_clean].[tenancy_exit]
				WHERE [hnz_te_exit_status_text] IS NOT NULL
			)
/*			Full GSS population with all household members*/
			,NZGSS_full_IDs as (
				select gss.snz_uid
				, gss.[gss_id_collection_code]
				, gss.[gss_pq_interview_start_date]
				, gss.snz_gss_hhld_uid
				, gss.as_at_date
				, gss2.snz_uid as snz_uid_pq
				FROM [IDI_Sandpit].[DL-MAA2016-15].[of_gss_hh_variables_sh3] gss
				left join [IDI_Sandpit].[DL-MAA2016-15].[of_gss_hh_variables_sh3] gss2 on gss.snz_gss_hhld_uid=gss2.snz_gss_hhld_uid where gss2.person=1
			)
			SELECT distinct gss.[gss_id_collection_code]
			,gss.[gss_pq_interview_start_date]
			,gss.[as_at_date]
			,gss.[snz_uid]
			,gss.[snz_uid_pq]
			,gss.[snz_gss_hhld_uid]
			,case when na.[snz_uid] = gss.[snz_uid] then 1 else 0 end as hnz_prim
			,na.[snz_uid] as snz_uid_hnz_prim
			,na.[hnz_na_date_of_application_date]
			,na.[snz_application_uid]
			,na.[snz_legacy_application_uid]
			,na.[snz_msd_application_uid]
			,re.[hnz_re_exit_date]
			,re.[hnz_re_exit_status_text]
			,ts.[house_entry_date]
			,datediff(m,gss.[gss_pq_interview_start_date],ts.[house_entry_date]) AS months_interview_to_entry /*in how many months will you enter*/
			,CASE WHEN datediff(d,gss.[gss_pq_interview_start_date],ts.[house_entry_date]) <= 0 
								THEN 'TREATED' ELSE 'CONTROL' END AS treat_control
			,CASE WHEN abs(datediff(d,re.[hnz_re_exit_date],ts.[house_entry_date])) IS NOT NULL 
								THEN datediff(d,re.[hnz_re_exit_date],ts.[house_entry_date]) ELSE 0 END AS days_granted_to_entry
			,re.[snz_house_uid]
			,re.[snz_legacy_house_uid]
			,re.[snz_msd_house_uid]
			,ts.[snz_hnz_ts_house_uid]
			,ts.[snz_hnz_ts_legacy_house_uid]
			,ts.[snz_household_uid]
			,ts.[snz_legacy_household_uid]
			,na.[hnz_na_analy_score_afford_text]
			,na.[hnz_na_analy_score_adeq_text]
			,na.[hnz_na_analy_score_suitably_text]
			,na.[hnz_na_analy_score_sustain_text]
			,na.[hnz_na_analy_score_access_text]
			,na.[hnz_na_analysis_total_score_text]
			,na.[hnz_na_main_reason_app_text]
			,na.[hnz_na_hshd_size_nbr]
			,na.[hnz_apply_type]

			FROM NZGSS_full_IDs gss
/*			Filter applications where GSS PQ respondant is in the application */
			INNER JOIN [trim_nah] nah
				ON gss.snz_uid_pq = nah.snz_uid
			inner join [trim_na] na
					ON (na.[snz_application_uid] = nah.[snz_application_uid]
					OR na.[snz_legacy_application_uid] = nah.[snz_legacy_application_uid]
					OR na.[snz_msd_application_uid] = nah.[snz_msd_application_uid] )
/*			Get the application exit status */
			LEFT JOIN [trim_re] re 
				ON na.[snz_application_uid] = re.[snz_application_uid]
				OR na.[snz_legacy_application_uid] = re.[snz_legacy_application_uid]
				OR na.[snz_msd_application_uid] = re.[snz_msd_application_uid] 
/*			Get the tenancy entry date if any after the application date*/
			left join [trim_ts] ts
				ON (ts.snz_uid = na.snz_uid AND ts.snz_hnz_ts_house_uid = re.snz_house_uid)
				OR (ts.snz_uid = na.snz_uid AND ts.snz_hnz_ts_legacy_house_uid = re.snz_legacy_house_uid)
				OR (ts.snz_uid = na.snz_uid AND ts.snz_msd_house_uid = re.snz_msd_house_uid)
			where datediff(d,na.[hnz_na_date_of_application_date],ts.[house_entry_date]) IS NULL
				or datediff(d,na.[hnz_na_date_of_application_date],ts.[house_entry_date]) >=0

			;

			);
	disconnect from odbc;
quit;

/*deduping to keep only one individual, if hnz_prim=1 then the hnz_prim applicant is in the GSS hhld (=snz_uid, not necessarly the gss_pq)*/
proc sort data= _temp_of_hnz_gss_population out=_temp_of_hnz_gss_population2;
 by snz_application_uid snz_legacy_application_uid snz_msd_application_uid descending hnz_prim ;
run;

proc sort data= _temp_of_hnz_gss_population2 out=_temp_of_hnz_gss_population3 nodupkey;
 by snz_application_uid snz_legacy_application_uid snz_msd_application_uid;
run;



/*Unique applications at that point = 3936*/
%si_write_to_db(si_write_table_in=work._temp_of_hnz_gss_population3,
	si_write_table_out=&si_sandpit_libname..of_hnz_gss_applications_sh3
	,si_cluster_index_flag=True,si_index_cols=%bquote(snz_uid)
	);

proc sql;
	select count(distinct catx(put(snz_application_uid, 20.), '_',put(snz_legacy_application_uid,20.),'_', put(snz_msd_application_uid,20.))) as count_apps
	from _temp_of_hnz_gss_population3;
quit;


/*Now filtering housed population in the time window and getting tenancy exit dates for each house*/
proc sql;
	connect to odbc (dsn=&idi_version._srvprd);
	create table _temp_of_hnz_gss_population4 as 
		select * from connection to odbc (
			with te as (
				SELECT distinct [snz_household_uid]
				  ,[snz_legacy_household_uid]
				  ,[snz_msd_application_uid]
				  ,[snz_msd_house_uid]
				  ,[hnz_te_exit_date]
				  ,[hnz_te_exit_status_text]
				FROM [IDI_Clean_20181020].[hnz_clean].[tenancy_exit]
				WHERE [hnz_te_exit_status_text] = 'EXIT ALL SOCIAL HOUSING'
			),
			hnz as(
			select distinct *,
				row_number() over(partition by snz_uid_pq order by snz_uid_pq, hnz_na_date_of_application_date) as app_number
			from [IDI_Sandpit].[DL-MAA2016-15].[of_hnz_gss_applications_sh3]
			where months_interview_to_entry between -12 and 15 and hnz_re_exit_status_text='HOUSED'
			)
			select distinct hnz.*
							,te.hnz_te_exit_date
							,te.hnz_te_exit_status_text
			from hnz	
			left join te
				ON (te.[snz_household_uid] = hnz.[snz_household_uid]) /* hnz.[snz_hnz_ts_house_uid]) */
				OR (te.[snz_legacy_household_uid] =  hnz.[snz_legacy_household_uid] ) /* hnz.[snz_hnz_ts_legacy_house_uid]) */
/*				OR (te.[snz_msd_house_uid] = hnz.[snz_msd_house_uid])*/

/*				ON (te.snz_household_uid = hnz.[snz_house_uid])*/
/*				OR (te.snz_legacy_household_uid = hnz.[snz_legacy_house_uid])*/
/*				OR (te.snz_msd_house_uid = hnz.[snz_msd_house_uid])*/
/*			Only the first HOUSED application in the time window*/
			where app_number=1
/*			and not( hnz.treat_control='TREATED' and te.hnz_te_exit_date between hnz.hnz_re_exit_date and hnz.as_at_date);*/
/*			No exit date between housed and interview */
/*			where (not (hnz.treat_control='TREATED' and te.hnz_te_exit_date between hnz.hnz_re_exit_date and hnz.as_at_date))*/
/*			where te.hnz_te_exit_date > hnz.hnz_re_exit_date or te.hnz_te_exit_status_text is NULL*/

/*			and hnz.hnz_re_exit_status_text='HOUSED'*/
/*			Window of interrest*/
/*			hnz.months_interview_to_entry between -12 and 15 and hnz.hnz_re_exit_status_text='HOUSED'*/

    		;

			);
	disconnect from odbc;
quit;




/*remove cases with tenacy exit before interview*/
proc sql;
	create table _temp_of_hnz_gss_population5 as 
	select *
	from _temp_of_hnz_gss_population4
	where not ((hnz_te_exit_date between hnz_re_exit_date and as_at_date) and treat_control='TREATED' );
quit;

/*No exit dates before the interview*/
data _temp_of_hnz_gss_population6;
	set _temp_of_hnz_gss_population5;
	if hnz_te_exit_date < hnz_re_exit_date then do; hnz_te_exit_date="31DEC2099:00:00:00.000"dt; hnz_te_exit_status_text=''; end;
run;


/*deduping by tenancy exit date */
proc sort data= _temp_of_hnz_gss_population6 out=_temp_of_hnz_gss_population7;
 by snz_application_uid snz_legacy_application_uid snz_msd_application_uid hnz_te_exit_date ;
run;

proc sort data= _temp_of_hnz_gss_population7 out=_temp_of_hnz_gss_population8 nodupkey;
 by snz_application_uid snz_legacy_application_uid snz_msd_application_uid;
run;

/*count*/
proc sql;
	select count(distinct catx(put(snz_application_uid, 20.), '_',put(snz_legacy_application_uid,20.),'_', put(snz_msd_application_uid,20.))) as count_apps
	from _temp_of_hnz_gss_population8;
quit;

/*Push to the DB*/
%si_write_to_db(si_write_table_in=work._temp_of_hnz_gss_population8,
	si_write_table_out=&si_sandpit_libname..of_hnz_gss_population_sh3
	,si_cluster_index_flag=True,si_index_cols=%bquote(snz_uid)
	);


/* Delete temporary datasets*/
proc datasets lib=work;
	delete _temp_: ;
run;


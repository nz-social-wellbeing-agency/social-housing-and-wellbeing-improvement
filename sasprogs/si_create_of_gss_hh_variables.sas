/*********************************************************************************************************
DESCRIPTION: 
Combines all the GSS household information across different waves into one single 
table. Only a limited set of variables which are useful for the outcomes framework / Social Housing 3 project have 
been retained in the output.
The final step is the spine linkage improvement (snz_uid replacement), following Craig Wright's Methodology

INPUT:
[&idi_version.].[gss_clean].[gss_household] = 2016 GSS household table
[&idi_version.].[gss_clean].[gss_household] = 2014 GSS household table
[&idi_version.].[gss_clean].[gss_household_2012] = 2012 GSS household table
[&idi_version.].[gss_clean].[gss_household_2010] = 2010 GSS household table
[&idi_version.].[gss_clean].[gss_household_2008] = 2008 GSS household table

OUTPUT:
sand.of_gss_hh_variables_sh3 = dataset with household variables for GSS

AUTHOR: 
V Benny

DEPENDENCIES:
SAS Marco %si_improve_linkage() and sand.gsswl_match_IDI_Clean_20181020 dataset

NOTES:   
1. Individuals in the GSS households are not linked to the spine at the time of writing this code, except
	for those individuals who also answer the personal questionnaire. 
2. All GSS waves are available only from &idi_version._20171027 onwards.


HISTORY: 
22 Nov 2017 VB	Converted the SQL version into SAS.
11 Jun 2018	VB	Added new variable for number of dependent children at the household level for all waves
				Added a count variable for number of family/non-family nuclei in a GSS household.
01 Aug 2018 WJ	For each person - asnwering the questionaire - what is the family structure in terms of number of children and adults
				variable created is adult_pq_ct and child_pq_ct
10 Aug 2018 VB	Changed the adult_ind variable definition from above 15 years to >= 18 years.
19 Oct 2018 VB	Added a join to the previous IDI refresh (IDI_Clean_20180720) to estimate interview dates
				for GSS2016 as these are absent in the IDI_Clean_20181020 refresh. Check Issues below.
21 Nov 2018 WJ  Added the latest refresh info 
03 Dec 2018 BV  QA for Social Housing 3

ISSUES:


***********************************************************************************************************/

proc sql;

	connect to odbc (dsn=&si_idi_dsnname.);

	create table work._temp_of_gss_hh_variables as
	select
		*,
		dhms(input(gss_pq_interview_start_date,yymmdd10.), 0, 0, 0) as as_at_date format=datetime20.
	from connection to odbc(		
			select 
				hh.snz_uid
				,hh.snz_gss_uid
				,hh.snz_gss_hhld_uid
				,gss_hq_collection_code as gss_id_collection_code
				,cast(p.gss_pq_PQinterview_date as date) as [gss_pq_interview_start_date]
				,cast(p.gss_pq_HQinterview_date as date) as [gss_hq_interview_start_date]
				,case when pp.snz_uid is not NULL then 1 else 0 end as person
				,hh.gss_hq_sex_dev
				,hh.[gss_hq_birth_month_nbr]
				,hh.[gss_hq_birth_year_nbr]
				,hh.[gss_hq_regcouncil_dev]
				,hh.[gss_hq_under_15_dev]
				,hh.gss_hq_age_dev
				,case hh.[gss_hq_under_15_dev] when 'N' then 1 when 'Y' then 0 end as [adult_ind]
				,hhnucleus.family_nuclei_ct
				,hhnucleus.nonfamily_nuclei_ct
				,adult_pq_ct  as family_size_adult
				,child_pq_ct  as family_size_child
				,cast(hh.gss_hq_house_trust as smallint) as gss_hq_house_trust 
				,cast(hh.gss_hq_house_own as smallint) as gss_hq_house_own
				,cast(hh.gss_hq_house_pay_mort_code as smallint) as gss_hq_house_pay_mort_code
				,cast(hh.gss_hq_house_pay_rent_code as smallint) as gss_hq_house_pay_rent_code
				,hh.gss_hq_house_who_owns_code
				,hh.gss_hq_household_inc1_dev
				,hh.gss_hq_fam_num_depchild_nbr
				,hh.gss_hq_fam_num_indepchild_nbr
			from [&idi_version.].[gss_clean].[gss_household] hh
			inner join (select snz_gss_hhld_uid, coalesce([Y], 0) as family_nuclei_ct, coalesce([N], 0) as nonfamily_nuclei_ct 
						from 
							(
							select snz_gss_hhld_uid, gss_hq_fam_nuc_yn_ind, count(distinct gss_hq_nucleus_nbr) as count_nuclei
							from [&idi_version.].[gss_clean].[gss_household]
							group by 
								snz_gss_hhld_uid, gss_hq_fam_nuc_yn_ind
							) as inner_query
						pivot (
							sum(count_nuclei)
							for
							gss_hq_fam_nuc_yn_ind in ([Y], [N])
							) as pivot_query
						) hhnucleus
						on (hh.snz_gss_hhld_uid = hhnucleus.snz_gss_hhld_uid)
			inner join [&idi_version.].[gss_clean].[gss_person] p on (hh.snz_gss_hhld_uid = p.snz_gss_hhld_uid) /* Removing hhld_uids not in the person ds */
			left join [&idi_version.].[gss_clean].[gss_person] pp on (hh.snz_uid = pp.snz_uid)
			left join(
			  		select bb.snz_gss_hhld_uid, bb.gss_hq_nucleus_nbr, sum(adult) as adult_pq_ct, sum(child) as child_pq_ct  
					from
							( 
							select	
								snz_gss_hhld_uid
								,gss_hq_nucleus_nbr
								,case when gss_hq_age_dev >= 18 then 1 else 0 end as adult
								,case when gss_hq_age_dev < 18 then 1 else 0 end as child
							from [&idi_version.].[gss_clean].[gss_household] ) bb
							group by  bb.snz_gss_hhld_uid ,bb.gss_hq_nucleus_nbr ) j   
					on (hh.snz_gss_hhld_uid = j.snz_gss_hhld_uid and hh.gss_hq_nucleus_nbr = j.gss_hq_nucleus_nbr)
			where gss_hq_collection_code = 'GSS2016'
			union all
			select 
				hh.snz_uid
				,hh.snz_gss_uid
				,hh.snz_gss_hhld_uid
				,gss_hq_collection_code as gss_id_collection_code
				,cast(p.gss_pq_PQinterview_date as date) as [gss_pq_interview_start_date]
				,cast(p.gss_pq_HQinterview_date as date) as [gss_hq_interview_start_date]
				,case when pp.snz_uid is not NULL then 1 else 0 end as person
				,hh.gss_hq_sex_dev
				,hh.[gss_hq_birth_month_nbr]
				,hh.[gss_hq_birth_year_nbr]
				,[gss_hq_regcouncil_dev]
				,[gss_hq_under_15_dev]
				,gss_hq_age_dev
				,case hh.[gss_hq_under_15_dev] when 'N' then 1 when 'Y' then 0 end as [adult_ind]
				,hhnucleus.family_nuclei_ct
				,hhnucleus.nonfamily_nuclei_ct
				,adult_pq_ct  as family_size_adult
				,child_pq_ct  as family_size_child
				,cast(hh.gss_hq_house_trust as smallint) as gss_hq_house_trust 
				,cast(hh.gss_hq_house_own as smallint) as gss_hq_house_own
				,cast(hh.gss_hq_house_pay_mort_code as smallint) as gss_hq_house_pay_mort_code
				,cast(hh.gss_hq_house_pay_rent_code as smallint) as gss_hq_house_pay_rent_code
				,hh.gss_hq_house_who_owns_code
				,hh.gss_hq_household_inc1_dev
				,hh.gss_hq_fam_num_depchild_nbr
				,hh.gss_hq_fam_num_indepchild_nbr
			from [&idi_version.].[gss_clean].[gss_household] hh
			inner join (select snz_gss_hhld_uid, coalesce([Y], 0) as family_nuclei_ct, coalesce([N], 0) as nonfamily_nuclei_ct 
						from 
							(
							select snz_gss_hhld_uid, gss_hq_fam_nuc_yn_ind, count(distinct gss_hq_nucleus_nbr) as count_nuclei
							from [&idi_version.].[gss_clean].[gss_household]
							group by 
								snz_gss_hhld_uid, gss_hq_fam_nuc_yn_ind
							) as inner_query
						pivot (
							sum(count_nuclei)
							for
							gss_hq_fam_nuc_yn_ind in ([Y], [N])
							) as pivot_query
						) hhnucleus
						on (hh.snz_gss_hhld_uid = hhnucleus.snz_gss_hhld_uid)
			inner join [&idi_version.].[gss_clean].[gss_person] p on (hh.snz_gss_hhld_uid = p.snz_gss_hhld_uid)
			left join [&idi_version.].[gss_clean].[gss_person] pp on (hh.snz_uid = pp.snz_uid)
			left join(
			  		select bb.snz_gss_hhld_uid, bb.gss_hq_nucleus_nbr, sum(adult) as adult_pq_ct, sum(child) as child_pq_ct  
					from
							( 
							select	
								snz_gss_hhld_uid
								,gss_hq_nucleus_nbr
								,case when gss_hq_age_dev >= 18 then 1 else 0 end as adult
								,case when gss_hq_age_dev < 18 then 1 else 0 end as child
							from [&idi_version.].[gss_clean].[gss_household] ) bb
							group by  bb.snz_gss_hhld_uid ,bb.gss_hq_nucleus_nbr ) j   
					on (hh.snz_gss_hhld_uid = j.snz_gss_hhld_uid and hh.gss_hq_nucleus_nbr = j.gss_hq_nucleus_nbr)
			where gss_hq_collection_code = 'GSS2014'
			union all
			select 
				hh.snz_uid
				,hh.snz_gss_uid
				,hh.snz_gss_hhld_uid
				,hh.gss_hq_collection_code as gss_id_collection_code
				,cast(coalesce(p.[gss_pq_interview_start_date],p.[gss_pq_Period_start_date]) as date) as [gss_pq_interview_start_date]
				,cast([gss_hq_interview_start_date] as date) as [gss_hq_interview_start_date]
				,case when pp.snz_uid is not NULL then 1 else 0 end as person
				,case [gss_hq_CORDV10] when '11' then 1 when '12' then 2 else NULL end as [gss_hq_CORDV10] 
				,[gss_hq_birth_month_nbr]
				,[gss_hq_birth_year_nbr]
				,null as [gss_hq_regcouncil_dev]
				,[gss_hq_Under15_DV]
				,[gss_hq_CORDV9] as gss_hq_age_dev
				,case hh.[gss_hq_Under15_DV] when 'N' then 1 when 'Y' then 0 end as [adult_ind]
				,NULL as family_nuclei_ct
				,NULL as nonfamily_nuclei_ct
				,NULL  as family_size_adult
				,NULL  as family_size_child
				,house.gss_hq_CORHQ05 as gss_hq_house_trust 
				,house.gss_hq_CORHQ07  as gss_hq_house_own
				,house.gss_hq_CORHQ08 as gss_hq_house_pay_mort_code
				,house.gss_hq_CORHQ10 as  gss_hq_house_pay_rent_code
				,house.gss_hq_CORHQ09 as gss_hq_house_who_owns_code
				,hh.gss_hq_CORDV13 as gss_hq_household_inc1_dev
				,NULL as gss_hq_fam_num_depchild_nbr
				,NULL as gss_hq_fam_num_indepchild_nbr
			from [&idi_version.].[gss_clean].[gss_household_2012] hh
			left join (select snz_gss_hhld_uid
						,max(gss_hq_CORHQ05) as gss_hq_CORHQ05
						,max(gss_hq_CORHQ07) as gss_hq_CORHQ07
						,max(gss_hq_CORHQ08) as gss_hq_CORHQ08
						,max(gss_hq_CORHQ10) as gss_hq_CORHQ10
						,max(gss_hq_CORHQ09) as gss_hq_CORHQ09
					from [&idi_version.].[gss_clean].[gss_household_2012]
					group by snz_gss_hhld_uid) house on ( hh.snz_gss_hhld_uid = house.snz_gss_hhld_uid)
			inner join [&idi_version.].[gss_clean].[gss_person_2012] p on (hh.snz_gss_hhld_uid = p.snz_gss_hhld_uid)
			/* Funfact: GSS refresh dated 20181020 has duplicates in the GSS household table for 2008. It isn't clear why this occurs
					or how to get unique records, so for now, as an interim fix, we have used the gss_hq_PersonCoreNonRespInd
					column as a determiner of unique records. This may need to change with future refreshes. The following join
					takes care of the de-duplication. You can remove this join once the duplicate issue is fixed on STATSNZ end.
					*/
			left join [&idi_version.].[gss_clean].[gss_person_2012] pp on (hh.snz_uid = pp.snz_uid)
			inner join (select snz_uid, gss_hq_collection_code, coalesce(max(gss_hq_PersonCoreNonRespInd), '') as gss_hq_PersonCoreNonRespInd 
						from [&idi_version.].gss_clean.gss_household_2012 
						group by snz_uid, gss_hq_collection_code) dupremove
				on (hh.snz_uid = dupremove.snz_uid 
				and coalesce(hh.gss_hq_PersonCoreNonRespInd, '') = dupremove.gss_hq_PersonCoreNonRespInd )

			union all

			select 
				hh.snz_uid
				,hh.snz_gss_uid
				,hh.snz_gss_hhld_uid
				,gss_hq_collection_code as gss_id_collection_code
				,cast(coalesce(p.[gss_pq_interview_start_date],p.[gss_pq_Period_start_date]) as date) as [gss_pq_interview_start_date]
				,cast([gss_hq_interview_start_date] as date) as [gss_hq_interview_start_date]
				,case when pp.snz_uid is not NULL then 1 else 0 end as person
				,case [gss_hq_CORDV10] when '11' then 1 when '12' then 2 else NULL end as [gss_hq_CORDV10]
				,[gss_hq_birth_month_nbr]
				,[gss_hq_birth_year_nbr]
				,null as [gss_hq_regcouncil_dev]
				,[gss_hq_Under15_DV]
				,[gss_hq_CORDV9] as gss_hq_age_dev
				,case hh.[gss_hq_Under15_DV] when 'N' then 1 when 'Y' then 0 end as [adult_ind]
				,NULL as family_nuclei_ct
				,NULL as nonfamily_nuclei_ct
				,NULL  as family_size_adult
				,NULL  as family_size_child
				,house.gss_hq_CORHQ05 as gss_hq_house_trust 
				,house.gss_hq_CORHQ07 as gss_hq_house_own
				,house.gss_hq_CORHQ08 as gss_hq_house_pay_mort_code
				,house.gss_hq_CORHQ10 as  gss_hq_house_pay_rent_code
				,house.gss_hq_CORHQ09 as gss_hq_house_who_owns_code
				,hh.gss_hq_CORDV13 as gss_hq_household_inc1_dev
				,NULL as gss_hq_fam_num_depchild_nbr
				,NULL as gss_hq_fam_num_indepchild_nbr
			from [&idi_version.].[gss_clean].[gss_household_2010] hh
			left join (select snz_gss_hhld_uid
						,max(gss_hq_CORHQ05) as gss_hq_CORHQ05
						,max(gss_hq_CORHQ07) as gss_hq_CORHQ07
						,max(gss_hq_CORHQ08) as gss_hq_CORHQ08
						,max(gss_hq_CORHQ10) as gss_hq_CORHQ10
						,max(gss_hq_CORHQ09) as gss_hq_CORHQ09
					from [&idi_version.].[gss_clean].[gss_household_2010]
					group by snz_gss_hhld_uid) house on ( hh.snz_gss_hhld_uid = house.snz_gss_hhld_uid)
			inner join [&idi_version.].[gss_clean].[gss_person_2010] p on (hh.snz_gss_hhld_uid = p.snz_gss_hhld_uid)
			left join [&idi_version.].[gss_clean].[gss_person_2010] pp on (hh.snz_uid = pp.snz_uid)

			union all

			select 
				hh.snz_uid
				,hh.snz_gss_uid
				,hh.snz_gss_hhld_uid
				,hh.gss_hq_collection_code as gss_id_collection_code
				,cast(coalesce(p.[gss_pq_interview_start_date],p.[gss_pq_Period_start_date]) as date) as [gss_pq_interview_start_date]
				,cast([gss_hq_interview_start_date] as date) as [gss_hq_interview_start_date]
				,case when pp.snz_uid is not NULL then 1 else 0 end as person
				,case [gss_hq_CORDV10] when '11' then 1 when '12' then 2 else NULL end as [gss_hq_CORDV10]
				,[gss_hq_birth_month_nbr]
				,[gss_hq_birth_year_nbr]
				,null as [gss_hq_regcouncil_dev]
				,[gss_hq_Under15_DV]
				,[gss_hq_CORDV9] as gss_hq_age_dev
				,case hh.[gss_hq_Under15_DV] when 'N' then 1 when 'Y' then 0 end as [adult_ind]
				,NULL as family_nuclei_ct
				,NULL as nonfamily_nuclei_ct
				,NULL  as family_size_adult
				,NULL  as family_size_child
				,house.gss_hq_CORHQ05 as gss_hq_house_trust 
				,house.gss_hq_CORHQ07 as gss_hq_house_own
				,house.gss_hq_CORHQ08 as gss_hq_house_pay_mort_code
				,house.gss_hq_CORHQ10 as  gss_hq_house_pay_rent_code
				,house.gss_hq_CORHQ09 as gss_hq_house_who_owns_code
				,hh.gss_hq_CORDV13 as gss_hq_household_inc1_dev
				,NULL as gss_hq_fam_num_depchild_nbr
				,NULL as gss_hq_fam_num_indepchild_nbr
			from [&idi_version.].[gss_clean].[gss_household_2008] hh
			left join (select snz_gss_hhld_uid
						,max(gss_hq_CORHQ05) as gss_hq_CORHQ05
						,max(gss_hq_CORHQ07) as gss_hq_CORHQ07
						,max(gss_hq_CORHQ08) as gss_hq_CORHQ08
						,max(gss_hq_CORHQ10) as gss_hq_CORHQ10
						,max(gss_hq_CORHQ09) as gss_hq_CORHQ09
					from [&idi_version.].[gss_clean].[gss_household_2008]
					group by snz_gss_hhld_uid) house on ( hh.snz_gss_hhld_uid = house.snz_gss_hhld_uid)
			inner join [&idi_version.].[gss_clean].[gss_person_2008] p on (hh.snz_gss_hhld_uid = p.snz_gss_hhld_uid)
			/* Funfact: GSS refresh dated 20181020 has duplicates in the GSS household table for 2008. It isn't clear why this occurs
					or how to get unique records, so for now, as an interim fix, we have used the gss_hq_PersonCoreNonRespInd
					column as a determiner of unique records. This may need to change with future refreshes. The following join
					takes care of the de-duplication. You can remove this join once the duplicate issue is fixed on STATSNZ end.
					*/
			left join [&idi_version.].[gss_clean].[gss_person_2008] pp on (hh.snz_uid = pp.snz_uid)
			inner join (select snz_uid, gss_hq_collection_code, coalesce(max(gss_hq_PersonCoreNonRespInd), '') as gss_hq_PersonCoreNonRespInd 
						from [&idi_version.].gss_clean.gss_household_2008 
						group by snz_uid, gss_hq_collection_code) dupremove
				on (hh.snz_uid = dupremove.snz_uid 
				and coalesce(hh.gss_hq_PersonCoreNonRespInd, '') = dupremove.gss_hq_PersonCoreNonRespInd )
	
	);

	disconnect from odbc;

quit;


/* Push the cohort into the database*/
%si_write_to_db(si_write_table_in=work._temp_of_gss_hh_variables,
	si_write_table_out=&si_sandpit_libname..of_gss_hh_variables_sh3
	,si_cluster_index_flag=True,si_index_cols=%bquote(&si_id_col.)
	);

/*Linkage improvement*/
%si_improve_gss_linkage(si_table_in=of_gss_hh_variables_sh3, 
							  si_table_out=of_gss_hh_variables_sh3, 
							  si_table_match=gsswl_match_IDI_Clean_20181020, 
							  unlinked_only=True, 
							  collection_code=gss_id_collection_code);


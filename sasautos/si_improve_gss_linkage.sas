/*********************************************************************************************************
DESCRIPTION: 
Improves the linkage rate by snz_uid replacement (from matching table)
Contains  snz_uid (new to consider) - snz_uid_original (old)

INPUT:
si_table_in= sandpit input table with snz_uid
si_table_match= match dataset (with snz_uid and b_snz_uid for replacement)
unlinked_only= Flag to replace only unlinked snz_uid, or false to consider everybody in the input table
collection_code= GSS collection code variable


OUTPUT:
si_table_out= output sandpit table with replaced snz_uid and snz_spine_ind before replacement

AUTHOR: 
B Vandenbroucke

DEPENDENCIES:
Uses the match dataset version created by Craig Wright (STEP_0/1/2 in ./include)
[IDI_Sandpit].[&si_proj_schema.].[gsswl_match_&idi_version.]

NOTES:   


HISTORY: 
20 Nov 2018 BV V1
26 Nov 2018 WJ fixed small bug duplicating some snz_uid
27 Nov 2018 BV Macro-ised

***********************************************************************************************************/

/*Replacing some unlinked snz_uid from the gss_hh by newly matched snz_uids from Craigs method*/

%macro si_improve_gss_linkage(si_table_in=, si_table_out=, si_table_match=, unlinked_only=True, collection_code=);

proc sql;

	connect to odbc (dsn=&si_idi_dsnname.);

	create table work._temp_matching as
	select *
	from connection to odbc(
		
	with match as(
		select
			case when hh.spine_ind_b4 =1 then hh.snz_uid else coalesce(m.b_snz_uid, hh.snz_uid) end as snz_uid_new
			,hh.*
			from 

				(select h.*, p.snz_spine_ind as spine_ind_b4 from [IDI_Sandpit].[&si_proj_schema.].[&si_table_in.] h
					left join [&idi_version.].[data].[personal_detail] p 
						on h.snz_uid=p.snz_uid 
				) hh
			left join [IDI_Sandpit].[&si_proj_schema.].[&si_table_match.] m 
				on hh.snz_uid=m.snz_uid and hh.&collection_code.=m.gss_hq_collection_code
			)
			select match.*
				, pp.snz_spine_ind as spine_ind_af
			from match
			left join [&idi_version.].[data].[personal_detail] pp
					on pp.snz_uid=match.snz_uid_new

				
		);

	disconnect from odbc;

quit;

/*renaming snz_uid*/
data _temp_matching2;
	set _temp_matching;
	rename snz_uid_new=snz_uid;
	rename snz_uid=snz_uid_original;
run;



/* Push the cohort into the database*/
%si_write_to_db(si_write_table_in=work._temp_matching2,
	si_write_table_out=&si_sandpit_libname..&si_table_out.
	,si_cluster_index_flag=True,si_index_cols=%bquote(snz_uid)
	);

/* Remove temporary datasets */
proc datasets lib=work;
	delete _temp_: ;
run;

%mend si_improve_gss_linkage ;


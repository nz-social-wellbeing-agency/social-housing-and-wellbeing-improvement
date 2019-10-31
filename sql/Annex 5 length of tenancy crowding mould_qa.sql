/* Coalesce tenancy table to get non-null entry date*/
with ts_roll as (select distinct snz_uid
, snz_household_uid
, snz_msd_house_uid
, snz_legacy_household_uid
, coalesce(hnz_ts_house_entry_date, msd_tenancy_start_date) as ten_start_date
, snz_hnz_ts_house_uid
, snz_hnz_ts_legacy_house_uid from [IDI_Clean_20181020].hnz_clean.tenancy_snapshot
-- and snz_legacy_household_uid is not null;
), 

/* Coalesce cells with null values*/
ts as (
select snz_uid, ten_start_date,
max(snz_household_uid) as snz_household_uid,
max(snz_legacy_household_uid) as snz_legacy_household_uid, 
max(snz_hnz_ts_house_uid) as snz_hnz_ts_house_uid,
max(snz_msd_house_uid) as snz_msd_house_uid,
max(snz_hnz_ts_legacy_house_uid) as snz_hnz_ts_legacy_house_uid from ts_roll
group by snz_uid, ten_start_date)

/* Get the two SNZ UIDs: housing applicant and GSS respondent */
, gss_app as (select distinct 
snz_uid_pq
, snz_uid_hnz_prim
, treat_control
, gss_pq_interview_start_date
from [IDI_Sandpit].[DL-MAA2016-15].[of_hnz_gss_applications_sh3])

/* Get Housing variables */
, gss_house as (select distinct 
snz_uid
 , gss_pq_house_mold_code
 , cast(case when gss_pq_HH_crowd_code in (1, 2) then 1 else 0 end as numeric) as house_crowding_ind 
from [IDI_Sandpit].[DL-MAA2016-15].[of_gss_ind_variables_sh3])
,te as (SELECT *
				FROM [IDI_Clean_20181020].[hnz_clean].[tenancy_exit]
				WHERE [hnz_te_exit_status_text] = 'EXIT ALL SOCIAL HOUSING'
)
/* Bring sub-queries together */
, final_table as(
select distinct ts.snz_uid
, ga.snz_uid_pq
, ga.gss_pq_interview_start_date
, ts.snz_household_uid
, ts.snz_legacy_household_uid
, snz_hnz_ts_house_uid
, snz_hnz_ts_legacy_house_uid
, ten_start_date
 , CASE WHEN hnz_te_exit_date IS NULL then DATEFROMPARTS(2099, 12, 01) ELSE hnz_te_exit_date END as hnz_te_exit_date
, gh.gss_pq_house_mold_code
, gh.house_crowding_ind
, datediff(m, ten_start_date, ga.gss_pq_interview_start_date) as months_interview_to_entry
from ts 
left join te
on te.snz_household_uid = ts.snz_household_uid or te.snz_legacy_household_uid = ts.snz_legacy_household_uid
left join [IDI_Clean_20181020].hnz_clean.register_exit re
on re.snz_house_uid = ts.snz_hnz_ts_house_uid or re.snz_legacy_house_uid = ts.snz_hnz_ts_legacy_house_uid
left join [IDI_Clean_20181020].hnz_clean.new_applications na
				ON (ts.snz_uid = na.snz_uid AND ts.snz_hnz_ts_house_uid = re.snz_house_uid)
				OR (ts.snz_uid = na.snz_uid AND ts.snz_hnz_ts_legacy_house_uid = re.snz_legacy_house_uid)
				OR (ts.snz_uid = na.snz_uid AND ts.snz_msd_house_uid = re.snz_msd_house_uid)
inner join gss_app ga on ga.snz_uid_hnz_prim = ts.snz_uid
inner join gss_house gh on gh.snz_uid = ga.snz_uid_pq
--order by snz_uid, ten_start_date

)

/* aggregation to get a single tenancy exit per tenancy spell*/ 
, exit_agg_table as (
select snz_uid_pq
, gss_pq_interview_start_date
, snz_household_uid
, snz_legacy_household_uid
, ten_start_date
, min(hnz_te_exit_date) as hnz_te_exit_date
, gss_pq_house_mold_code
, house_crowding_ind
, months_interview_to_entry
from final_table
group by snz_uid_pq, gss_pq_interview_start_date, snz_household_uid, snz_legacy_household_uid, ten_start_date
, gss_pq_house_mold_code, house_crowding_ind, months_interview_to_entry
)

/* Cut months from entry to interview into 7 level factor */
, agg_table as(
SELECT distinct snz_uid_pq
, gss_pq_interview_start_date
, ten_start_date
, hnz_te_exit_date
, gss_pq_house_mold_code
, house_crowding_ind
, months_interview_to_entry
, case when abs(eat.months_interview_to_entry) <= 6 then '1. 0 - 6 months'
			when abs(eat.months_interview_to_entry) > 6 and abs(eat.months_interview_to_entry) <= 12 then '2. 7 - 12 months'
			when abs(eat.months_interview_to_entry) > 12 and abs(eat.months_interview_to_entry) <= 18 then '3. 13 - 18 months'
			when abs(eat.months_interview_to_entry) > 18 and abs(eat.months_interview_to_entry) <= 24 then '4. 19 - 24 months'
			when abs(eat.months_interview_to_entry) > 24 and abs(eat.months_interview_to_entry) <= 48 then '5. 25 - 48 months'
			when abs(eat.months_interview_to_entry) > 48 and abs(eat.months_interview_to_entry) <= 72 then '6. 49 - 72 months'
			when abs(eat.months_interview_to_entry) >72 then '7. > 72 months'
			end as length_from_interview from exit_agg_table eat
where gss_pq_interview_start_date between ten_start_date and hnz_te_exit_date
)

/* Get single overlapping spell */
, one_overlapping_spell as(
SELECT distinct snz_uid_pq
, min(months_interview_to_entry) as months_interview_to_entry
from agg_table
group by snz_uid_pq
)

/* Aggregate for results */
select length_from_interview
, avg(gss_pq_house_mold_code) as Mould
, avg(house_crowding_ind) as crowding
, count(*) as count from agg_table at
inner join one_overlapping_spell oos on oos.snz_uid_pq = at.snz_uid_pq 
									 AND oos.months_interview_to_entry = at.months_interview_to_entry
group by length_from_interview
order by length_from_interview



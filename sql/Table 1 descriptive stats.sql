/* HNZ housing use summary statistics for Outcomes Framework Paper			
16/11/2017			
Wen Jhe Lee		
			
		
*/			
select  year([hnz_na_date_of_application_date]), count(*) from (
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
				FROM [IDI_Clean_20181020].[hnz_clean].[new_applications] ) t group by year([hnz_na_date_of_application_date]) order by year([hnz_na_date_of_application_date])

/* Outputing  Application resulting in placement in social shousing */


				select  year(hnz_re_exit_date), count(distinct concat(snz_application_uid
				,snz_legacy_application_uid
				,snz_msd_application_uid)) from [IDI_Clean_20181020].[hnz_clean].[register_exit] WHERE [hnz_re_exit_status_text]='HOUSED' group by year(hnz_re_exit_date) 




/* Outputing  Application TOTAL NUMBER OF PEOPLE IN HSE HOLD resulting in placement in social shousing */

				select  year([hnz_na_date_of_application_date]), SUM(hnz_na_hshd_size_nbr) from (
SELECT DISTINCT [snz_uid]
						,[hnz_na_date_of_application_date]
						,[hnz_na_hshd_size_nbr]
						,'new application' AS hnz_apply_type
				FROM [IDI_Clean_20181020].[hnz_clean].[new_applications] ) t group by year([hnz_na_date_of_application_date]) order by year([hnz_na_date_of_application_date])


				
/* Outputing  Application TOTAL NUMBER OF PEOPLE IN HSE HOLD resulting in placement in social shousing */
select  year(a.[hnz_na_date_of_application_date]) AS yr, SUM(a.hnz_na_hshd_size_nbr) as sum_cnt INTO [IDI_Sandpit].[DL-MAA2016-15].[total_house] from (
SELECT DISTINCT [snz_uid]
						,[hnz_na_date_of_application_date]
						,[hnz_na_hshd_size_nbr]
						,A.[snz_application_uid]
						,A.[snz_legacy_application_uid]
						,A.[snz_msd_application_uid]
				FROM [IDI_Clean_20181020].[hnz_clean].[new_applications] A
				INNER JOIN (select 
						[snz_application_uid]
						,[snz_legacy_application_uid]
						,[snz_msd_application_uid]
				 from [IDI_Clean_20181020].[hnz_clean].[register_exit] WHERE [hnz_re_exit_status_text]='HOUSED') B
			ON (coalesce(A.snz_application_uid, -1)  = coalesce(B.snz_application_uid, -1)
			OR coalesce(A.snz_legacy_application_uid, -1)  = coalesce(B.snz_legacy_application_uid, -1)
			OR A.[snz_msd_application_uid] = B.[snz_msd_application_uid])) a GROUP BY  year([hnz_na_date_of_application_date])
			
			

/****** applications for the GSS population by year  ******/
SELECT year([hnz_na_date_of_application_date]) as application_year
      ,count(concat(na.[snz_application_uid]
	  ,na.[snz_legacy_application_uid]
      ,na.[snz_msd_application_uid])) as n_applications
     
  FROM [IDI_Clean_20181020].[hnz_clean].[new_applications] na
  inner join [IDI_Sandpit].[DL-MAA2016-15].[of_gss_ind_variables_sh3] gss on na.snz_uid=gss.snz_uid
  group by year([hnz_na_date_of_application_date])
  order by year([hnz_na_date_of_application_date])


-- by wave
  SELECT gss.gss_id_collection_code
      ,count(concat(na.[snz_application_uid]
	  ,na.[snz_legacy_application_uid]
      ,na.[snz_msd_application_uid])) as n_applications
     
  FROM [IDI_Clean_20181020].[hnz_clean].[new_applications] na
  inner join [IDI_Sandpit].[DL-MAA2016-15].[of_gss_ind_variables_sh3] gss on na.snz_uid=gss.snz_uid
  where datediff(m,gss.[gss_pq_interview_date],na.[hnz_na_date_of_application_date]) between -12 and 15
  group by gss.gss_id_collection_code
  order by gss.gss_id_collection_code

-- for all GSS members

  SELECT gss.gss_id_collection_code
      ,count(concat(na.[snz_application_uid]
	  ,na.[snz_legacy_application_uid]
      ,na.[snz_msd_application_uid])) as n_applications
     
  FROM [IDI_Clean_20181020].[hnz_clean].[new_applications] na
  inner join [IDI_Sandpit].[DL-MAA2016-15].[of_gss_hh_variables_sh3] gss on na.snz_uid=gss.snz_uid
  where datediff(m,gss.[gss_hq_interview_start_date],na.[hnz_na_date_of_application_date]) between -12 and 15
  group by gss.gss_id_collection_code
  order by gss.gss_id_collection_code


-- by wave
  SELECT gss.gss_id_collection_code
      ,count(concat(na.[snz_application_uid]
	  ,na.[snz_legacy_application_uid]
      ,na.[snz_msd_application_uid])) as n_applications
     
  FROM [IDI_Clean_20181020].[hnz_clean].[new_applications] na
  inner join [IDI_Clean_20181020].[hnz_clean].[new_applications_household] nah
				ON na.[snz_application_uid] = nah.[snz_application_uid]
				OR na.[snz_legacy_application_uid] = nah.[snz_legacy_application_uid]
				OR na.[snz_msd_application_uid] = nah.[snz_msd_application_uid] 
  inner join [IDI_Sandpit].[DL-MAA2016-15].[of_gss_ind_variables_sh3] gss on nah.snz_uid=gss.snz_uid
  where datediff(m,gss.[gss_pq_interview_date],na.[hnz_na_date_of_application_date]) between -12 and 15
  group by gss.gss_id_collection_code
  order by gss.gss_id_collection_code


  /****** HOUSED applications for the GSS population by year  ******/
SELECT year([hnz_na_date_of_application_date]) as application_year
      ,count(concat(na.[snz_application_uid]
	  ,na.[snz_legacy_application_uid]
      ,na.[snz_msd_application_uid])) as n_applications
     
  FROM [IDI_Clean_20181020].[hnz_clean].[new_applications] na
  inner join  [IDI_Clean_20181020].[hnz_clean].[register_exit] re
				ON na.[snz_application_uid] = re.[snz_application_uid]
				OR na.[snz_legacy_application_uid] = re.[snz_legacy_application_uid]
				OR na.[snz_msd_application_uid] = re.[snz_msd_application_uid] 
  inner join [IDI_Sandpit].[DL-MAA2016-15].[of_gss_ind_variables_sh3] gss on na.snz_uid=gss.snz_uid
  where re.hnz_re_exit_status_text='HOUSED'
  group by year([hnz_na_date_of_application_date])
  order by year([hnz_na_date_of_application_date])


  /****** HOUSED applications for the GSS population by year, with HOUSED exit within 12 to 15 month from the itw  ******/
SELECT year([hnz_re_exit_date]) as re_year
      ,count(concat(na.[snz_application_uid]
	  ,na.[snz_legacy_application_uid]
      ,na.[snz_msd_application_uid])) as n_applications
     
  FROM [IDI_Clean_20181020].[hnz_clean].[new_applications] na
  inner join  [IDI_Clean_20181020].[hnz_clean].[register_exit] re
				ON na.[snz_application_uid] = re.[snz_application_uid]
				OR na.[snz_legacy_application_uid] = re.[snz_legacy_application_uid]
				OR na.[snz_msd_application_uid] = re.[snz_msd_application_uid] 
  inner join [IDI_Sandpit].[DL-MAA2016-15].[of_gss_ind_variables_sh3] gss on na.snz_uid=gss.snz_uid
  where re.hnz_re_exit_status_text='HOUSED' and datediff(m,gss.[gss_pq_interview_date],re.[hnz_re_exit_date]) between -12 and 15
  group by year([hnz_re_exit_date])
  order by year([hnz_re_exit_date])

  SELECT gss.gss_id_collection_code
      ,count(concat(na.[snz_application_uid]
	  ,na.[snz_legacy_application_uid]
      ,na.[snz_msd_application_uid])) as n_applications
     
  FROM [IDI_Clean_20181020].[hnz_clean].[new_applications] na
  inner join  [IDI_Clean_20181020].[hnz_clean].[register_exit] re
				ON na.[snz_application_uid] = re.[snz_application_uid]
				OR na.[snz_legacy_application_uid] = re.[snz_legacy_application_uid]
				OR na.[snz_msd_application_uid] = re.[snz_msd_application_uid] 
  inner join [IDI_Sandpit].[DL-MAA2016-15].[of_gss_ind_variables_sh3] gss on na.snz_uid=gss.snz_uid
  where re.hnz_re_exit_status_text='HOUSED' and datediff(m,gss.[gss_pq_interview_date],re.[hnz_re_exit_date]) between -12 and 15
  group by gss.gss_id_collection_code
  order by gss.gss_id_collection_code


-- Censoring 

  SELECT gss_id_collection_code
      ,min([gss_pq_interview_date]),max([gss_pq_interview_date])
  from [IDI_Sandpit].[DL-MAA2016-15].[of_gss_ind_variables_sh3] group by gss_id_collection_code order by gss_id_collection_code
 

  SELECT year([gss_pq_interview_date]),month([gss_pq_interview_date]), count(snz_uid)
  from [IDI_Sandpit].[DL-MAA2016-15].[of_gss_ind_variables_sh3] group by year([gss_pq_interview_date]),month([gss_pq_interview_date]) order by year([gss_pq_interview_date]),month([gss_pq_interview_date])

  -- register exit
 select max(re.hnz_re_exit_date)
 from [IDI_Clean_20181020].[hnz_clean].[register_exit] re

  select year(re.hnz_re_exit_date) as year, month(re.hnz_re_exit_date) as month, count(*)
 from [IDI_Clean_20181020].[hnz_clean].[register_exit] re 
 group by year(re.hnz_re_exit_date), month(re.hnz_re_exit_date)
  order by year(re.hnz_re_exit_date), month(re.hnz_re_exit_date)

  -- tenancy snapshot
SELEct max(coalesce([hnz_ts_house_entry_date],[msd_tenancy_start_date])) as max_house_entry_date
FROM [IDI_Clean_20181020].[hnz_clean].[tenancy_snapshot] where year(coalesce([hnz_ts_house_entry_date],[msd_tenancy_start_date])) <2100

SELEct year(coalesce([hnz_ts_house_entry_date],[msd_tenancy_start_date])) as year, month(coalesce([hnz_ts_house_entry_date],[msd_tenancy_start_date])) as month
, count(distinct concat(snz_uid, snz_hnz_ts_house_uid, snz_hnz_ts_legacy_house_uid, snz_msd_house_uid ))
FROM [IDI_Clean_20181020].[hnz_clean].[tenancy_snapshot] where year(coalesce([hnz_ts_house_entry_date],[msd_tenancy_start_date])) <2100
group by year(coalesce([hnz_ts_house_entry_date],[msd_tenancy_start_date])), month(coalesce([hnz_ts_house_entry_date],[msd_tenancy_start_date]))
order by year(coalesce([hnz_ts_house_entry_date],[msd_tenancy_start_date])), month(coalesce([hnz_ts_house_entry_date],[msd_tenancy_start_date]))
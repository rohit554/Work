delete from dgdm_{tenant}.mv_user_metrics a 
where a.metricDate >= date_sub(CAST('{extract_date}' AS DATE), 7)
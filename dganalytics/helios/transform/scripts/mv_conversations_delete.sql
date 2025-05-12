delete from dgdm_{tenant}.mv_conversations a 
where a.conversationStartDateId >= (select dateId from dgdm_{tenant}.dim_date where dateVal = date_sub(CAST('{extract_date}' AS DATE), 7))
DELETE FROM dgdm_{tenant}.{transformation} where conversationStartDateId > (select dateId from dgdm_{tenant}.dim_date where dateVal = cast(DATEADD(DAY, -3, '{extract_date}') as date))

select
  conversationId,
  index,
  menuId, 
  previousMenuId,
  n_nextSelectedMenuId nextSelectedMenuId,
  cast(menuEntryTime as timestamp) menuEntryTime,
  cast(menuExitTime as timestamp) as menuExitTime,
  menuExitReason,
  conversationStart, 
  conversationStartDateId
  from
(
select 
  conversationId,
  index,
  n_menuId menuId, 
  CASE WHEN SUBSTRING(previousMenuId, 1, 4) REGEXP '^[0-9]{4}$' AND LEN(previousMenuId) > 24 AND SUBSTRING(previousMenuId, 5, 1) = '-' AND SUBSTRING(previousMenuId, 8, 1) = '-' AND SUBSTRING(previousMenuId, 11, 1) = 'T' 
      THEN LTRIM(SUBSTRING(previousMenuId, 25, LEN(previousMenuId)))
      WHEN SUBSTRING(previousMenuId, 1, 4) REGEXP '^[A-Za-z]{4}$' AND LEN(previousMenuId) > 24 AND SUBSTRING(previousMenuId, len(previousMenuId)-19, 1) = '-' AND SUBSTRING(previousMenuId, len(previousMenuId)-16, 1) = '-' AND SUBSTRING(previousMenuId, len(previousMenuId)-13, 1) = 'T' THEN RTRIM(SUBSTRING(previousMenuId, 1, LEN(previousMenuId)-24))
    ELSE previousMenuId
    END AS previousMenuId,
  CASE WHEN SUBSTRING(nextSelectedMenuId, 1, 4) REGEXP '^[0-9]{4}$' AND LEN(nextSelectedMenuId) > 24 AND SUBSTRING(nextSelectedMenuId, 5, 1) = '-' AND SUBSTRING(nextSelectedMenuId, 8, 1) = '-' AND SUBSTRING(nextSelectedMenuId, 11, 1) = 'T' 
      THEN LTRIM(SUBSTRING(nextSelectedMenuId, 25, LEN(nextSelectedMenuId)))
      WHEN SUBSTRING(nextSelectedMenuId, 1, 4) REGEXP '^[A-Za-z]{4}$' AND LEN(nextSelectedMenuId) > 24 AND SUBSTRING(nextSelectedMenuId, len(nextSelectedMenuId)-19, 1) = '-' AND SUBSTRING(nextSelectedMenuId, len(nextSelectedMenuId)-16, 1) = '-' AND SUBSTRING(nextSelectedMenuId, len(nextSelectedMenuId)-13, 1) = 'T' THEN RTRIM(SUBSTRING(nextSelectedMenuId, 1, LEN(nextSelectedMenuId)-24))
    ELSE nextSelectedMenuId
    END AS n_nextSelectedMenuId,
  case when menuEntryTime is null then TIMESTAMPADD(MICROSECOND, 1000 * (index + 35), conversationStart) else menuEntryTime end as menuEntryTime, 
  CASE WHEN SUBSTRING(nextSelectedMenuId, 1, 4) REGEXP '^[0-9]{4}$' AND LEN(nextSelectedMenuId) > 24 AND SUBSTRING(nextSelectedMenuId, 5, 1) = '-' AND SUBSTRING(nextSelectedMenuId, 8, 1) = '-' AND SUBSTRING(nextSelectedMenuId, 11, 1) = 'T' 
          THEN SUBSTRING(nextSelectedMenuId, 1, 24)
      WHEN SUBSTRING(nextSelectedMenuId, 1, 4) REGEXP '^[A-Za-z]{4}$' AND LEN(nextSelectedMenuId) > 24 AND SUBSTRING(nextSelectedMenuId, len(nextSelectedMenuId)-19, 1) = '-' AND SUBSTRING(nextSelectedMenuId, len(nextSelectedMenuId)-16, 1) = '-' AND SUBSTRING(nextSelectedMenuId, len(nextSelectedMenuId)-13, 1) = 'T' THEN SUBSTRING(nextSelectedMenuId, len(nextSelectedMenuId)-23, len(nextSelectedMenuId))
        ELSE NULL
      END AS menuExitTime,
  null as menuExitReason,
  conversationStart, 
  conversationStartDateId
  from
(
  -- 19,16,13,10,7,
  select 
  conversationId,
  index+1 as index,
  CASE WHEN SUBSTRING(menuId, 1, 4) REGEXP '^[0-9]{4}$' AND LEN(menuId) > 24 AND SUBSTRING(menuId, 5, 1) = '-' AND SUBSTRING(menuId, 8, 1) = '-' AND SUBSTRING(menuId, 11, 1) = 'T' THEN SUBSTRING(menuId, 1, 24)
        WHEN SUBSTRING(menuId, 1, 4) REGEXP '^[A-Za-z]{4}$' AND LEN(menuId) > 24 AND SUBSTRING(menuId, len(menuId)-19, 1) = '-' AND SUBSTRING(menuId, len(menuId)-16, 1) = '-' AND SUBSTRING(menuId, len(menuId)-13, 1) = 'T' THEN SUBSTRING(menuId, len(menuId)-23, len(menuId))
        ELSE TIMESTAMPADD(MICROSECOND, 1000 * (index + 35), conversationStart)
      END AS menuEntryTime,
  CASE WHEN SUBSTRING(menuId, 1, 4) REGEXP '^[0-9]{4}$' AND LEN(menuId) > 24 AND SUBSTRING(menuId, 5, 1) = '-' AND SUBSTRING(menuId, 8, 1) = '-' AND SUBSTRING(menuId, 11, 1) = 'T' THEN LTRIM(SUBSTRING(menuId, 25, LEN(menuId)))
    WHEN SUBSTRING(menuId, 1, 4) REGEXP '^[A-Za-z]{4}$' AND LEN(menuId) > 24 AND SUBSTRING(menuId, len(menuId)-19, 1) = '-' AND SUBSTRING(menuId, len(menuId)-16, 1) = '-' AND SUBSTRING(menuId, len(menuId)-13, 1) = 'T' THEN RTRIM(SUBSTRING(menuId, 1, LEN(menuId)-24))
    ELSE menuId
    END AS n_menuId,
  lag(menuId,1) OVER (PARTITION BY conversationId order by conversationId) previousMenuId, 
  lead (menuId) over (PARTITION BY conversationId order by conversationId) nextSelectedMenuId, 
  null as menuExitTime, 
  null as menuExitReason,
  conversationStart, 
  conversationStartDateId 
  from
  (
    select conversationId, index, LTRIM(menuId) menuId, conversationStartDateId, conversationStart  from
    (
      select ie.conversationId, posexplode(SPLIT(ie.eventValue,',')) as (index,menuId), ie.conversationStartDateId, dc.conversationStart
      from dgdm_simplyenergy.dim_conversation_ivr_events ie
      join dgdm_simplyenergy.dim_conversations dc
      on ie.conversationId = dc.conversationId 
        and ie.conversationStartDateId = dc.conversationStartDateId
      where ie.eventName='MenuID'  
      and cast(dc.conversationStart as date) >= DATE_SUB('{extract_date}', 3)
    )
    where menuId<>''
  )
)
)
where ( NOT (SUBSTRING(menuId, 1, 1) REGEXP '^[0-9]{1}$' AND LENGTH(TRIM(menuId)) < 25)) 
        and (menuId <> '')
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
  CASE WHEN LEN(previousMenuId) > 24 AND SUBSTRING(previousMenuId, 5, 1) = '-' AND SUBSTRING(previousMenuId, 8, 1) = '-' AND SUBSTRING(previousMenuId, 11, 1) = 'T' 
      THEN LTRIM(SUBSTRING(previousMenuId, 25, LEN(previousMenuId)))
    ELSE previousMenuId
    END AS previousMenuId,
  CASE WHEN LEN(nextSelectedMenuId) > 24 AND SUBSTRING(nextSelectedMenuId, 5, 1) = '-' AND SUBSTRING(nextSelectedMenuId, 8, 1) = '-' AND SUBSTRING(nextSelectedMenuId, 11, 1) = 'T' 
      THEN LTRIM(SUBSTRING(nextSelectedMenuId, 25, LEN(nextSelectedMenuId)))
    ELSE nextSelectedMenuId
    END AS n_nextSelectedMenuId,
  case when menuEntryTime is null then TIMESTAMPADD(MICROSECOND, 1000 * (index + 35), conversationStart) else menuEntryTime end as menuEntryTime, 
  CASE WHEN LEN(nextSelectedMenuId) > 24 AND SUBSTRING(nextSelectedMenuId, 5, 1) = '-' AND SUBSTRING(nextSelectedMenuId, 8, 1) = '-' AND SUBSTRING(nextSelectedMenuId, 11, 1) = 'T' 
          THEN SUBSTRING(nextSelectedMenuId, 1, 24)
        ELSE NULL
      END AS menuExitTime,
  null as menuExitReason,
  conversationStart, 
  conversationStartDateId
  from
(
  select 
  conversationId,
  index+1 as index,
  CASE WHEN LEN(menuId) > 24 AND SUBSTRING(menuId, 5, 1) = '-' AND SUBSTRING(menuId, 8, 1) = '-' AND SUBSTRING(menuId, 11, 1) = 'T' THEN SUBSTRING(menuId, 1, 24)
        ELSE TIMESTAMPADD(MICROSECOND, 1000 * (index + 35), conversationStart)
      END AS menuEntryTime,
  CASE WHEN LEN(menuId) > 24 AND SUBSTRING(menuId, 5, 1) = '-' AND SUBSTRING(menuId, 8, 1) = '-' AND SUBSTRING(menuId, 11, 1) = 'T' THEN LTRIM(SUBSTRING(menuId, 25, LEN(menuId)))
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
    select conversationId, index, menuId, conversationStartDateId, conversationStart  from
    (
      select ie.conversationId, posexplode(SPLIT(ie.eventValue,',')) as (index,menuId), ie.conversationStartDateId, dc.conversationStart
      from dgdm_{tenant}.dim_conversation_ivr_events ie
      join dgdm_{tenant}.dim_conversations dc
      on ie.conversationId = dc.conversationId 
        and ie.conversationStartDateId = dc.conversationStartDateId
      where ie.eventName='MenuID' and cast(dc.conversationStart as date) = '{extract_date}'
    )
    where menuId<>''
  )
)
)
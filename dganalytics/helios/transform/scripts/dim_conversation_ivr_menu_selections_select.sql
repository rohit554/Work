select 
  conversationId,
  index,
  menuId, 
  case when ARRAY_SIZE(split(previousMenuId,' '))>1 then split(previousMenuId,' ')[1] else previousMenuId END as  previousMenuId, 
  case when ARRAY_SIZE(split(nextSelectedMenuId,' '))>1 then split(nextSelectedMenuId,' ')[1] else nextSelectedMenuId END as  nextSelectedMenuId, 
  menuEntryTime, 
  case when ARRAY_SIZE(split(nextSelectedMenuId,' '))>1 then cast(split(nextSelectedMenuId,' ')[0] as TIMESTAMP) else null END as menuExitTime, 
  null as menuExitReason,
  conversationStart, 
  conversationStartDateId
  from
(
  select 
  conversationId,
  index+1 as index,
  case when data[1] is null then data[0] else data[1] END as menuId, 
  lag(menuId,1) OVER (PARTITION BY conversationId order by conversationId) previousMenuId, 
  lead (menuId) over (PARTITION BY conversationId order by conversationId) nextSelectedMenuId, 
  case when data[1] is not null then cast(data[0] AS TIMESTAMP) else null end as menuEntryTime, 
  null as menuExitTime, 
  null as menuExitReason,
  conversationStart, 
  conversationStartDateId 
  from
  (
    select conversationId, index, split(menuId,' ') data,menuId, conversationStartDateId, conversationStart  from
    (
      select ie.conversationId, posexplode(SPLIT(ie.eventValue,',')) as (index,menuId), ie.conversationStartDateId, dc.conversationStart
      from dgdm_simplyenergy.dim_conversation_ivr_events ie
      join dgdm_simplyenergy.dim_conversations dc
      on ie.conversationId = dc.conversationId 
        and ie.conversationStartDateId = dc.conversationStartDateId
      where ie.eventName='MenuID'  and cast(dc.conversationStart as date) = '{extract_date}'
    )
    where menuId<>''
  )
)
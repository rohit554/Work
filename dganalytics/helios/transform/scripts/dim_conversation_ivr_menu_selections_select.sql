select 
conversationId,
index+1 as index,
menuId, 
lag(menuId,1) OVER (PARTITION BY conversationId order by conversationId) previousMenuId, 
lead (menuId) over (PARTITION BY conversationId order by conversationId) nextSelectedMenuId, 
null as menuEntryTime, 
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
    where ie.eventName='MenuID'  and cast(dc.conversationStart as date) = '{extract_date}'
  )
  where menuId<>''
)
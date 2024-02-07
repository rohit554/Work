delete from dgdm_{tenant}.dim_conversation_ivr_events a 
where exists (
  select 1 from dim_conversation_ivr_events b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
)
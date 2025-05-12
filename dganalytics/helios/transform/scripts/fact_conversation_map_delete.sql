delete from dgdm_{tenant}.fact_conversation_map a 
where exists (
  select 1 from fact_conversation_map b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
)
delete from dgdm_{tenant}.dim_conversation_sessions a 
where exists (
  select 1 from dim_conversation_sessions b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
          and a.sessionId = b.sessionId
)
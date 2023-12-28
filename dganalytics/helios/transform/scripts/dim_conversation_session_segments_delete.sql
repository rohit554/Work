delete from dgdm_{tenant}.dim_conversation_session_segments a 
where exists (
  select 1 from dim_conversation_session_segments b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
          and a.sessionId = b.sessionId
)
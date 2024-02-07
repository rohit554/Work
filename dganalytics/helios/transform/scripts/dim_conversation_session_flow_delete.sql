delete from dgdm_{tenant}.dim_conversation_session_flow a 
where exists (
  select 1 from dim_conversation_session_flow b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
)
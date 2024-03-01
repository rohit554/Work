delete from dgdm_{tenant}.dim_conversation_session_segments 
where conversationId in (
  select distinct conversationId from dim_conversation_session_segments
  )
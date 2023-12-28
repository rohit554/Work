delete from dgdm_{tenant}.fact_conversation_metrics 
where conversationId in (
  select distinct conversationId from fact_conversation_metrics
  )
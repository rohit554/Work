delete from dgdm_{tenant}.fact_conversation_evaluations a 
where exists (
  select 1 from fact_conversation_evaluations b 
          where a.conversationStartDateId = b.conversationStarDateId
          and a.evaluationId = b.evaluationId
)
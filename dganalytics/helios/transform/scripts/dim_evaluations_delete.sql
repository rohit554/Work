delete from dgdm_{tenant}.dim_evaluations a 
where exists (
  select 1 from dim_evaluations b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStarDateId
          and a.evaluationId = b.evaluationId
)
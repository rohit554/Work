delete from dgdm_{tenant}.fact_objection a 
where exists (
  select 1 from fact_objection b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
)
delete from dgdm_{tenant}.fact_apprehension a 
where exists (
  select 1 from fact_apprehension b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
)
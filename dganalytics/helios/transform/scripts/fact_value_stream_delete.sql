delete from dgdm_{tenant}.fact_value_stream a 
where exists (
  select 1 from fact_value_stream b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
)
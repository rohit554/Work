delete from dgdm_{tenant}.dim_summary a 
where exists (
  select 1 from dim_summary b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
)
delete from dgdm_{tenant}.dim_conversations a 
where exists (
  select 1 from dim_conversations b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
)
delete from dgdm_{tenant}.dim_flow_outcomes a 
where exists (
  select 1 from dim_flow_outcomes b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
)
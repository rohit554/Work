delete from dgdm_{tenant}.dim_surveys a 
where exists (
  select 1 from dim_surveys b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
          and a.surveyId = b.surveyId
)
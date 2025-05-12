SELECT
  T.conversationId,
  insight.agent_identifier agent_identifier,
  insight.satisfaction,
  insight.resolved,
  insight.process_knowledge,
  insight.system_knowledge,
  insight.additional_service,
  C.conversationStartDateId,
  insight.sentiment,
  summary,
  insight.process_name
  
FROM
  (
    select
      conversationId,
      explode(insights) insight,
      summary
    from
      (
        SELECT
          conversation_id conversationId,
          insights,
          summary,
          row_number() OVER (PARTITION BY conversation_id ORDER BY recordInsertTime DESC) RN
        FROM
          gpc_{tenant}.raw_conversation_insights
        WHERE extractDate between cast('{extract_start_time}' as date) and cast('{extract_end_time}' as date)
      )
    where
      RN = 1
  ) T
  JOIN dgdm_{tenant}.dim_conversations C 
  ON T.conversationId = C.conversationId
SELECT 
  s.conversationId,
  s.summary,
  c.conversationStartDateId AS conversationStartDateId
FROM (
    SELECT 
      conversation_id AS conversationId,
      summary,
      extractDate,
      ROW_NUMBER() OVER (PARTITION BY conversation_id ORDER BY recordInsertTime DESC) AS rn
    FROM gpc_{tenant}.raw_summary
    WHERE extractDate between cast('{extract_start_time}' as date) and cast('{extract_end_time}' as date)
    QUALIFY rn = 1 
) AS s
JOIN dgdm_{tenant}.dim_conversations c 
    on s.conversationId = c.conversationId

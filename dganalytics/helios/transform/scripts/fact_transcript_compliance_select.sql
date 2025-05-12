SELECT
  q.conversationId,
  compliance.question,
  compliance.answer,
  c.conversationStartDateId
FROM
  (
    SELECT
      conversationId,
      EXPLODE(compliance) compliance
    FROM
      (
        SELECT
          conversation_id conversationId,
          compliance,
          row_number() OVER (PARTITION BY conversation_id ORDER BY recordInsertTime DESC) RN
        FROM
          gpc_{tenant}.raw_conversation_compliance
        WHERE
          extractDate between cast('{extract_start_time}' as date) and cast('{extract_end_time}' as date)
      )
    WHERE
      RN = 1
  ) q
  JOIN dgdm_{tenant}.dim_conversations c on q.conversationId = c.conversationId
SELECT
  q.conversation_id,
  quality.question,
  quality.answer,
  c.conversationStartDateId
FROM
  (
    SELECT
      conversation_id,
      EXPLODE(quality) quality
    FROM
      (
        SELECT
          conversation_id,
          quality,
          row_number() OVER(
            PARTITION BY conversation_id
            ORDER BY
              recordInsertTime DESC
          ) RN
        FROM
          gpc_{tenant}.raw_conversation_quality 
        WHERE extractDate between cast('{extract_start_time}' as date) and cast('{extract_end_time}' as date)
      )
    WHERE
      RN = 1
  ) q
  JOIN dgdm_{tenant}.dim_conversations c 
  on q.conversation_id = c.conversationId
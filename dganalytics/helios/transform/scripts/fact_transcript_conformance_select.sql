
SELECT
  c.conversation_id conversationId,
  conformity.question,
  conformity.answer,
  c.conversationStartDateId
FROM
  (
    SELECT
      conversation_id,
      EXPLODE(conformance) conformity
    FROM
      (
        SELECT
          conversation_id,
          conformance,
          row_number() OVER(
            PARTITION BY conversation_id
            ORDER BY
              recordInsertTime DESC
          ) RN
        FROM
          gpc_{tenant}.raw_conversation_conformance
        WHERE extractDate between cast('{extract_start_time}' as date) and cast('{extract_end_time}' as date)
        Qualify RN = 1
      )
    
  ) c
  JOIN dgdm_{tenant}.dim_conversations c 
    on c.conversation_id = c.conversationId
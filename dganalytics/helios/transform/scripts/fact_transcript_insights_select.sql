SELECT T.conversation_id conversationId,
T.satisfaction,
T.resolved,
T.process_knowledge,
T.system_knowledge,
T.additional_service,
C.conversationStartDateId
FROM 
(
  select * from(
    SELECT *,
          row_number() OVER (PARTITION BY conversation_id ORDER BY recordInsertTime DESC) RN
       FROM gpc_{tenant}.raw_transcript_insights 
       WHERE
        extractDate = '{extract_date}'
        AND extractIntervalStartTime = '{extract_start_time}'
        AND extractIntervalEndTime = '{extract_end_time}'
      ) 
 where RN=1     
)T
JOIN dgdm_{tenant}.dim_conversations C
    ON T.conversation_id = C.conversationId
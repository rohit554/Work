SELECT T.conversation_id conversationId,
T.satisfaction,
T.resolved,
T.process_knowledge,
T.system_knowledge,
T.additional_service,
C.conversationStartDateId
FROM (SELECT * FROM gpc_{tenant}.raw_transcript_insights 
      WHERE
        extractDate = '{extract_date}'
        AND extractIntervalStartTime = '{extract_start_time}'
        AND extractIntervalEndTime = '{extract_end_time}') T
JOIN dgdm_{tenant}.dim_conversations C
    ON T.conversation_id = C.conversationId
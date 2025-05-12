SELECT
  conversationId,
  participant,
  row_number() OVER (PARTITION BY conversationId ORDER BY eventStart, eventEnd) AS line,
  action,
  eventStart startTime,
  eventEnd endTime,
  userId,
  conversationStartDateId,
  '{tenant}' orgId,
  sentiment
FROM
  (
    SELECT
      P.conversationId,
      'Queue' participant,
      L.queueName action,
      MIN(S.segmentStart) eventStart,
      MAX(S.segmentEnd) eventEnd,
      null userId,
      P.conversationStartDateId,
      null sentiment
    FROM
      dgdm_{tenant}.dim_conversation_participants P
        JOIN dgdm_{tenant}.dim_conversation_sessions SS
          ON P.conversationStartDateId = SS.conversationStartDateId
          AND P.conversationId = SS.conversationId
          AND P.participantId = SS.participantId
          AND P.purpose = 'acd'
          AND SS.mediaTypeId IN (1, 5)
       JOIN dgdm_{tenant}.dim_conversation_session_segments S
          ON P.conversationStartDateId = S.conversationStartDateId
          AND P.conversationId = S.conversationId
          AND P.participantId = S.participantId
        JOIN dgdm_{tenant}.dim_queue_language L
          ON L.queueId = S.queueId
          AND L.language = 'English'
    WHERE
      P.conversationStartDateId BETWEEN DATE_FORMAT(cast('{extract_start_time}' as timestamp), 'yyyyMMdd') AND DATE_FORMAT(cast('{extract_end_time}' as timestamp), 'yyyyMMdd')
			AND S.segmentStart between cast('{extract_start_time}' as timestamp) and cast('{extract_end_time}' as timestamp)
      AND EXISTS (
        SELECT
          1
        FROM
          gpc_{tenant}.fact_conversation_transcript_phrases TP
        WHERE
          date_format(TP.conversationStartDate, 'yyyyMMdd') = P.conversationStartDateId
          AND TP.conversationId = P.conversationId
      )
    GROUP BY
      P.conversationId,
      S.sessionId,
      S.queueId,
      P.conversationStartDateId,
      L.queueName
    UNION ALL
    SELECT
      M.conversationId,
      CASE
        WHEN M.name = 'tHeld' THEN 'Hold'
        WHEN M.name = 'nBlindTransferred' THEN 'Transfer'
        WHEN M.name = 'nConsultTransferred' THEN 'Transfer'
      END participant,
      CASE
        WHEN M.name = 'tHeld' THEN 'Hold'
        WHEN M.name = 'nBlindTransferred' THEN 'Blind Transferred'
        WHEN M.name = 'nConsultTransferred' THEN 'Consult Transferred'
      END action,
      CASE
        WHEN
          M.name = 'tHeld'
        THEN
          date_format(
            from_unixtime(unix_timestamp(eventTime) - (value / 1000)), "yyyy-MM-dd'T'HH:mm:ss'Z'"
          )
        ELSE M.eventTime
      END eventStart,
      M.eventTime eventEnd,
      p.userId,
      M.conversationStartDateId,
      null sentiment
    FROM
      dgdm_{tenant}.fact_conversation_metrics M
        JOIN dgdm_{tenant}.dim_conversation_participants P
          ON P.conversationStartDateId = M.conversationStartDateId
          AND M.conversationId = P.conversationId
          AND M.participantId = P.participantId
          AND P.purpose = 'agent'
          AND M.name IN ('tHeld', 'nBlindTransferred', 'nConsultTransferred')
        JOIN dgdm_{tenant}.dim_conversations SS
          ON P.conversationStartDateId = SS.conversationStartDateId
          AND P.conversationId = SS.conversationId
          AND SS.initialSessionMediaTypeId IN (1, 5)
        JOIN gpc_{tenant}.dim_last_handled_conversation C
          ON CAST(date_format(C.conversationStartDate, 'yyyyMMdd') AS INT)
          = M.conversationStartDateId
          AND C.conversationId = M.conversationId
        JOIN dgdm_{tenant}.dim_queue_language L
          ON L.queueId = C.queueId
          AND L.language = 'English'
    WHERE
      M.conversationStartDateId BETWEEN DATE_FORMAT(cast('{extract_start_time}' as timestamp), 'yyyyMMdd') AND DATE_FORMAT(cast('{extract_end_time}' as timestamp), 'yyyyMMdd')
			AND SS.conversationStart between cast('{extract_start_time}' as timestamp) and cast('{extract_end_time}' as timestamp)
      AND EXISTS (
        SELECT
          1
        FROM
          gpc_{tenant}.fact_conversation_transcript_phrases TP
        WHERE
          date_format(TP.conversationStartDate, 'yyyyMMdd') = M.conversationStartDateId
          AND TP.conversationId = M.conversationId
      )
    UNION ALL
    SELECT
      TP.conversationId,
      case
        when TP.participantPurpose = 'internal' THEN 'Agent'
        ELSE 'Customer'
      END participant,
      text action,
      timestamp_millis(TP.startTimeMs) eventStart,
      timestamp_millis(TP.startTimeMs + TP.milliseconds) eventEnd,
      P.userId,
      date_format(TP.conversationStartDate, 'yyyyMMdd') conversationStartDateId,
      TP.sentiment
    FROM
      gpc_{tenant}.fact_conversation_transcript_phrases TP
        JOIN dgdm_{tenant}.dim_conversation_participants P
          ON P.conversationStartDateId = date_format(TP.conversationStartDate, 'yyyyMMdd')
          AND P.conversationId = TP.conversationId
          AND P.purpose = 'agent'
        JOIN dgdm_{tenant}.dim_conversation_session_segments S
          ON S.conversationStartDateId = P.conversationStartDateId
          AND S.conversationId = P.conversationId
          AND S.participantId = P.participantId
          AND TP.startTimeMs / 1000 BETWEEN
            unix_timestamp(S.segmentStart)
          AND
            unix_timestamp(S.segmentEnd)
          and S.segmentType = 'interact'
				JOIN dgdm_{tenant}.dim_conversations C
				ON C.conversationStartDateId = P.conversationStartDateId
				AND C.conversationId = Tp.conversationId
        JOIN dgdm_{tenant}.dim_queue_language L
          ON L.queueId = S.queueId
          AND L.language = 'English'
    where
			TP.conversationStartDate BETWEEN cast('{extract_start_time}' as DATE) AND cast('{extract_end_time}' as DATE)
			AND C.conversationStart BETWEEN '{extract_start_time}' AND '{extract_end_time}'
  )
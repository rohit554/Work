SELECT conversationId,
        participantId,
        sessionId,
        M.mediaTypeId,
        O.originatingDirectionId,
        metric.emitDate eventTime,
        metric.name,
        CAST(metric.value AS FLOAT) value,
        D.dateId AS conversationStartId
FROM (SELECT conversationId,
            conversationStart,
            originatingDirection,
            participantId,
            session.sessionId,
            session.mediaType,
            explode(session.metrics) metric
  FROM (SELECT conversationId,
              conversationStart,
              originatingDirection,
              participant.participantId,
              explode(participant.sessions) session
      FROM (SELECT conversationId,
                  conversationStart,
                  originatingDirection,
                  EXPLODE(participants) participant
              FROM (SELECT conversationId,
                          conversationStart,
                          originatingDirection,
                          participants,
                          row_number() OVER (PARTITION BY conversationId ORDER BY recordInsertTime DESC) RN
                  FROM gpc_{tenant}.raw_conversation_details 
                  where extractDate >= '{extract_date}' 
              )
          WHERE RN =1
          )
      )
  ) C
JOIN dgdm_{tenant}.dim_interaction_originating_direction O
    ON c.originatingDirection = O.originatingDirection
JOIN dgdm_{tenant}.dim_media_types M
    ON c.mediaType = M.mediaType
JOIN dgdm_{tenant}.dim_date D
    ON CAST(C.conversationStart AS Date) = D.dateVal

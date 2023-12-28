SELECT conversationId,
        -- conversationStart,
        participantId,
        sessionId,
        segment.segmentStart,
        segment.segmentEnd,
        segment.queueId,
        segment.errorCode,
        segment.disconnectType,
        segment.segmentType,
        segment.wrapUpCode wrapUpCodeId,
        segment.wrapUpNote,
        dateId as conversationStartDateId
FROM (SELECT conversationId,
        conversationStart,
        participantId,
        session.sessionId,
        EXPLODE(session.segments) segment
        -- dateId as conversationStartDateId
FROM(SELECT conversationId,
        conversationStart,
        participant.participantId,
        EXPLODE(participant.sessions) session
        -- dateId as conversationStartDateId
    FROM (
        SELECT conversationId, conversationStart, explode(participants) participant
            FROM (select    conversationId,
                            conversationStart,
                            participants, 
                            row_number() over (partition by conversationId order by recordInsertTime DESC) rn
        from gpc_{tenant}.raw_conversation_details
        where extractDate = '{extract_date}'
        and  extractIntervalStartTime = '{extract_start_time}' 
        and extractIntervalEndTime = '{extract_end_time}'
    )
    where rn = 1
    )
 )
 ) C
INNER JOIN dgdm_{tenant}.dim_date D
  ON D.dateval = CAST(C.conversationStart as date)

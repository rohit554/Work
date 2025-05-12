SELECT conversationId,
        -- conversationStart,
        participantId,
        sessionId,
        addressFrom,
        addressOther,
        addressSelf,
        addressTo,
        ani,
        callbackNumbers,
        callbackScheduledTime,
        callbackUserName,
        direction,
        dnis,
        M.mediaTypeId,
        messageType,
        peerId,
        dateId as conversationStartDateId
FROM (SELECT conversationId,
        conversationStart,
        participantId,
        session.sessionId,
        session.addressFrom,
        session.addressOther,
        session.addressSelf,
        session.addressTo,
        session.ani,
        session.callbackNumbers,
        session.callbackScheduledTime,
        session.callbackUserName,
        session.direction,
        session.dnis,
        session.mediaType,
        session.messageType,
        session.peerId
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
        where extractDate = '{extract_date}' and conversationStart between cast('{extract_start_time}' as timestamp) and cast('{extract_end_time}' as timestamp)
    )
    where rn = 1
    )
 )
 ) C
 INNER JOIN dgdm_{tenant}.dim_media_types M
  ON C.mediaType = M.mediaType
INNER JOIN dgdm_{tenant}.dim_date D
  ON D.dateval = CAST(C.conversationStart as date)

SELECT conversationId, dateId as conversationStartDateId, conversationStart, conversationEnd,
        OD.originatingDirectionId,
        divisionIds,
        initialParticipantPurpose,
        mediaTypeId initialSessionMediaTypeId,
        messageType initialSessionMessageType,
        null as location
FROM (
    SELECT conversationId, conversationStart, conversationEnd, originatingDirection, divisionIds,
            initialParticipant.purpose initialParticipantPurpose,
            initialParticipant.sessions[0].mediaType mediaType,
            initialParticipant.sessions[0].messageType messageType  FROM
            (
                select conversationId, conversationStart, conversationEnd, originatingDirection, divisionIds, participants[0] initialParticipant, row_number() over (partition by conversationId order by recordInsertTime DESC) rn
                from gpc_{tenant}.raw_conversation_details
                where extractDate = '{extract_date}'
                and  extractIntervalStartTime = '{extract_start_time}' 
                and extractIntervalEndTime = '{extract_end_time}'
            )
            where rn = 1
) C
INNER JOIN dgdm_{tenant}.dim_media_types M
ON C.mediaType = M.mediaType
INNER JOIN dgdm_{tenant}.dim_date D
ON D.dateval = CAST(C.conversationStart as date)
INNER JOIN dgdm_{tenant}.dim_interaction_originating_direction OD
ON OD.originatingDirection = C.originatingDirection

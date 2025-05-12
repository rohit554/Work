SELECT conversationId, dateId as conversationStartDateId, conversationStart, conversationEnd,
        OD.originatingDirectionId,
        divisionIds,
        initialParticipantPurpose,
        mediaTypeId initialSessionMediaTypeId,
        messageType initialSessionMessageType,
        location,false isManualAssessmentNeeded, false hasManuallyEvaluated
FROM (
    SELECT conversationId, conversationStart, conversationEnd, originatingDirection, divisionIds,
            initialParticipant.purpose initialParticipantPurpose,
            initialParticipant.sessions[0].mediaType mediaType,
            initialParticipant.sessions[0].messageType messageType,
            coalesce(participant.attributes["Mailing State"],participant.attributes["MailingState"]) as location  FROM
            (
                select conversationId, conversationStart, conversationEnd, originatingDirection, divisionIds, participants[0] initialParticipant, explode(participants) participant, row_number() over (partition by conversationId order by recordInsertTime DESC) rn
                from gpc_{tenant}.raw_conversation_details
                where extractDate = '{extract_date}' and conversationStart between cast('{extract_start_time}' as timestamp) and cast('{extract_end_time}' as timestamp)
            )
            where rn = 1
) C
INNER JOIN dgdm_{tenant}.dim_media_types M
ON C.mediaType = M.mediaType
INNER JOIN dgdm_{tenant}.dim_date D
ON D.dateval = CAST(C.conversationStart as date)
INNER JOIN dgdm_{tenant}.dim_interaction_originating_direction OD
ON OD.originatingDirection = C.originatingDirection

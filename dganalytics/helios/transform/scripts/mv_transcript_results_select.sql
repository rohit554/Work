SELECT COUNT(DISTINCT D.conversationId) noOfInteractions,
    D.conversationStartDateId,
    DD.dateVal,
    D.initialSessionMediaTypeId mediaTypeId,
    D.location region,
    D.originatingDirectionId,
    contactReason,
    mainInquiry,
    rootCause,
    inquiry_type,
    resolved,
    T.satisfaction,
    collect_list(DISTINCT S.queueId) queueIds,
    collect_list(DISTINCT S.wrapUpCodeId) wrapUpCodeIds
FROM dgdm_{tenant}.dim_conversations D
JOIN dgdm_{tenant}.dim_conversation_session_segments S
    ON D.conversationStartDateId = S.conversationStartDateId
    AND D.conversationId = S.conversationId
LEFT JOIN dgdm_{tenant}.fact_transcript_insights T
    ON D.conversationStartDateId = T.conversationStartDateId
        AND D.conversationId = T.conversationId
LEFT JOIN dgdm_{tenant}.fact_transcript_contact_reasons F
    ON D.conversationStartDateId = F.conversationStartDateId
        AND D.conversationId = F.conversationId
JOIN dgdm_{tenant}.dim_date DD
    ON DD.dateId = D.conversationStartDateId 
WHERE D.conversationStartDateId > (select dateId from dgdm_{tenant}.dim_date where dateVal = cast(DATEADD(DAY, -3, '{extract_date}') as date))
GROUP BY D.conversationStartDateId, 
    DD.dateVal,
    D.initialSessionMediaTypeId, 
    D.location,
    D.originatingDirectionId,
    contactReason, 
    mainInquiry, 
    rootCause, 
    inquiry_type,
    resolved,
    T.satisfaction
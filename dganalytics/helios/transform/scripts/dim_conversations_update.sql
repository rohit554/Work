UPDATE dgdm_{tenant}.{transformation} C
SET location = (
    SELECT coalesce(participant.attributes["Mailing State"],participant.attributes["MailingState"]) 
    FROM (SELECT conversationId, explode(participants) participant FROM gpc_{tenant}.raw_conversation_details
    WHERE 
    extractDate = '{extract_date}'
    and  extractIntervalStartTime = '{extract_start_time}' 
    and extractIntervalEndTime = '{extract_end_time}'
    
    ) 
    WHERE conversationId = C.conversationId AND 
    coalesce(participant.attributes["Mailing State"], participant.attributes["MailingState"]) IS NOT NULL     LIMIT 1
)
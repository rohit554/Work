UPDATE dgdm_{tenant}.{transformation} C
SET location = (
    SELECT participant.attributes["Mailing State"] 
    FROM (SELECT conversationId, explode(participants) participant FROM gpc_{tenant}.raw_conversation_details
    WHERE 
    extractDate = '{extract_date}'
    and  extractIntervalStartTime = '{extract_start_time}' 
    and extractIntervalEndTime = '{extract_end_time}'
    
    ) 
    WHERE conversationId = C.conversationId AND participant.attributes["Mailing State"] IS NOT NULL 
    LIMIT 1
)
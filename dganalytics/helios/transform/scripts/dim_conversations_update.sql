UPDATE dgdm_{tenant}.{transformation} C
SET location = (
    SELECT coalesce(participant.attributes["Mailing State"],participant.attributes["MailingState"]) 
    FROM (SELECT conversationId, explode(participants) participant FROM gpc_{tenant}.raw_conversation_details
    WHERE 
    extractDate = '{extract_date}'
    
    ) 
    WHERE conversationId = C.conversationId AND 
    coalesce(participant.attributes["Mailing State"], participant.attributes["MailingState"]) IS NOT NULL     
    LIMIT 1
)
delete from dgdm_{tenant}.fact_conversation_surveys
where conversationId IN 
    (select distinct conversationId from fact_conversation_surveys)
delete from dgdm_{tenant}.fact_transcript_contact_reasons
where conversationId IN 
    (select distinct conversationId from fact_transcript_contact_reasons)
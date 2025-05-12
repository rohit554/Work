delete from dgdm_{tenant}.fact_transcript_actions
where conversationId IN (select distinct conversationId from fact_transcript_actions)
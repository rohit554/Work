delete from dgdm_{tenant}.fact_transcript_insights
where conversationId IN 
      (select distinct conversationId from fact_transcript_insights)
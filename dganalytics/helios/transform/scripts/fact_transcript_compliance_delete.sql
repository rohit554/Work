DELETE FROM dgdm_{tenant}.fact_transcript_compliance a 
WHERE EXISTS (
  SELECT 1 FROM fact_transcript_compliance b 
          WHERE a.conversationId = b.conversationId     
          AND a.conversationStartDateId = b.conversationStartDateId
)
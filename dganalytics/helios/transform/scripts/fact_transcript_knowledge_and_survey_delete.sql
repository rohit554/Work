DELETE FROM dgdm_{tenant}.fact_transcript_knowledge_and_survey a 
WHERE EXISTS (
  SELECT 1 FROM fact_transcript_knowledge_and_survey b 
          WHERE a.conversationId = b.conversationId     
          AND a.conversationStartDateId = b.conversationStartDateId
)
DELETE FROM dgdm_{tenant}.fact_transcript_quality a 
WHERE EXISTS (
  SELECT 1 FROM fact_transcript_quality b 
          WHERE a.conversation_id = b.conversation_id     
          AND a.conversationStartDateId = b.conversationStartDateId
)
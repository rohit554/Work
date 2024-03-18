DELETE FROM dgdm_{tenant}.fact_transcript_actions a 
WHERE EXISTS (
  SELECT 1 FROM fact_transcript_actions b 
          WHERE a.conversationId = b.conversationId
          
)
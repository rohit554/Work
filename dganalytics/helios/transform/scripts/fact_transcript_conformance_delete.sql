delete from dgdm_{tenant}.fact_transcript_conformance a 
where exists (
  select 1 from fact_transcript_conformance b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
)
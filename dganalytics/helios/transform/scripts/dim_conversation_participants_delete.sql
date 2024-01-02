delete from dgdm_{tenant}.dim_conversation_participants a 
where exists (
  select 1 from dim_conversation_participants b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
          and a.participantId = b.participantId
)
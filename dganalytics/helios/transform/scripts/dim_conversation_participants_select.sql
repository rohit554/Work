SELECT conversationId,
        participant.participantId,
        participant.participantName,
        participant.purpose,
        participant.userId,
        dateId as conversationStartDateId
FROM (
      SELECT conversationId, conversationStart, explode(participants) participant
        FROM (select    conversationId,
                        conversationStart,
                        participants, 
                        row_number() over (partition by conversationId order by recordInsertTime DESC) rn
      from gpc_{tenant}.raw_conversation_details
      where extractDate = '{extract_date}' and conversationStart between cast('{extract_start_time}' as timestamp) and cast('{extract_end_time}' as timestamp)
)
where rn = 1
) C
INNER JOIN dgdm_{tenant}.dim_date D
  ON D.dateval = CAST(C.conversationStart as date)
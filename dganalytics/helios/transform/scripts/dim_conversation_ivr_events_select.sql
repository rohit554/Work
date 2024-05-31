SELECT
  conversationId,
  eventName,
  eventValue,
  CASE
    WHEN INSTR(eventName, 'attrFlowStatus') > 0 THEN CAST(
      SUBSTRING(
        eventName
        FROM
          0 FOR 24
      ) AS TIMESTAMP
    )
    ELSE NULL
  END AS eventTime,
  participantName,
  conversationStartDateId
FROM
  (
    SELECT
      conversationId,
      explode(participant.attributes) (eventName, eventValue),
      participant.participantName,
      dateId as conversationStartDateId
    FROM
      (
        SELECT
          conversationId,
          EXPLODE(participants) participant,
          conversationStart
        FROM
          (
            SELECT
            conversationId,
            participants,
            conversationStart,
            ROW_NUMBER() OVER (PARTITION BY conversationId order by recordInsertTime DESC) rn
            FROM
            gpc_{tenant}.raw_conversation_details
            WHERE
            extractDate='{extract_date}'
            and participants is not null
            
            and date_diff(extractIntervalEndTime,extractIntervalStartTime) = 1
          )
          where rn = 1
        
      )
      JOIN dgdm_{tenant}.dim_date on dateVal = cast(conversationStart as date)
          
  )
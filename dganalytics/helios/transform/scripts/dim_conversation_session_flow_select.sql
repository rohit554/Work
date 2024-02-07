select distinct conversationId,
    participantId,
    sessionId,
    flowId,
    endingLanguage,
    entryReason,
    entryType,
    exitReason,
    flowName,
    flowType,
    flowVersion,
    null as flowOutcome,
    null as flowOutcomeStartTimestamp,
    null as flowOutcomeEndTimestamp,
    null as flowOutcomeId,
    null as flowOutcomeValue,
    issuedCallback,
    startingLanguage,
    transferTargetAddress,
    transferTargetName,
    transferType,
    conversationStartDateId
    from
    (
        select conversationId,
        participantId,
        sessionId,
        flowId,
        endingLanguage,
        entryReason,
        entryType,
        entryReason,
        exitReason,
        flowName,
        flowType,
        flowVersion,
        issuedCallback,
        startingLanguage,
        transferTargetAddress,
        transferTargetName,
        transferType,
        explode(metrics) as metric,
        conversationStartDateId
        from
        (
          select conversationId,
          participantId,
          session.sessionId,
          session.flow.flowId,
          session.flow.endingLanguage,
          session.flow.entryReason,
          session.flow.entryType,
          session.flow.exitReason,
          session.flow.flowName,
          session.flow.flowType,
          session.flow.flowVersion,
          session.flow.issuedCallback,
          session.flow.startingLanguage,
          session.flow.transferTargetAddress,
          session.flow.transferTargetName,
          session.flow.transferType,
          session.metrics as metrics,
          conversationStartDateId
          from (
            select conversationId,
            conversationStartDateId,
            participant.participantId,
            explode(participant.sessions) as session
            from
            (
              select conversationId,
              conversationStartDateId,
              explode(participants) as participant
              from 
              (
                SELECT
                  conversationId,
                  dateId conversationStartDateId,
                  participants,
                  row_number() OVER (
                    PARTITION BY conversationId
                    ORDER BY
                      recordInsertTime DESC
                  ) rn
                FROM
                  gpc_{tenant}.raw_conversation_details
                join dgdm_{tenant}.dim_date on cast(conversationStart as date) = dateVal
                where extractDate='{extract_date}'
              )
               WHERE rn = 1
            )
          )
          where session.flow is not null
        )

    )






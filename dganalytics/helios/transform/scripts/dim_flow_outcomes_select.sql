select distinct conversationId,
participantId,
sessionId,
flowId,
outcome.flowOutcome,
cast(outcome.flowOutcomeStartTimestamp as timestamp) flowOutcomeStartTimestamp,
cast(outcome.flowOutcomeEndTimestamp as timestamp) flowOutcomeEndTimestamp,
outcome.flowOutcomeId,
outcome.flowOutcomeValue,
conversationStartDateId
    from(
        select conversationId,
            conversationStartDateId,
            participantId,
            sessionId,
            flow.flowId,
            outcome
            from(
                select conversationId,
                    conversationStartDateId,
                    participantId,
                    session.sessionId sessionId,
                    session.flow flow,
                    explode(session.flow.outcomes) outcome
                    from(
                        select conversationId,
                            conversationStartDateId,
                            participant.participantId,
                            explode(participant.sessions) as session
                            from
                            (
                                select conversationId,conversationStartDateId,
                                explode(participants) as participant
                                from 
                                (
                                    SELECT 
                                        conversationId,
                                        dateId conversationStartDateId,
                                        participants
                                        FROM
                                        gpc_simplyenergy.raw_conversation_details
                                        join dgdm_simplyenergy.dim_date on cast(conversationStart as date) = dateVal
                                    where  extractDate = '{extract_date}'
                                    -- limit 2
                                ) 
                            )
                    )
            ) 
    ) where outcome is not null
        
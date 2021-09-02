dim_users = """
    SELECT 
        DISTINCT user.username AS userName,
        user.id AS userId,
        user.name AS userFullName,
        user.email AS userEmail,
        user.title AS userTitle,
        user.department AS department,
        user.division.id AS divisionId,
        user.division.name AS divisionName,
        user.manager.id AS managerId,
        manager.name AS managerFullName,
        user.state
    FROM
        raw_users user
    LEFT JOIN
        raw_users manager
    ON
        user.manager.id = manager.id
"""

dim_routing_queues = """
    SELECT 
        DISTINCT id AS queueId,
        name AS queueName,
        acwSettings.wrapupPrompt AS wrapUpPrompt,
        cast(mediaSettings.call.serviceLevel.durationMs AS integer) AS callServiceLevelDurationMs,
        cast(mediaSettings.call.serviceLevel.percentage * 100 AS float) AS callServiceLevelPercentage,
        cast(mediaSettings.callback.serviceLevel.durationMs AS integer) AS callbackServiceLevelDurationMs,
        cast(mediaSettings.callback.serviceLevel.percentage * 100 AS float) AS callbackServiceLevelPercentage,
        cast(mediaSettings.chat.serviceLevel.durationMs AS integer) AS chatServiceLevelDurationMs,
        cast(mediaSettings.chat.serviceLevel.percentage * 100 AS float) AS chatServiceLevelPercentage,
        cast(mediaSettings.email.serviceLevel.durationMs AS integer) AS emailServiceLevelDurationMs,
        cast(mediaSettings.email.serviceLevel.percentage * 100 AS float) AS emailServiceLevelPercentage,
        cast(mediaSettings.message.serviceLevel.durationMs AS integer) AS messageServiceLevelDurationMs,
        cast(mediaSettings.message.serviceLevel.percentage * 100 AS float) AS messageServiceLevelPercentage
    FROM
        raw_routing_queues
"""

dim_wrap_up_codes = """
    SELECT 
        DISTINCT id as wrapUpCode,
        name as wrapUpCodeName
    FROM
        raw_wrapup_codes
"""

dim_evaluation_forms = """
    SELECT 
        DISTINCT evaluationFormId,
        evaluationFormName,
        questionGroupId,
        questionGroupName,
        questionGroupDefaultAnswersToHighest,
        questionGroupDefaultAnswersToNA,
        questionGroupManualWeight,
        questionGroupNAEnabled,
        questionGroupWeight,
        questionId,
        questionText,
        questionCommentsRequired,
        questionIsCritical,
        questionIsKill,
        questionNAEnabled,
        questionType,
        answerOption.id AS answerOptionId,
        answerOption.text AS answerOptionText,
        answerOption.value AS answerOptionValue
    FROM (	
        SELECT 
            DISTINCT evaluationFormId,
            evaluationFormName,
            questionGroupId,
            questionGroupName,
            questionGroupDefaultAnswersToHighest,
            questionGroupDefaultAnswersToNA,
            questionGroupManualWeight,
            questionGroupNAEnabled,
            questionGroupWeight,
            question.id AS questionId,
            question.text AS questionText,
            question.commentsRequired AS questionCommentsRequired,
            question.isCritical AS questionIsCritical,
            question.isKill AS questionIsKill,
            question.naEnabled AS questionNAEnabled,
            question.type AS questionType,
            explode(question.answerOptions) AS answerOption
        FROM (
            SELECT
                evaluationFormId,
                evaluationFormName,
                questionGroup.id AS questionGroupId,
                questionGroup.name AS questionGroupName,
                questionGroup.defaultAnswersToHighest AS questionGroupDefaultAnswersToHighest,
                questionGroup.defaultAnswersToNA AS questionGroupDefaultAnswersToNA,
                questionGroup.manualWeight AS questionGroupManualWeight,
                questionGroup.naEnabled AS questionGroupNAEnabled,
                questionGroup.weight AS questionGroupWeight,
                explode(questionGroup.questions) AS question
            FROM (
                SELECT
                    id AS evaluationFormId,
                    name AS evaluationFormName,
                    explode(questionGroups) AS questionGroup
                FROM
                    raw_evaluation_forms
            )
        )
    )
"""


def fact_conversation_metrics(extract_start_time: str, extract_end_time: str):
    return f"""
        SELECT 
            conversationId,
            min(conversationStart) AS conversationStart,
            max(conversationEnd) AS conversationEnd,
            originatingDirection,
            min(element_at(sessionSegments, 1).segmentStart) AS interactionStartTime,
            max(element_at(sessionSegments, size(sessionSegments)).segmentEnd) AS interactionEndTime,
            userId,
            purpose,
            participantTransactionType,
            participantTransactionId,
            mediaType,
            messageType,
            direction,
            ani,
            dnis,
            dispositionName,
            outboundCampaignId,
            outboundContactId,
            outboundContactListId,
            element_at(sessionSegments, size(sessionSegments)).queueId AS queueId,
            element_at(sessionSegments, size(sessionSegments)).wrapUpCode AS wrapUpCode,
            sum(CASE WHEN coalesce(tAbandon, 0) > 0 THEN 1 ELSE 0 END) AS nAbandon,
            sum(CASE WHEN coalesce(tAcd, 0) > 0 THEN 1 ELSE 0 END) AS nAcd,
            sum(CASE WHEN coalesce(tAcw, 0) > 0 THEN 1 ELSE 0 END) AS nAcw,
            sum(CASE WHEN coalesce(tAnswered, 0) > 0 THEN 1 ELSE 0 END) AS nAnswered,
            sum(nBlindTransferred) AS nBlindTransferred,
            sum(nConnected) AS nConnected,
            sum(nConsult) AS nConsult,
            sum(nConsultTransferred) AS nConsultTransferred,
            sum(nError) AS nError,
            sum(CASE WHEN coalesce(tHandle, 0) > 0 THEN 1 ELSE 0 END) AS nHandle,
            sum(CASE WHEN coalesce(tHeldComplete, 0) > 0 THEN 1 ELSE 0 END) AS nHeldComplete,
            sum(nOffered) AS nOffered,
            sum(nOutbound) AS nOutbound,
            sum(nOutboundAbandoned) AS nOutboundAbandoned,
            sum(nOutboundAttempted) AS nOutboundAttempted,
            sum(nOutboundConnected) AS nOutboundConnected,
            sum(nOverSla) AS nOverSla,
            sum(CASE WHEN coalesce(tShortAbandon, 0) > 0 THEN 1 ELSE 0 END) AS nShortAbandon,
            sum(CASE WHEN coalesce(tTalkComplete, 0) > 0 THEN 1 ELSE 0 END) AS nTalkComplete,
            sum(nTransferred) AS nTransferred,
            sum(tAbandon) AS tAbandon,
            sum(tAcd) AS tAcd,
            sum(tAcw) AS tAcw,
            sum(tAgentResponseTime) AS tAgentResponseTime,
            sum(tAnswered) AS tAnswered,
            sum(tContacting) AS tContacting,
            sum(tDialing) AS tDialing,
            sum(tHandle) AS tHandle,
            sum(tHeldComplete) AS tHeldComplete,
            sum(tIvr) AS tIvr,
            sum(tNotResponding) AS tNotResponding,
            sum(tShortAbandon) AS tShortAbandon,
            sum(tTalkComplete) AS tTalkComplete,
            sum(tVoicemail) AS tVoicemail,
            sum(tWait) AS tWait
        FROM (
            SELECT 
                *
            FROM (
                SELECT 
                    conversationId,
                    conversationStart,
                    conversationEnd,
                    originatingDirection,
                    userId,
                    purpose,
                    participantTransactionType,
                    participantTransactionId,
                    mediaType,
                    messageType,
                    direction,
                    ani,
                    dnis,
                    dispositionName,
                    outboundCampaignId,
                    outboundContactId,
                    outboundContactListId,
                    sessionMetric.name AS sessionMetricName,
                    sessionMetric.value AS sessionMetricValue,
                    sessionSegments
                FROM (
                    SELECT 
                        conversationId,
                        conversationStart,
                        conversationEnd,
                        originatingDirection,
                        userId,
                        purpose,
                        participantTransactionType,
                        participantTransactionId,
                        session.mediaType AS mediaType,
                        session.messageType AS messageType,
                        session.direction AS direction,
                        session.ani AS ani,
                        session.dnis AS dnis,
                        session.dispositionName AS dispositionName,
                        session.outboundCampaignId AS outboundCampaignId,
                        session.outboundContactId AS outboundContactId,
                        session.outboundContactListId AS outboundContactListId,
                        explode(session.metrics) AS sessionMetric,
                        session.segments AS sessionSegments
                    FROM (
                        SELECT 
                            conversationId,
                            conversationStart,
                            conversationEnd,
                            originatingDirection,
                            participant.userId AS userId,
                            participant.purpose AS purpose,
                            participant.attributes["transaction_type"] AS participantTransactionType,
                            participant.attributes["transaction_id"] AS participantTransactionId,
                            explode(participant.sessions) AS session
                        FROM (
                            SELECT 
                                conversationId,
                                conversationStart,
                                conversationEnd,
                                originatingDirection,
                                explode(participants) AS participant
                            FROM
                                raw_conversation_details
                            WHERE 
                                recordInsertTime >= '{extract_start_time}' and recordInsertTime < '{extract_end_time}'
                        )
                    )
                )
            )
            pivot (
                sum(sessionMetricValue) FOR sessionMetricName IN (
                    'nBlindTransferred', 
                    'nConnected', 
                    'nConsult', 
                    'nConsultTransferred', 
                    'nError',
                    'nOffered', 
                    'nOutbound', 
                    'nOutboundAbandoned',
                    'nOutboundAttempted', 
                    'nOutboundConnected', 
                    'nOverSla',
                    'nStateTransitionError', 
                    'nTransferred',
                    'tAbandon',
                    'tAcd',
                    'tAcw',
                    'tAgentResponseTime',
                    'tAlert',
                    'tAnswered',
                    'tContacting',
                    'tDialing',
                    'tFlowOut',
                    'tHandle',
                    'tHeld',
                    'tHeldComplete',
                    'tIvr',
                    'tMonitoring',
                    'tNotResponding',
                    'tShortAbandon',
                    'tTalk',
                    'tTalkComplete',
                    'tUserResponseTime',
                    'tVoicemail',
                    'tWait')
            )
        )
        GROUP BY
            conversationId,
            originatingDirection,
            userId,
            purpose,
            participantTransactionId,
            participantTransactionType,
            mediaType,
            messageType,
            direction,
            ani,
            dnis,
            dispositionName,
            outboundCampaignId,
            outboundContactId,
            outboundContactListId,
            element_at(sessionSegments, size(sessionSegments)).queueId,
            element_at(sessionSegments, size(sessionSegments)).wrapUpCode
        ORDER BY
            conversationId
    """


def fact_conversation_evaluations(extract_start_time: str, extract_end_time: str):
    return f"""
        SELECT 
          conversationId,
          conversationDate,
          agentId,
          evaluationId,
          evaluatorId,
          evaluationFormId,
          status,
          assignedDate,
          releaseDate,
          changedDate,
          mediaType,
          agentHasRead,
          questionGroupId,
          questionScore.questionId AS questionId,
          questionScore.answerId AS answerId,
          questionScore.comments AS comments,
          questionScore.failedKillQuestion AS failedKillQuestion,
          questionScore.markedNA AS markedNA,
          questionScore.score AS score
        FROM (
          SELECT
            conversationId,
            conversationDate,
            agentId,
            evaluationId,
            evaluatorId,
            evaluationFormId,
            status,
            assignedDate,
            releaseDate,
            changedDate,
            mediaType,
            agentHasRead,
            questionGroupScore.questionGroupId AS questionGroupId,
            explode(questionGroupScore.questionScores) AS questionScore
          FROM (
            SELECT
                DISTINCT id AS evaluationId,
                conversation.id AS conversationId,
                conversationDate,
                agent.id AS agentId,
                evaluator.id AS evaluatorId,
                evaluationForm.id AS evaluationFormId,
                status,
                assignedDate,
                releaseDate,
                changedDate,
                mediaType[0] AS mediaType,
                agentHasRead,
                explode(answers.questionGroupScores) AS questionGroupScore
            FROM
                raw_evaluations
            WHERE
                recordInsertTime >= '{extract_start_time}' and recordInsertTime < '{extract_end_time}'
          )
        )
    """


def fact_conversation_surveys(extract_start_time: str, extract_end_time: str):
    return f"""
        SELECT 
            conversationId,
            survey.userId AS agentId,
            survey.queueId AS queueId,
            survey.eventTime AS eventTime,
            survey.surveyId AS surveyId,
            survey.surveyStatus AS surveyStatus,
            survey.surveyCompletedDate AS surveyCompletedDate,
            survey.surveyFormContextId AS surveyFormContextId,
            survey.surveyFormId AS surveyFormId,
            survey.surveyFormName AS surveyFormName,
            survey.surveyPromoterScore AS surveyPromoterScore,
            survey.oSurveyTotalScore AS oSurveyTotalScore
        FROM (
            SELECT
                conversationId,
                explode(surveys) AS survey
            FROM
                raw_conversation_details
            WHERE
                recordInsertTime >= '{extract_start_time}' and recordInsertTime < '{extract_end_time}'
        )
        WHERE 
            survey IS NOT NULL
    """


def fact_user_primary_presence(extract_start_time: str, extract_end_time: str):
    return f"""
        SELECT
            userId,
            primaryPresence.startTime AS startTime,
            primaryPresence.endTime AS endTime,
            primaryPresence.systemPresence AS systemPresence
        FROM (
            SELECT
                DISTINCT userId,
                explode(primaryPresence) as primaryPresence
            FROM
                raw_users_details
            WHERE
                recordInsertTime >= '{extract_start_time}' and recordInsertTime < '{extract_end_time}'
        )
    """


def fact_user_routing_status(extract_start_time: str, extract_end_time: str):
    return f"""
        SELECT
            userId,
            routingStatus.startTime AS startTime,
            routingStatus.endTime AS endTime,
            routingStatus.routingStatus AS routingStatus
        FROM (
            SELECT
                DISTINCT userId,
                explode(routingStatus) as routingStatus
            FROM
                raw_users_details
            WHERE
                recordInsertTime >= '{extract_start_time}' and recordInsertTime < '{extract_end_time}'
        )
    """

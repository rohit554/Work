dim_wrapup_codes = """
            select wrapupId, wrapupCode from dim_wrapup_codes
            """

dim_evaluation_form_answer_options = """
            select
            evaluationFormId, questionGroupId, questionId , answerOptionId , answerOptionText , answerOptionValue
             from dim_evaluation_form_answer_options
            """

dim_evaluation_form_question_groups = """
            select
                evaluationFormId , questionGroupId , questionGroupName , defaultAnswersToHighest ,
                defaultAnswersToNA , manualWeight, naEnabled , weight
            from dim_evaluation_form_question_groups
            """

dim_evaluation_form_questions = """
            select
                evaluationFormId, questionGroupId, questionId , questionText , commentsRequired ,
                helpText , isCritical , isKill , naEnabled , questionType
             from dim_evaluation_form_questions
            """

dim_evaluation_forms = """
            select evaluationFormId , evaluationFormName , published  from dim_evaluation_forms
            """

dim_evaluations = """
            select
                evaluationId , evaluatorId , agentId , evaluationFormId , conversationId, conversationDate,
                status , int(date_format(assignedDate, 'yyyyMMdd')) assignedDate,
                int(date_format(assignedDate, 'HHmmss')) assignedTime,
                int(date_format(releaseDate, 'yyyyMMdd')) releaseDate,
                int(date_format(releaseDate, 'HHmmss')) releaseTime,
                mediaType, agentHasRead, neverRelease, queueId, wrapUpCode
             from dim_evaluations
            """

dim_routing_queues = """
            select
                queueId , queueName , wrapupPrompt, callSLDuration , callSLPercentage ,
                callbackSLDuration , callbackSLPercentage ,
                chatSLDuration , chatSLPercentage , emailSLDuration , emailSLPercentage ,
                messageSLDuration , messageSLPercentage
            from dim_routing_queues
            """


dim_user_groups = """
            select userId, groupId, groupName , groupState from dim_user_groups
            """

dim_users = """
            select userName , userId , userFullName , userEmail , userTitle ,
                department , managerId , managerFullName, state 
            from dim_users
            """

fact_conversation_metrics = """
                select
                    agentId , mediaType, messageType , originatingDirection , queueId ,
                    wrapUpCode,
                    date_format(emitDateTime, 'yyyyMMdd') emitDate,
                    int(date_format(emitDateTime, 'HHmmss')) emitTime,
                    sum(nAbandon) nAbandon,
                    sum(nAcd) nAcd,
                    sum(nAcw) nAcw,
                    sum(nAnswered) nAnswered,
                    sum(nBlindTransferred) nBlindTransferred,
                    sum(nConnected) nConnected,
                    sum(nConsult) nConsult,
                    sum(nConsultTransferred) nConsultTransferred,
                    sum(nError) nError,
                    sum(nHandle) nHandle,
                    sum(nHeldComplete) nHeldComplete,
                    sum(nOffered) nOffered,
                    sum(nOutbound) nOutbound,
                    sum(nOutboundAbandoned) nOutboundAbandoned,
                    sum(nOutboundAttempted) nOutboundAttempted,
                    sum(nOutboundConnected) nOutboundConnected,
                    sum(nOverSla) nOverSla,
                    sum(nShortAbandon) nShortAbandon,
                    sum(nTalkComplete) nTalkComplete,
                    sum(nTransferred) nTransferred,
                    sum(tAbandon) tAbandon,
                    sum(tAcd) tAcd,
                    sum(tAcw) tAcw,
                    sum(tAgentResponse) tAgentResponse,
                    sum(tAnswered) tAnswered,
                    sum(tContacting) tContacting,
                    sum(tDialing) tDialing,
                    sum(tHandle) tHandle,
                    sum(tHeldComplete) tHeldComplete,
                    sum(tIvr) tIvr,
                    sum(tNotResponding) tNotResponding,
                    sum(tShortAbandon) tShortAbandon,
                    sum(tTalkComplete) tTalkComplete,
                    sum(tVoicemail) tVoicemail,
                    sum(tWait) tWait
            from fact_conversation_metrics
            where
                emitDate >= date_add(CURRENT_DATE() , -190)
            GROUP BY 
                agentId , mediaType, messageType , originatingDirection , queueId , 
                wrapUpCode, date_format(emitDateTime, 'yyyyMMdd'),
                int(date_format(emitDateTime, 'HHmmss'))
            """

fact_conversation_metrics_per_conversation = """
                SELECT  agentId,
                        mediaType,
                        messageType,
                        originatingDirection,
                        queueId,
                        wrapUpCode,
                        date_format(emitDateTime, 'yyyyMMdd') emitDate,
                        int(date_format(emitDateTime, 'HHmmss')) emitTime,
                        conversationId,
                        sum(nAbandon) nAbandon,
                        sum(nAcd) nAcd,
                        sum(nAcw) nAcw,
                        sum(nAnswered) nAnswered,
                        sum(nBlindTransferred) nBlindTransferred,
                        sum(nConnected) nConnected,
                        sum(nConsult) nConsult,
                        sum(nConsultTransferred) nConsultTransferred,
                        sum(nError) nError,
                        sum(nHandle) nHandle,
                        sum(nHeldComplete) nHeldComplete,
                        sum(nOffered) nOffered,
                        sum(nOutbound) nOutbound,
                        sum(nOutboundAbandoned) nOutboundAbandoned,
                        sum(nOutboundAttempted) nOutboundAttempted,
                        sum(nOutboundConnected) nOutboundConnected,
                        sum(nOverSla) nOverSla,
                        sum(nShortAbandon) nShortAbandon,
                        sum(nTalkComplete) nTalkComplete,
                        sum(nTransferred) nTransferred,
                        sum(tAbandon) tAbandon,
                        sum(tAcd) tAcd,
                        sum(tAcw) tAcw,
                        sum(tAgentResponse) tAgentResponse,
                        sum(tAnswered) tAnswered,
                        sum(tContacting) tContacting,
                        sum(tDialing) tDialing,
                        sum(tHandle) tHandle,
                        sum(tHeldComplete) tHeldComplete,
                        sum(tIvr) tIvr,
                        sum(tNotResponding) tNotResponding,
                        sum(tShortAbandon) tShortAbandon,
                        sum(tTalkComplete) tTalkComplete,
                        sum(tVoicemail) tVoicemail,
                        sum(tWait) tWait
            FROM fact_conversation_metrics
            WHERE emitDate >= date_add(CURRENT_DATE() , -190)
            GROUP BY    agentId,
                        mediaType,
                        messageType,
                        originatingDirection,
                        queueId,
                        wrapUpCode,
                        date_format(emitDateTime, 'yyyyMMdd'),
                        int(date_format(emitDateTime, 'HHmmss')),
                        conversationId
            """

fact_conversation_surveys = """
            SELECT 
                cs.conversationId,
                cs.conversationStart,
                cs.conversationEnd,
                cm.mediaType,
                cs.agentId,
                cs.oSurveyTotalScore AS csat,
                cs.surveyPromoterScore AS nps,
                "" AS fcr,
                case when surveyCompletedDate IS NOT NULL THEN TRUE ELSE FALSE END AS survey_completed,
                case when surveyFormId IS NOT NULL THEN TRUE ELSE FALSE END AS survey_initiated,
                eventTime AS insertTimestamp
            FROM 
                fact_conversation_surveys AS cs
            LEFT JOIN (
              SELECT
                DISTINCT conversationId,
                mediaType
              FROM
                fact_conversation_metrics
              WHERE
                mediaType IS NOT NULL
              ) AS cm
            ON 
              cs.conversationId = cm.conversationId
            WHERE
              eventDate >= date_add(CURRENT_DATE() , -190)
            """

fact_evaluation_question_group_scores = """
            select
                evaluationId , questionGroupId , markedNA , maxTotalCriticalScore , maxTotalCriticalScoreUnweighted ,
                maxTotalNonCriticalScore , maxTotalNonCriticalScoreUnweighted , maxTotalScore , maxTotalScoreUnweighted,
                totalCriticalScore , totalCriticalScoreUnweighted , totalNonCriticalScore ,
                totalNonCriticalScoreUnweighted ,totalScore , totalScoreUnweighted
            from fact_evaluation_question_group_scores
            """

fact_evaluation_question_scores = """
            select
                evaluationId , questionGroupId , questionId , answerId , failedKillQuestion , markedNA , score
            from fact_evaluation_question_scores
            """

fact_evaluation_total_scores = """
            select
                evaluationId , totalCriticalScore , totalNonCriticalScore , totalScore
            from fact_evaluation_total_scores
            """

fact_primary_presence = """
            select
                userId ,
                int(date_format(startTime, 'yyyyMMdd')) startDate,
                int(date_format(startTime, 'HHmmss')) startTime,
                int(date_format(endTime, 'yyyyMMdd')) endDate,
                int(date_format(endTime, 'HHmmss')) endTime,
                systemPresence
            from fact_primary_presence
            """

fact_routing_status = """
            select
                userId ,
                int(date_format(startTime, 'yyyyMMdd')) startDate,
                int(date_format(startTime, 'HHmmss')) startTime,
                int(date_format(endTime, 'yyyyMMdd')) endDate,
                int(date_format(endTime, 'HHmmss')) endTime,
                routingStatus
            from fact_routing_status
            """

fact_wfm_actuals = """
            select
                userId ,
                managementUnitId,
                int(date_format(startDate, 'yyyyMMdd')) startDate,
                int(date_format(startDate, 'HHmmss')) startTime,
                int(date_format(endDate, 'yyyyMMdd')) endDate,
                int(date_format(endDate, 'HHmmss')) endTime,
                actualActivityCategory, (to_unix_timestamp(endDate) - to_unix_timestamp(startDate)) duration
            from fact_wfm_actuals
            """

fact_wfm_day_metrics = """
            select
                userId,
                managementUnitId,
                int(date_format(startDate, 'yyyyMMdd')) startDate,
                int(date_format(startDate, 'HHmmss')) startTime,
                int(date_format(endDate, 'yyyyMMdd')) endDate,
                int(date_format(endDate, 'HHmmss')) endTime,
                actualLengthSecs , adherenceScheduleSecs,
                conformanceActualSecs , conformanceScheduleSecs,
                dayStartOffsetSecs , exceptionCount , exceptionDurationSecs , impactSeconds , scheduleLengthSecs, impact
            from fact_wfm_day_metrics
            """

fact_wfm_exceptions = """
            select
                userId,
                managementUnitId,
                int(date_format(startDate, 'yyyyMMdd')) startDate,
                int(date_format(startDate, 'HHmmss')) startTime,
                int(date_format(endDate, 'yyyyMMdd')) endDate,
                int(date_format(endDate, 'HHmmss')) endTime,
                actualActivityCategory, impact, routingStatus, scheduledActivityCategory,
                scheduledActivityCodeId, systemPresence, secondaryPresenceId
            from fact_wfm_exceptions
            """

dim_presence_definitions = """
                select id, name, deactivated, primary, coalesce(get_json_object(languageLabels, '$.en'), 
                    get_json_object(languageLabels, '$.en_US')) as label  from raw_presence_definitions
                """

dim_bu_mu_mapping = """
                    select a.id as managementUnitId, a.name as managementUnitName, b.id as businessUnitId, 
                        b.name as businessUnitName, b.division.id as divisionId
                    from raw_management_units a, raw_business_units  b
                        where a.businessUnit.id = b.id
                    """

fact_wfm_forecast = """ \
    SELECT FORECAST_ID, \
            PLANNING_GROUP_ID, \
                BU_ID as BUSINESS_UNIT_ID, \
                    int(date_format(IntervalStart, 'yyyyMMdd')) IntervalStartDate, \
                        int(date_format(IntervalStart, 'HHmmss')) IntervalStartTime, \
                            int(date_format(WEEKDATE, 'yyyyMMdd')) WEEKDATE, \
                                PLANNING_GROUP, \
                                    int(date_format(DATA_REF_DT, 'yyyyMMdd')) DATA_REF_DATE, \
                                        int(date_format(DATA_REF_DT, 'HHmmss')) DATA_REF_TIME, \
                                            BU_NAME, \
                                                averageHandleTimeSeconds, \
                                                    offered, \
                                                        int(date_format(META_REF_DT, 'yyyyMMdd')) META_REF_DATE, \
                                                            int(date_format(META_REF_DT, 'HHmmss')) META_REF_TIME \
                                                                FROM fact_wfm_forecast \
;"""

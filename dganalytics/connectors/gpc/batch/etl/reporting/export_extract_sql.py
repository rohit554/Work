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
                evaluationId , evaluatorId , agentId , evaluationFormId ,
                status , date_format(assignedDate, 'yyyyMMdd') assignedDate,
                int(date_format(assignedDate, 'HHmmss')) assignedTime,
                date_format(releaseDate, 'yyyyMMdd') releaseDate,
                int(date_format(releaseDate, 'HHmmss')) releaseTime,
                mediaType, neverRelease
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
                date_format(startTime, 'yyyyMMdd') startDate,
                int(date_format(startTime, 'HHmmss')) startTime,
                date_format(endTime, 'yyyyMMdd') endDate,
                int(date_format(endTime, 'HHmmss')) endTime,
                systemPresence
            from fact_primary_presence
            """

fact_routing_status = """
            select
                userId ,
                date_format(startTime, 'yyyyMMdd') startDate,
                int(date_format(startTime, 'HHmmss')) startTime,
                date_format(endTime, 'yyyyMMdd') endDate,
                int(date_format(endTime, 'HHmmss')) endTime,
                routingStatus
            from fact_routing_status
            """

fact_wfm_actuals = """
            select
                userId ,
                managementUnitId,
                date_format(from_unixtime(unix_timestamp(startDate) + startOffsetSeconds), 'yyyyMMdd') startDate,
                int(date_format(from_unixtime(unix_timestamp(startDate) + startOffsetSeconds), 'yyyyMMdd')) startTime,
                date_format(from_unixtime(unix_timestamp(endDate) + startOffsetSeconds), 'yyyyMMdd') endDate,
                int(date_format(from_unixtime(unix_timestamp(endDate) + startOffsetSeconds), 'yyyyMMdd')) endTime,
                actualActivityCategory, (endOffsetSeconds - startOffsetSeconds) duration
            from fact_wfm_actuals
            """

fact_wfm_day_metrics = """
            select
                userId,
                managementUnitId,
                date_format(startDate, 'yyyyMMdd') startDate,
                int(date_format(startDate, 'HHmmss')) startTime,
                date_format(endDate, 'yyyyMMdd') endDate,
                int(date_format(endDate, 'HHmmss')) endTime,
                actualLengthSecs , adherenceScheduleSecs,
                conformanceActualSecs , conformanceScheduleSecs,
                dayStartOffsetSecs , exceptionCount , exceptionDurationSecs , impactSeconds , scheduleLengthSecs
            from fact_wfm_day_metrics
            """

dim_presence_definitions = """
                select id, name, deactivated, primary, get_json_object(languageLabels, '$.en') as label  from raw_presence_definitions
                """

dim_bu_mu_mapping = """
                    select a.id as managementUnitId, a.name as managementUnitName, b.id as businessUnitId, 
                        b.name as businessUnitName, b.division.id as divisionId
                    from raw_management_units a, raw_business_units  b
                        where a.businessUnit.id = b.id
                    """
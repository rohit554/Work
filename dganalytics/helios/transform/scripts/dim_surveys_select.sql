SELECT conversation.id conversationId,
id surveyId,
status surveyStatus,
surveyForm.contextId surveyFormContextId,
surveyForm.id surveyFormId,
surveyForm.name surveyFormName,
agent.id agentId,
C.conversationStartDateId
FROM (select *from gpc_{tenant}.raw_surveys WHERE
                extractDate = '{extract_date}'
                AND extractIntervalStartTime = '{extract_start_time}'
                AND extractIntervalEndTime = '{extract_end_time}'
    )S

INNER JOIN dgdm_{tenant}.dim_conversations C
    ON S.conversation.id = C.conversationId
        AND c.conversationStartDateId < date_format(S.extractDate, 'yyyyMMdd')
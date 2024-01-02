SELECT conversation.id conversationId,
        id surveyId,
        queue.id queueId,
        agent.id userId,
        surveyForm.id surveyFormId,
        completedDate SurveyCompletionDate,
        answers.npsScore promoterScore,
        10 maxPromoterScore,
        answers.totalScore satisfactionScore,
        100 maxSatisfactionScore,
        null isResolved,
        null isFirstTimeResolution,
        C.conversationStartDateId
FROM (SELECT * FROM gpc_{tenant}.raw_surveys 
          WHERE extractDate = '{extract_date}'
          AND extractIntervalStartTime = '{extract_start_time}'
          AND extractIntervalEndTime = '{extract_end_time}'
      ) S
JOIN (SELECT conversationId, conversationStartDateId FROM dgdm_{tenant}.dim_conversations) C
    ON S.conversation.id = C.conversationId
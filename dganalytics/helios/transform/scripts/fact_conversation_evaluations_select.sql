SELECT conversation.id conversationId,
        id evaluationId,
        evaluator.id evaluatorId,
        agent.id agentId,
        assignedDate,
        releaseDate,
        changedDate,
        answers.totalCriticalScore,
        answers.totalNonCriticalScore,
        answers.totalScore,
        D.dateId conversationStarDateId
FROM (SELECT * FROM gpc_{tenant}.raw_evaluations 
        where extractDate = '{extract_date}'
  ) E
JOIN dgdm_{tenant}.dim_date D
    ON CAST(conversationDate AS date) = D.dateVal

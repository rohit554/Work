SELECT 
conversation.id conversationId,
id evaluationId,
evaluationForm.id evaluationFormId,
evaluationForm.name evaluationFormName,
status,
agentHasRead,
answers.anyFailedKillQuestions,
answers.comments,
evaluationForm.published evaluationFormPublished,
neverRelease,
resourceType,
D.dateId conversationStarDateId
FROM (SELECT * FROM gpc_{tenant}.raw_evaluations 
            where extractDate = '{extract_date}'
      ) E
JOIN dgdm_{tenant}.dim_date D
    ON CAST(conversationDate AS date) = D.dateVal

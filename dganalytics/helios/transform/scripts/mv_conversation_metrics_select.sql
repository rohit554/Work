SELECT
  D.conversationId,
  mediaTypeId,
  D.conversationStartDateId,
  userId,
  nBlindTransferred,
  nConsultTransferred,
  nConsult,
  nTransferred,
  CASE
    WHEN COALESCE(LOWER(T.resolved), '') IN ('resolved', 'partially resolved') THEN 1
    ELSE 0
  END AS resolved,
  CASE
    WHEN COALESCE(LOWER(T.resolved), '') = 'not resolved' THEN 1
    ELSE 0
  END AS notResolved,
  tHandle,
  CASE
    WHEN COALESCE(LOWER(T.resolved), "") In ('resolved', 'partially resolved') then tHandle
    ELSE 0
  end AS tHandleResolved,
  CASE
    WHEN COALESCE(LOWER(T.resolved), "") = "not resolved" then tHandle
    ELSE 0
  end AS tHandleNotResolved,
  tTalkComplete,
  tHeldComplete,
  tAcwComplete,
  CASE
    WHEN LOWER(T.satisfaction) = 'lowest' THEN 20
    WHEN LOWER(T.satisfaction) = 'low' THEN 40
    WHEN LOWER(T.satisfaction) = 'medium' THEN 60
    WHEN LOWER(T.satisfaction) = 'high' THEN 80
    WHEN LOWER(T.satisfaction) = 'highest' THEN 100
    ELSE 0
  END AS satisfaction,
	tAnswered
FROM
  (
    SELECT
      C.conversationId,
      C.mediaTypeId,
      C.conversationStartDateId,
      P.userId userId,
      SUM(C.nBlindTransferred) nBlindTransferred,
      SUM(C.nConsultTransferred) nConsultTransferred,
      SUM(C.nConsult) nConsult,
      SUM(C.nTransferred) nTransferred,
      SUM(C.tHandle) tHandle,
      SUM(C.tTalkComplete) tTalkComplete,
      SUM(C.tHeldComplete) tHeldComplete,
      SUM(C.tAcw) tAcwComplete,
      SUM(C.tAnswered) tAnswered
    FROM
      (
        SELECT
          *
        FROM
          dgdm_{tenant}.fact_conversation_metrics PIVOT(
            SUM(value) for name in (
              'nBlindTransferred',
              'nConsultTransferred',
              'nConsult',
              'nTransferred',
              'tHandle',
              'tTalkComplete',
              'tHeldComplete',
              'tAcw',
							'tAnswered'
            )
          )
      ) C
      JOIN dgdm_{tenant}.dim_conversation_participants P ON C.conversationStartDateId = P.conversationStartDateId
      AND C.conversationId = P.conversationId
      AND C.participantId = P.participantId
    WHERE
      P.userId IS NOT NULL
      AND NOT(
        nBlindTransferred IS NULL
        AND nConsultTransferred IS NULL
        AND nConsult IS NULL
        AND nTransferred IS NULL
        AND tHandle IS NULL
        AND tTalkComplete IS NULL
        AND tHeldComplete IS NULL
        AND tAcw IS NULL
      )
    GROUP BY
      C.conversationId,
      C.mediaTypeId,
      C.conversationStartDateId,
      P.userId
  ) D
  LEFT JOIN dgdm_{tenant}.fact_transcript_insights T ON D.conversationId = T.conversationId
WHERE
  D.conversationStartDateId > (select dateId from dgdm_{tenant}.dim_date where dateVal = cast(DATEADD(DAY, -3, '{extract_date}') as date))
GROUP BY
  D.conversationId,
  mediaTypeId,
  D.conversationStartDateId,
  userId,
  nBlindTransferred,
  nConsultTransferred,
  nConsult,
  nTransferred,
  tHandle,
  tTalkComplete,
  tHeldComplete,
  tAcwComplete,
  resolved,
  satisfaction,
	tAnswered
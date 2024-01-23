SELECT
  V.*,
  A.tHandle
FROM
  (
    SELECT
      D.conversationId,
      D.conversationStartDateId,
      D.initialSessionMediaTypeId mediaTypeId,
      D.location region,
      F.contactReason,
      F.mainInquiry,
      F.rootCause,
      F.inquiry_type,
      T.resolved,
      T.satisfaction,
      collect_list(DISTINCT S.queueId) queueIds
    FROM
      dgdm_{tenant}.dim_conversations D
      JOIN dgdm_{tenant}.dim_conversation_session_segments S ON D.conversationStartDateId = S.conversationStartDateId
      AND D.conversationId = S.conversationId
      LEFT JOIN dgdm_{tenant}.fact_transcript_insights T ON D.conversationStartDateId = S.conversationStartDateId
      AND D.conversationId = T.conversationId
      LEFT JOIN dgdm_{tenant}.fact_transcript_contact_reasons F ON D.conversationStartDateId = S.conversationStartDateId
      AND D.conversationId = F.conversationId
    GROUP BY
      D.conversationStartDateId,
      D.conversationId,
      D.initialSessionMediaTypeId,
      D.location,
       F.contactReason,
      F.mainInquiry,
      F.rootCause,
      F.inquiry_type,
      T.resolved,
      T.satisfaction
  ) as V
  JOIN (
    SELECT
      C.conversationId,
      C.mediaTypeId,
      SUM(C.tHandle) tHandle
    FROM
      (
        SELECT
          *
        FROM
          dgdm_{tenant}.fact_conversation_metrics PIVOT(
            SUM(value) for name in ('tHandle')
          )
      ) C
    WHERE
      NOT(tHandle IS NULL)
    GROUP BY
      C.conversationId,
      C.mediaTypeId,
      C.conversationStartDateId
  ) A ON A.conversationId = V.conversationId
WHERE
  conversationStartDateId > (select dateId from dgdm_{tenant}.dim_date where dateVal = cast(DATEADD(DAY, -3, '{extract_date}') as date))
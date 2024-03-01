INSERT INTO dgdm_{tenant}.mv_classification
SELECT
*,
CASE
WHEN businessValue = 0
AND customerValue = 0 THEN "Eliminate"
WHEN businessValue = 0
AND customerValue = 1 THEN "Digitize"
WHEN businessValue = 1a
AND customerValue = 0 THEN "Simplify"
WHEN businessValue = 1
AND customerValue = 1 THEN "Encourage"
END AS category
FROM
  (
    SELECT
      CL.contactReason,
      CL.mainInquiry,
      CL.mediaTypeId,
      (tHandle / (totalInteractions * 1000)) tHandle,
      CL.additionalService,
      CL.resolved,
      CL.totalInteractions,
      CASE
        WHEN (additionalService * 100) / totalInteractions >= 10 THEN 1
        ELSE 0
      END AS businessValue,
      CASE
        WHEN (resolved * 100) / totalInteractions >= 50
        AND 
          (tHandle / (totalInteractions * 1000) < 400) THEN 1
          ELSE 0
        END AS customerValue
        FROM
          (
            SELECT
              contactReason,
              mainInquiry,
              mediaTypeId,
              SUM(TRY_CAST(tHandle AS DOUBLE)) tHandle,
              COUNT(
                DISTINCT(
                  CASE
                    WHEN lower(additional_service) = "yes" THEN conversationId
                  END
                )
              ) additionalService,
              COUNT(
                DISTINCT(
                  CASE
                    WHEN lower(resolved) In ("resolved", "partially resolved") THEN conversationId
                  END
                )
              ) resolved,
              COUNT(DISTINCT conversationId) totalInteractions
            FROM
              (
                SELECT
                  F.*,
                  T.mainInquiry,
                  T.contactReason,
                  A.tHandle,
                  A.mediaTypeId
                FROM
                  dgdm_{tenant}.fact_transcript_insights F
                  JOIN dgdm_{tenant}.fact_transcript_contact_reasons T ON T.conversationId = F.conversationId
                  JOIN (
                    SELECT
                      C.conversationId,
                      C.mediaTypeId,
                      C.conversationStartDateId,
                      SUM(TRY_CAST(C.tHandle AS DOUBLE)) tHandle
                    FROM
                      (
                        SELECT
                          *
                        FROM
                          dgdm_{tenant}.fact_conversation_metrics PIVOT(sum(value) FOR name IN ('tHandle'))
                      ) C
                    WHERE
                      NOT(tHandle IS NULL)
                    GROUP BY
                      C.conversationId,
                      C.mediaTypeId,
                      C.conversationStartDateId
                  ) A ON A.conversationId = F.conversationId
                WHERE
                  F.conversationStartDateId > (select dateId from dgdm_{tenant}.dim_date where dateVal = cast(DATEADD(DAY, -30, '{extract_date}') as date))
              )
            GROUP BY
              contactReason,
              mainInquiry,
              mediaTypeId
          ) as CL
      )
    
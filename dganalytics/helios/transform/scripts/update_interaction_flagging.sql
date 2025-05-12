with CTE as (
  select
    CS.conversationId,
    CS.conversationStartDateId,
    complianceScore,
    wrapUpCode
  from
    (
      select
        conversationId,
        conversationStartDateId,
        ((num * 100) / denom) complianceScore
      from
        (
          select
            conversationId,
            COUNT(DISTINCT question) AS denom,
            SUM(
              CASE
                WHEN answer = 'yes' THEN 1
                ELSE 0
              END
            ) AS num,
            conversationStartDateId
          from
            (
              select
                *
              from
                dgdm_{tenant}.fact_transcript_compliance
              where
                conversationStartDateId >= 20250101
            )
          group by
            conversationId,
            conversationStartDateId
        )
      where
        ((num * 100) / denom) < 100
    ) CS
      JOIN (
        select
          conversationId,
          conversationStartDateId,
          wrapUpCode
        from
          (
            SELECT
              s.conversationId,
              s.conversationStartDateId,
              s.wrapUpCodeId AS finalWrapupCode
            FROM
              (
                SELECT
                  conversationId,
                  wrapUpCodeId,
                  conversationStartDateId,
                  ROW_NUMBER() OVER (
                      PARTITION BY conversationStartDateId, conversationId, segmentType
                      ORDER BY segmentEnd DESC
                    ) AS rn
                FROM
                  dgdm_{tenant}.dim_conversation_session_segments
                WHERE
                  segmentType = 'wrapup'
                  and wrapUpCodeId is not null
              ) AS s
            WHERE
              s.rn = 1
          ) C
            join dgdm_{tenant}.dim_wrap_up_codes w
              on C.finalWrapupCode = w.wrapUpId
        where
          w.wrapupCode IN (
            'Z-OUT-Success-1Week',
            'Z-OUT-Success-2Week',
            'Z-OUT-Success-3Week',
            'Z-OUT-Success-4Week',
            'Z-OUT-Success-5+Week-TL APPROVED'
          )
      ) seg
        on CS.conversationId = seg.conversationId
        and CS.conversationStartDateId = seg.conversationStartDateId
) update
  dgdm_{tenant}.dim_conversations C
set
  C.isManualAssessmentNeeded = TRUE
where
  exists (
    select
      1
    from
      CTE
    where
      C.conversationId = CTE.conversationId
      and C.conversationStartDateId = CTE.conversationStartDateId
  )
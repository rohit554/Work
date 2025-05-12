select
  SPLIT(userId, ',') AS userIds,
  metricDate,
  sum(num) noOfYesAnswer,
  sum(denom) totalQuestions,
  originatingDirectionId,
  mediaTypeId,
  finalWrapupCode finalWrapupCodeId,
  queueId finalQueueId,
  resolved,
  process_name scenarioName,
  location,
  conversationStartDateId
from
  (
    select
      C.conversationId,
      num,
      denom,
      finalWrapupCode,
      ss.queueId,
      seg.userId,
      dc.originatingDirectionId,
      dc.initialSessionMediaTypeId mediaTypeId,
      CAST(
        date_format(
          FROM_UTC_TIMESTAMP(
            to_utc_timestamp(dc.conversationStart, 'UTC'),
            timezone
          ),
          'yyyyMMdd'
        ) AS INT
      ) conversationStartDateId,
      label process_name,
      f.resolved,
      mql.Region location,
      D.dateVal metricDate
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
              dgdm_{tenant}.fact_transcript_conformance
            where
              conversationStartDateId >= (
                select
                  dateId
                from
                  dgdm_{tenant}.dim_date
                where
                  dateVal = date_sub(CAST('{extract_date}' AS DATE), 7)
              )
          )
        group by
          conversationId,
          conversationStartDateId
      ) C
      JOIN (
        SELECT
          s.conversationId,
          s.conversationStartDateId,
          s.wrapUpCodeId AS finalWrapupCode,
          userId
        FROM
          (
            SELECT
              conversationId,
              wrapUpCodeId,
              conversationStartDateId,
              participantId,
              ROW_NUMBER() OVER (
                PARTITION BY conversationStartDateId,
                conversationId,
                segmentType
                ORDER BY
                  segmentEnd DESC
              ) AS rn
            FROM
              dgdm_{tenant}.dim_conversation_session_segments
            WHERE
              segmentType = 'wrapup'
              AND wrapUpCodeId IS NOT NULL AND wrapUpCodeId in (select distinct wrapUpId from dgdm_hellofresh.mv_wrap_up_codes)
          ) AS s
          JOIN dgdm_{tenant}.dim_conversation_participants p ON s.conversationId = P.conversationId
          AND s.participantId = p.participantId
          AND s.conversationStartDateId = P.conversationStartDateId
        WHERE
          s.rn = 1
      ) seg ON C.conversationId = seg.conversationId
      AND C.conversationStartDateId = seg.conversationStartDateId
      JOIN (
        SELECT
          conversationId,
          queueId,
          conversationStartDateId
        FROM
          (
            SELECT
              s.conversationId,
              s.queueId,
              s.conversationStartDateId,
              ROW_NUMBER() OVER (
                PARTITION BY conversationStartDateId,
                conversationId
                ORDER BY
                  segmentEnd DESC
              ) AS rn
            FROM
              dgdm_{tenant}.dim_conversation_session_segments s
            WHERE
              s.queueId IS NOT NULL
          )
        WHERE
          rn = 1
      ) ss ON c.conversationId = ss.conversationId
      AND c.conversationStartDateId = ss.conversationStartDateId
      AND seg.conversationId = ss.conversationId
      AND seg.conversationStartDateId = ss.conversationStartDateId
      JOIN dgdm_{tenant}.mv_queue_language mql ON ss.queueId = mql.queueId
      JOIN dgdm_{tenant}.dim_conversations dc on c.conversationId = dc.conversationId
      and c.conversationStartDateId = dc.conversationStartDateId
      and dc.initialSessionMediaTypeId = 1
      join dgdm_{tenant}.dim_user_group_region_mapping u on seg.userId = u.userId
      JOIN dgdm_{tenant}.fact_transcript_insights f on c.conversationId = f.conversationId
      and c.conversationStartDateId = f.conversationStartDateId
      left join dgdm_{tenant}.fact_transcript_contact_reasons cr ON C.conversationId = cr.conversationId
      AND C.conversationStartDateId = cr.conversationStartDateId
      left join dgdm_{tenant}.label_classification l on cr.root_cause_raw = l.phrase
      and l.type = 'root_cause'
      JOIN dgdm_{tenant}.dim_date D on c.conversationStartDateId = D.dateId
  )
group by
  userId,
  finalWrapupCode,
  queueId,
  originatingDirectionId,
  mediaTypeId,
  conversationStartDateId,
  process_name,
  resolved,
  location,
  metricDate

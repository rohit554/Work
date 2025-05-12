select
  conversationId,
  d.dateVal metricDate,
  conversationStartDateId,
  originatingDirectionId,
  mediaTypeId,
  location,
  finalWrapupCodeId,
  SPLIT(userId, ',') AS userIds,
  finalQueueId,
  scenarioName,
  resolved,
  isManualAssessmentNeeded,
  coalesce(hasManuallyEvaluated, false) hasManuallyEvaluated
from
  (
    select
      C.conversationId,
      CAST(
        date_format(
          FROM_UTC_TIMESTAMP(
            to_utc_timestamp(c.conversationStart, 'UTC'),
            timezone
          ),
          'yyyyMMdd'
        ) AS INT
      ) conversationStartDateId,
      originatingDirectionId,
      initialSessionMediaTypeId mediaTypeId,
      mql.Region location,
      isManualAssessmentNeeded,
      hasManuallyEvaluated,
      seg.finalWrapupCodeId,
      seg.userId,
      ss.queueId finalQueueId,
      label scenarioName,
      T.resolved
    from
      dgdm_{tenant}.dim_conversations C
      JOIN (
        SELECT
          s.conversationId,
          s.conversationStartDateId,
          s.wrapUpCodeId AS finalWrapupCodeId,
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
              AND wrapUpCodeId IS NOT NULL and wrapUpCodeId in (select distinct wrapUpId from dgdm_hellofresh.mv_wrap_up_codes)
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
      JOIN dgdm_{tenant}.fact_transcript_insights T ON C.conversationId = T.conversationId
      AND C.conversationStartDateId = T.conversationStartDateId
      left join dgdm_{tenant}.fact_transcript_contact_reasons cr ON C.conversationId = cr.conversationId
      AND C.conversationStartDateId = cr.conversationStartDateId
      left join dgdm_{tenant}.label_classification l on cr.root_cause_raw = l.phrase
      and l.type = 'root_cause'
      JOIN dgdm_{tenant}.dim_user_group_region_mapping u on seg.userId = u.userId
    where
      C.isManualAssessmentNeeded = TRUE
      AND C.conversationStartDateId >= (
        select
          dateId
        from
          dgdm_{tenant}.dim_date
        where
          dateVal = date_sub(CAST('{extract_date}' AS DATE), 7)
      ) AND C.initialSessionMediaTypeId = 1
  ) CT
  JOIN dgdm_{tenant}.dim_date d on CT.conversationStartDateId = d.dateId

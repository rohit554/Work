select distinct
  conversationId,
  case
    when timezone is not null then FROM_UTC_TIMESTAMP(
      conversationStart,
      timezone
    )
    ELSE conversationStart
  END as conversationStart,
  case
    when timezone is not null then FROM_UTC_TIMESTAMP(
      conversationEnd,
      timezone
    )
    ELSE conversationEnd
  END as conversationEnd,
  originatingDirectionId,
  mediaTypeId,
  location `location`,
  userIds,
  finalQueue finalQueueId,
  finalWrapupCode finalWrapupCodeId,
  label scenarioName,
  resolved,
  case
    when timezone is null then CAST(
      date_format(conversationStart, 'yyyyMMdd') AS INT
    )
    ELSE CAST(
      date_format(
        FROM_UTC_TIMESTAMP(conversationStart, timezone),
        'yyyyMMdd'
      ) AS INT
    )
  END AS conversationStartDateId
from
  (
    select
      c.conversationId,
      to_utc_timestamp(c.conversationStart, 'UTC') conversationStart,
      to_utc_timestamp(c.conversationEnd, 'UTC') conversationEnd,
      originatingDirectionId,
      initialSessionMediaTypeId mediaTypeId,
      qrm.Region location,
      p.userIds,
      qrm.queueId as finalQueue,
      ssw.finalWrapupCode,
      l.label,
      i.resolved,
      rm.timezone
    from
      dgdm_{tenant}.dim_conversations c
      join (
        select
          conversationId,
          conversationStartDateId,
          collect_list(DISTINCT userId) AS userIds
        from
          dgdm_{tenant}.dim_conversation_participants
        where
          userId is not null
        group by
          conversationId,
          conversationStartDateId
      ) p on c.conversationId = p.conversationId
      and p.conversationStartDateId = c.conversationStartDateId
      join (
        select
          conversationId,
          queueId,
          conversationStartDateId
        from
          (
            select
              s.conversationId,
              s.queueId,
              s.conversationStartDateId,
              ROW_NUMBER() OVER (
                PARTITION BY conversationStartDateId,
                conversationId
                ORDER BY
                  segmentEnd DESC
              ) AS rn
            from
              dgdm_{tenant}.dim_conversation_session_segments s
            where
              s.queueId is not null
          )
        where
          rn = 1
      ) ss on c.conversationId = ss.conversationId
      and c.conversationStartDateId = ss.conversationStartDateId
      and p.conversationId = ss.conversationId
      and p.conversationStartDateId = ss.conversationStartDateId
      join (
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
              and wrapUpCodeId is not null and wrapUpCodeId in (select distinct wrapUpId from dgdm_{tenant}.mv_wrap_up_codes)
          ) AS s
        WHERE
          s.rn = 1
      ) ssw on c.conversationId = ssw.conversationId
      and c.conversationStartDateId = ssw.conversationStartDateId
      and p.conversationId = ssw.conversationId
      and p.conversationStartDateId = ssw.conversationStartDateId
      left join dgdm_{tenant}.fact_transcript_insights i on c.conversationId = i.conversationId
      and c.conversationStartDateId = i.conversationStartDateId
      left join dgdm_{tenant}.fact_transcript_contact_reasons cr ON c.conversationId = cr.conversationId
      AND c.conversationStartDateId = cr.conversationStartDateId
      left join dgdm_{tenant}.label_classification l on cr.root_cause_raw = l.phrase
      and l.type = 'root_cause'
      JOIN dgdm_{tenant}.mv_queue_language qrm ON ss.queueId = qrm.queueId
      JOIN dgdm_{tenant}.dim_queue_region_mapping rm ON qrm.queueId = rm.queueId and ss.queueId = rm.queueId
    where
      c.conversationStartDateId >= (
        select
          dateId
        from
          dgdm_{tenant}.dim_date
        where
          dateVal = date_sub(CAST('{extract_date}' AS DATE), 7)
      ) and c.initialSessionMediaTypeId = 1
  )

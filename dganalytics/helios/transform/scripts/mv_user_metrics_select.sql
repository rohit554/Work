with CTE as (
  select
    conversationStartDateId,
    originatingDirectionId,
    mediaTypeId,
    userId,
    queueId,
    finalWrapupCode,
    resolved,
    scenarioName,
    sum(tHandle) tHandle,
    sum(tHeldComplete) tHeldComplete,
    sum(tAcwComplete) tAcwComplete,
    location,
    count(*) nHandle
  from
    (
      SELECT
        C.conversationId,
        CAST(
          date_format(
            FROM_UTC_TIMESTAMP(conversationStart, timezone),
            'yyyyMMdd'
          ) AS INT
        ) conversationStartDateId,
        FROM_UTC_TIMESTAMP(conversationStart, ut.timezone) AS conversationStart,
        FROM_UTC_TIMESTAMP(conversationEnd, ut.timezone) AS conversationEnd,
        originatingDirectionId,
        initialSessionMediaTypeId mediaTypeId,
        seg.userId,
        ss.queueId,
        seg.finalWrapupCode,
        metric.resolved,
        scenarioName,
        coalesce(metric.tHandle, 0) tHandle,
        coalesce(tHeldComplete, 0) tHeldComplete,
        coalesce(tAcwComplete, 0) tAcwComplete,
        metric.satisfaction,
        mql.region location
      FROM
        dgdm_{tenant}.dim_conversations C
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
                AND wrapUpCodeId IS NOT NULL AND wrapUpCodeId IN (SELECT DISTINCT wrapUpId FROM dgdm_hellofresh.mv_wrap_up_codes)
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
        
        JOIN (
          SELECT
            D.conversationId,
            D.conversationStartDateId,
            userId,
            label AS scenarioName,
            T.resolved,
            tHandle,
            tHeldComplete,
            tAcwComplete,
            T.satisfaction
          FROM
            (
              SELECT
                C.conversationId,
                C.conversationStartDateId,
                P.userId AS userId,
                SUM(C.tHandle) AS tHandle,
                SUM(C.tHeldComplete) AS tHeldComplete,
                SUM(C.tAcw) AS tAcwComplete
              FROM
                (
                  SELECT
                    *
                  FROM
                    dgdm_{tenant}.fact_conversation_metrics PIVOT(
                      SUM(value) FOR name IN ('tHandle', 'tHeldComplete', 'tAcw')
                    )
                ) C
                JOIN dgdm_{tenant}.dim_conversation_participants P ON C.conversationStartDateId = P.conversationStartDateId
                AND C.conversationId = P.conversationId
                AND C.participantId = P.participantId
              WHERE
                P.userId IS NOT NULL
                AND NOT (
                  tHandle IS NULL
                  AND tHeldComplete IS NULL
                  AND tAcw IS NULL
                )
              GROUP BY
                C.conversationId,
                C.conversationStartDateId,
                P.userId
            ) D
            LEFT JOIN dgdm_{tenant}.fact_transcript_insights T ON D.conversationId = T.conversationId
            AND D.conversationStartDateId = T.conversationStartDateId
            left join dgdm_{tenant}.fact_transcript_contact_reasons cr ON D.conversationId = cr.conversationId
            AND D.conversationStartDateId = cr.conversationStartDateId
            left join dgdm_{tenant}.label_classification l on cr.root_cause_raw = l.phrase
            and l.type = 'root_cause'
          GROUP BY
            D.conversationId,
            D.conversationStartDateId,
            userId,
            tHandle,
            tHeldComplete,
            tAcwComplete,
            resolved,
            scenarioName,
            satisfaction
        ) metric ON metric.conversationId = C.conversationId
        AND metric.conversationStartDateId = C.conversationStartDateId
        AND metric.userId = seg.userId
        JOIN user_timezones ut ON seg.userId = ut.userId
        AND metric.userId = ut.userId
      WHERE
        C.conversationStartDateId >= (
          select
            dateId
          from
            dgdm_{tenant}.dim_date
          where
            dateVal = date_sub(CAST('{extract_date}' AS DATE), 7)
        ) AND C.initialSessionMediaTypeId = 1
    )
  group by
    conversationStartDateId,
    originatingDirectionId,
    mediaTypeId,
    userId,
    queueId,
    finalWrapupCode,
    resolved,
    scenarioName,
    location
)
select SPLIT(userId, ',') AS userIds,D.dateVal metricDate,kpiName,num,denom,scenarioName,resolved,originatingDirectionId,mediaTypeId,finalWrapupCode finalWrapupCodeId,queueId finalQueueId,location 
from
  (
    select
      conversationStartDateId,
      originatingDirectionId,
      mediaTypeId,
      userId,
      queueId,
      finalWrapupCode,
      resolved,
      scenarioName,
      location,
      'sales' kpiName,
      case
        when ow.sales then nHandle
        else 0
      end as num,
      1 denom
    from
      CTE ce
      left join outbound_wrap_codes ow on ce.finalWrapupCode = ow.wrapupId
    union
    select
      conversationStartDateId,
      originatingDirectionId,
      mediaTypeId,
      userId,
      queueId,
      finalWrapupCode,
      resolved,
      scenarioName,
      location,
      'AHT' kpiName,
      tHandle num,
      nHandle * 1000 denom
    from
      CTE ce
    union
    select
      conversationStartDateId,
      originatingDirectionId,
      mediaTypeId,
      userId,
      queueId,
      finalWrapupCode,
      resolved,
      scenarioName,
      location,
      'Hold' kpiName,
      tHeldComplete num,
      nHandle * 1000 denom
    from
      CTE ce
    union
    select
      conversationStartDateId,
      originatingDirectionId,
      mediaTypeId,
      userId,
      queueId,
      finalWrapupCode,
      resolved,
      scenarioName,
      location,
      'ACW' kpiName,
      tAcwComplete num,
      nHandle * 1000 denom
    from
      CTE
    union
    select
      conversationStartDateId,
      originatingDirectionId,
      mediaTypeId,
      userId,
      queueId,
      wrapupCode AS finalWrapupCode,
      resolved,
      scenarioName,
      location,
      'CSAT' AS kpiName,
      (csatAchieved * 100) num,
      csatMax denom
    from
      (
        select
          userKey userId,
          queueKey queueId,
          wrapUpCodeKey wrapupCode,
          dd.dateId conversationStartDateId,
          sum(csatAchieved) csatAchieved,
          sum(csatMax) csatMax,
          c.originatingDirectionId,
          c.initialSessionMediaTypeId mediaTypeId,
          mql.Region location,
          t.process_name scenarioName,
          t.resolved
        from
          sdx_{tenant}.dim_hellofresh_interactions i
          join dgdm_{tenant}.dim_conversations c on i.conversationId = c.conversationId and c.initialSessionMediaTypeId = 1
          JOIN user_timezones ut ON i.userKey = ut.userId
          join dgdm_{tenant}.dim_date dd on dd.dateVal = CAST(
            FROM_UTC_TIMESTAMP(conversationStart, timezone) AS DATE
          )
          left join dgdm_{tenant}.fact_transcript_insights t on i.conversationId = t.conversationId
          and c.conversationId = t.conversationId
          and c.conversationStartDateId = t.conversationStartDateId
          join dgdm_hellofresh.mv_wrap_up_codes mvc on i.wrapUpCodeKey = mvc.wrapupId
          join dgdm_hellofresh.mv_queue_language mql on i.queueKey = mql.queueId

        where
          csatAchieved is not null
          and CAST(
            FROM_UTC_TIMESTAMP(surveyCompletionDate, timezone) AS DATE
          ) >= date_sub(CAST('{extract_date}' AS DATE), 7)
        group by
          userKey,
          queueKey,
          wrapUpCodeKey,
          dateId,
          c.originatingDirectionId,
          c.initialSessionMediaTypeId,
          mql.Region,
          t.process_name,
          t.resolved
      )
    union
    SELECT
      conversationStartDateId,
      originatingDirectionId,
      mediaTypeId,
      userId,
      queueId,
      wrapupCode AS finalWrapupCode,
      resolved,
      scenarioName,
      location,
      'NPS' AS kpiName,
      (promoter - detractor) * 100 AS num,
      totalSurveys AS denom
    from
      (
        select
          userKey userId,
          queueKey queueId,
          wrapUpCodeKey wrapupCode,
          CAST(
            date_format(
              FROM_UTC_TIMESTAMP(conversationStart, timezone),
              'yyyyMMdd'
            ) AS INT
          ) conversationStartDateId,
          SUM(
            CASE
              WHEN npsScore <= 8 THEN 1
              ELSE 0
            END
          ) AS detractor,
          SUM(
            CASE
              WHEN npsScore IN (9, 10) THEN 1
              ELSE 0
            END
          ) AS promoter,
          count(npsMaxResponse) totalSurveys,
          c.originatingDirectionId,
          c.initialSessionMediaTypeId mediaTypeId,
          mql.Region location,
          t.process_name scenarioName,
          t.resolved
        from
          sdx_{tenant}.dim_hellofresh_interactions i
          join dgdm_{tenant}.dim_conversations c on i.conversationId = c.conversationId and c.initialSessionMediaTypeId = 1
          JOIN user_timezones ut ON i.userKey = ut.userId
          left join dgdm_{tenant}.fact_transcript_insights t on i.conversationId = t.conversationId
          and c.conversationId = t.conversationId
          and c.conversationStartDateId = t.conversationStartDateId
          join dgdm_hellofresh.mv_wrap_up_codes mvc on i.wrapUpCodeKey = mvc.wrapupId
          join dgdm_hellofresh.mv_queue_language mql on i.queueKey = mql.queueId
        where
          CAST(
            FROM_UTC_TIMESTAMP(surveyCompletionDate, timezone) AS DATE
          ) >= date_sub(CAST('{extract_date}' AS DATE), 7)
        group by
          userKey,
          queueKey,
          wrapUpCodeKey,
          CAST(
            date_format(
              FROM_UTC_TIMESTAMP(conversationStart, timezone),
              'yyyyMMdd'
            ) AS INT
          ),
          c.originatingDirectionId,
          c.initialSessionMediaTypeId,
          mql.Region,
          t.process_name,
          t.resolved
      )
    union
    select
      conversationStartDateId,
      originatingDirectionId,
      mediaTypeId,
      userId,
      queueId,
      finalWrapupCode,
      resolved,
      scenarioName,
      location,
      'sales conversion' kpiName,
      case
        when ow.sales then nHandle * 100
        else 0
      end as num,
      case
        when ow.decisionMakerContact then nHandle
        else 0
      end as denom
    from
      CTE ce
      left join outbound_wrap_codes ow on ce.finalWrapupCode = ow.wrapupId
  ) A
  join dgdm_{tenant}.dim_date D on A.conversationStartDateId = D.dateId

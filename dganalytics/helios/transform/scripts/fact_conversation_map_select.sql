select
  conversationId,
  stepName,
  startLine,
  endLine,
  summary,
  sentiment,
  satisfaction,
  stageName,
  conversationStartDateId,
  case when startTime > endTime then endTime
        else coalesce(startTime,endTime) 
    end as startTime,
  case when endTime < startTime then startTime
        else coalesce(endTime,startTime) 
    end as endTime
  from
(
  select
    conversationId,
    stepName,
    startLine,
    endLine,
    summary,
    sentiment,
    satisfaction,
    stageName,
    conversationStartDateId,
    cast(
      MIN(
        CASE
          WHEN line = startLine THEN from_unixtime(startTimeMs / 1000)
        END
      ) as timestamp
    ) as startTime,
    cast(
      MAX(
        CASE
          WHEN line = endLine THEN from_unixtime((startTimeMs + COALESCE(milliseconds, 0)) / 1000)
        END
      ) as timestamp
    ) as endTime
  from
    (
      select
        a.*,
        b.line,
        b.startTimeMs,
        b.milliseconds,
        b.sentiment
      from
        (
          select distinct
            conversationId,
            conversation_map.step_name stepName,
            conversation_map.start_line startLine,
            conversation_map.end_line endLine,
            conversation_map.summary,
            conversation_map.satisfaction,
            conversation_map.stage_name stageName,
            conversationStartDateId
          from
            (
              select
                conversation_id conversationId,
                explode(conversation_map) conversation_map,
                conversationStartDateId
              from
                (
                  select
                    conversation_id,
                    conversation_map,
                    extractDate,
                    row_number() over (partition by conversation_id order by recordInsertTime DESC) rn
                  from
                    gpc_{tenant}.raw_conversation_map
                  WHERE extractDate between cast('{extract_start_time}' as date) and cast('{extract_end_time}' as date)
                  qualify
                    rn = 1
                ) as m
                  JOIN dgdm_{tenant}.dim_conversations c
                    on m.conversation_id = c.conversationId
            )
        ) a
          join (
            SELECT
              TP.conversationId,
              TP.text,
              TP.startTimeMs,
              TP.milliseconds,
              row_number() OVER (PARTITION BY TP.conversationId ORDER BY startTimeMs) line,
              d.dateId conversationStartDateId,
              TP.sentiment
            FROM
              gpc_{tenant}.fact_conversation_transcript_phrases TP
                JOIN dgdm_{tenant}.dim_date d
                  on cast(TP.conversationStartDate as date) = d.dateVal

          ) b
            on a.conversationId = b.conversationId
            and a.conversationStartDateId = b.conversationStartDateId
            and (
              startLine = b.line
              OR endLine = b.line
            )
      order by
        a.conversationId
    ) fta
  GROUP BY
    fta.conversationId,
    stepName,
    startLine,
    endLine,
    summary,
    sentiment,
    satisfaction,
    stageName,
    startLine,
    endLine,
    conversationStartDateId
)
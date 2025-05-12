select 
  conversationId,
  stageName,
  didHappen,
  successful,
  startLine,
  endLine,
  summary,
  conversationStartDateId,
  case when startTime > endTime then endTime
        else coalesce(startTime,endTime) 
    end as startTime,
  case when endTime < startTime then startTime
        else coalesce(endTime,startTime) 
    end as endTime,
  sentiment
  from
  (select
    conversationId,
    stageName,
    didHappen,
    successful,
    startLine,
    endLine,
    summary,
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
    ) as endTime,
    sentiment
  from
    (
      select
        a.*,
        b.line,
        b.startTimeMs,
        b.milliseconds,
        sentiment
      from
        (
          SELECT
            conversationId,
            value_stream.stage_name stageName,
            value_stream.did_happen didHappen,
            value_stream.successful,
            value_stream.start_line startLine,
            value_stream.end_line endLine,
            value_stream.summary,
            conversationStartDateId
          FROM
            (
              SELECT
                conversation_id conversationId,
                EXPLODE(value_stream) AS value_stream,
                conversationStartDateId
              from
                (
                  select
                    conversation_id,
                    value_stream,
                    extractDate,
                    row_number() over (
                      partition by conversation_id
                      order by
                        recordInsertTime DESC
                    ) rn
                  from
                    gpc_{tenant}.raw_value_stream 
                    WHERE extractDate between cast('{extract_start_time}' as date) and cast('{extract_end_time}' as date)
                    qualify rn = 1
                ) as m
                JOIN dgdm_{tenant}.dim_conversations c on m.conversation_id = c.conversationId
            )
        ) a
        left join (
          SELECT
            TP.conversationId,
            TP.text,
            TP.startTimeMs,
            TP.milliseconds,
            row_number() OVER (
              PARTITION BY TP.conversationId
              ORDER BY
                startTimeMs
            ) line,
            d.dateId conversationStartDateId,TP.sentiment
          FROM
            gpc_{tenant}.fact_conversation_transcript_phrases TP
            JOIN dgdm_{tenant}.dim_date d on cast(TP.conversationStartDate as date) = d.dateVal
        ) b on a.conversationId = b.conversationId
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
    startLine,
    endLine,
    stageName,
    didHappen,
    successful,
    startLine,
    endLine,
    summary,
    conversationStartDateId,
    sentiment)
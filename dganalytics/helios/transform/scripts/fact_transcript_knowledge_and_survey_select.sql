SELECT
  q.conversationId,
  agent_identifier,
  q.scoring.nps_score,
  q.scoring.nps_reason,
  q.scoring.knowledge_score,
  q.scoring.knowledge_reason,
  q.scoring.empathy_score,
  q.scoring.empathy_reason,
  q.scoring.clarity_score,
  q.scoring.clarity_reason,
  q.scoring.reasoning_score,
  q.scoring.reasoning_reason,
  q.scoring.csat_verbatim,
  c.conversationStartDateId
FROM
  (
    select
      conversationId,
      insight.agent_identifier,
      insight.scoring
    from
      (
        SELECT
          conversationId,
          explode(insights) insight
        FROM
          (
            SELECT
              conversation_id conversationId,
              insights,
              row_number() OVER(
                PARTITION BY conversation_id
                ORDER BY
                  recordInsertTime DESC
              ) RN
            FROM
              gpc_{tenant}.raw_conversation_insights 
            WHERE extractDate between cast('{extract_start_time}' as date) and cast('{extract_end_time}' as date)
          )
        WHERE
          RN = 1
      )
  ) q
  JOIN dgdm_{tenant}.dim_conversations c 
  on q.conversationId = c.conversationId
select
  o.conversation_id conversationId,
  objection,
  objection_start_line objectionStartLine,
  objection_end_line objectionEndLine,
  objection_summary objectionSummary,
  action,
  action_start_line actionStartLine,
  action_end_line actionEndLine,
  action_summary actionSummary,
  stage_name stageName,
  d.conversationStartDateId
from
  (
    select
      conversation_id ,
      objection.objection,
      objection.objection_start_line,
      objection.objection_end_line,
      objection.objection_summary,
      objection.action,
      objection.action_start_line,
      objection.action_end_line,
      objection.action_summary,
      objection.stage_name
    from
    (select conversation_id, explode(objection) objection from
    (select
      *,
      row_number() over (
        partition by conversation_id
        order by
          recordInsertTime DESC
      ) rn
    from
      gpc_{tenant}.raw_objection
    WHERE
      extractDate between cast('{extract_start_time}' as date) and cast('{extract_end_time}' as date)
      qualify rn = 1))
  ) o
    join dgdm_{tenant}.dim_conversations d
      on o.conversation_id = d.conversationId
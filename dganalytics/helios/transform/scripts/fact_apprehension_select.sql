SELECT 
  conversationId,
  apprehension.apprehension,
  apprehension.category,
  apprehension.start_line startLine,
  apprehension.end_line endLine,
  apprehension.agent_response agentResponse,
  apprehension.stage_name stageName,
  conversationStartDateId
FROM (
  SELECT 
      conversation_id conversationId, 
      EXPLODE(apprehension) AS apprehension, 
      conversationStartDateId
    from
      (
        select
          conversation_id,
          apprehension, 
          extractDate,
          row_number() over (partition by conversation_id order by recordInsertTime DESC) rn
        from
          gpc_{tenant}.raw_apprehension
        WHERE extractDate between cast('{extract_start_time}' as date) and cast('{extract_end_time}' as date)
        qualify rn = 1
      ) as m
        JOIN dgdm_{tenant}.dim_conversations c 
                on m.conversation_id = c.conversationId
  )
update
  dgdm_{tenant}.dim_conversations C
set
  hasManuallyEvaluated = TRUE
where
  exists (
    select
      1
    from
      (
        select
          conversationId,
          d.dateId conversationStartDateId
        from
          gpc_{tenant}.dim_evaluations e
        join dgdm_{tenant}.dim_date d 
            on e.conversationDatePart = d.dateVal
        where
          e.status = 'FINISHED' 
          and e.conversationDatePart >= date_sub(CAST('{extract_date}' AS DATE), 15)
      ) E
    where
      c.conversationId = E.conversationId
      and C.conversationStartDateId = E.conversationStartDateId
      and C.isManualAssessmentNeeded = TRUE
  )
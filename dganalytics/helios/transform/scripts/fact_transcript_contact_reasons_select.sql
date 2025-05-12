SELECT
  T.conversationId,
  agent_identifier,
  contactReason,
  mainInquiry,
  rootCause,
  inquiry_type,
  additional_inquiry,
  main_inquiry_raw,
  root_cause_raw,
  contact_reason_raw,
  C.conversationStartDateId,
  outcome,
  outcome_raw,
  objection,
  objection_raw,
  null product_feedback,null retention_offer
FROM
  (
    select
      conversationId,
      agent_identifier,
      contactReason,
      contact_reason_raw,
      inquiry.main_inquiry AS mainInquiry,
      inquiry.root_cause AS rootCause,
      inquiry.inquiry_type,
      inquiry.additional_inquiry,
      inquiry.main_inquiry_raw,
      inquiry.root_cause_raw,
      outcome,
      outcome_raw,
      objection,
      objection_raw
    from
      (
        SELECT
          conversationId,
          agent_identifier,
          contact.contact_reason AS contactReason,
          contact.contact_reason_raw AS contact_reason_raw,
          EXPLODE(contact.inquiries) AS inquiry,
          contact.outcome,
          contact.outcome_raw,
          contact.objection,
          contact.objection_raw
        from
          (
            select
              conversationId,
              insight.agent_identifier as agent_identifier,
              explode(insight.contact) contact
            from
              (
                SELECT
                  conversationId,
                  explode(insights) insight
                from
                  (
                    SELECT
                      conversation_id AS conversationId,
                      insights,
                      row_number() OVER (
                        PARTITION BY conversation_id
                        ORDER BY
                          recordInsertTime DESC
                      ) AS RN
                    FROM
                      gpc_{tenant}.raw_conversation_insights 
                    WHERE extractDate between cast('{extract_start_time}' as date) and cast('{extract_end_time}' as date)
                  )
                where
                  rn = 1
              )
          )
      )
  ) T
  JOIN dgdm_{tenant}.dim_conversations C 
  ON T.conversationId = C.conversationId;
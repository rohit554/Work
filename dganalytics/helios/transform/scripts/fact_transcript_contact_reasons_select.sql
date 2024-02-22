SELECT T.conversationId,
 T.contactReason,
 contact_reason_raw,
 T.mainInquiry,
 T.rootCause,
 T.inquiry_type,
 additional_inquiry,
 main_inquiry_raw,
 root_cause_raw,
 C.conversationStartDateId
 FROM (
  SELECT conversationId,
            contactReason,
            contact_reason_raw,
            inquiry.main_inquiry mainInquiry,
            inquiry.root_cause rootCause,
            inquiry.inquiry_type,
            inquiry.additional_inquiry,
            inquiry.main_inquiry_raw,
            inquiry.root_cause_raw
    FROM (   SELECT conversationId,
                contact.contact_reason contactReason,
                contact.contact_reason_raw contact_reason_raw,
                EXPLODE(contact.inquiries) inquiry
        FROM ( SELECT conversation_id conversationId,
                        EXPLODE(contact) contact
                FROM gpc_{tenant}.raw_transcript_insights
                WHERE extractDate = '{extract_date}'
                  AND extractIntervalStartTime = '{extract_start_time}'
                  AND extractIntervalEndTime = '{extract_end_time}'
            )
        )
    ) T
JOIN dgdm_{tenant}.dim_conversations C
  ON T.conversationId = C.conversationId
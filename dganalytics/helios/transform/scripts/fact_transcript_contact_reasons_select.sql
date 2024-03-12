SELECT T.conversationId,
 T.contactReason,
 T.mainInquiry,
 T.rootCause,
 T.inquiry_type,
 C.conversationStartDateId,
 main_inquiry_raw,
 root_cause_raw,
 contact_reason_raw,
 additional_inquiry
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
                        EXPLODE(contact) contact,
                        row_number() OVER (PARTITION BY conversation_id ORDER BY recordInsertTime DESC) RN

                FROM gpc_{tenant}.raw_transcript_insights
                WHERE extractDate = '{extract_date}'
                  AND extractIntervalStartTime = '{extract_start_time}'
                  AND extractIntervalEndTime = '{extract_end_time}'

            ) where RN=1
        )
    ) T
JOIN dgdm_{tenant}.dim_conversations C
  ON T.conversationId = C.conversationId
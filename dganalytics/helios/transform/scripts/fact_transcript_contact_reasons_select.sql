SELECT T.conversationId,
T.contactReason,
T.mainInquiry,
T.rootCause,
T.inquiry_type,
C.conversationStartDateId
FROM (SELECT conversationId,
            contactReason,
            inquiry.main_inquiry mainInquiry,
            inquiry.root_cause rootCause,
            inquiry.inquiry_type
    FROM (   SELECT conversationId,
                contact.contact_reason contactReason,
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
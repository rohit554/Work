SELECT
conversationId,
category,
action,
action_label,
contact_reason,
main_inquiry,
root_cause,
MIN(
    CASE
        WHEN line = startLine THEN from_unixtime(startTimeMs / 1000)
    END
    ) as startTime,
MAX(
    CASE
        WHEN line = endLine THEN from_unixtime((startTimeMs + COALESCE(milliseconds, 0)) / 1000)
    END
    ) as endTime,
    speaker,
    start_line,
    end_line
FROM
(
    SELECT
        *
    FROM
        (
            SELECT
                P.conversationId,
                category,
                action,
                action_label,
                contact_reason,
                inquiries.main_inquiry,
                inquiries.root_cause,
                T.startTimeMs,
                T.milliseconds,
                T.line,
                element_at(lines, 1) startLine,
                element_at(lines, size(lines)) endLine,
                speaker,
                start_line,
                end_line
            FROM
            (
                SELECT
                    conversationId,
                    step.category,
                    step.action,
                    step.action_label,
                    split(step.line, ',') lines,
                    contact.contact_reason,
                    explode(contact.inquiries) inquiries,
                    step.speaker,
                    step.start_line,
                    step.end_line 
                FROM
                    (
                    SELECT
                        conversationId,
                        EXPLODE(process_map) step,
                        contact
                    FROM
                        (
                        SELECT
                            conversation_id conversationId,
                            process_map,
                            explode(contact) contact,
                            row_number() OVER(
                            PARTITION BY conversation_id
                            ORDER BY
                                recordInsertTime DESC
                            ) RN
                        FROM
                        gpc_{tenant}.raw_transcript_insights
                        WHERE extractDate = '{extract_date}'
                        )
                    WHERE
                        RN = 1
                    )
                ) P
                JOIN (
                    SELECT
                        TP.conversationId,
                        TP.text,
                        TP.startTimeMs,
                        TP.milliseconds,
                        row_number() OVER (
                        PARTITION BY TP.conversationId
                        ORDER BY
                            startTimeMs
                        ) line
                    FROM
                        gpc_{tenant}.fact_conversation_transcript_phrases TP
                        INNER JOIN gpc_{tenant}.raw_speechandtextanalytics_transcript C ON TP.conversationId = c.conversationId
                        where
                        c.extractDate = '{extract_date}'
                ) T ON T.conversationid = P.conversationId
            and (
            element_at(lines, 1) = T.line
            OR element_at(lines, size(lines)) = T.line
            )
        order by
            P.conversationId
    )
)
GROUP BY
conversationId,
category,
action,
action_label,
contact_reason,
main_inquiry,
root_cause,
speaker,
start_line,
end_line
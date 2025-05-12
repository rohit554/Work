with CTE as (
SELECT
conversationId,
category,
action,
'' action_label,
contact_reason,
main_inquiry,
root_cause,
MIN(
    CASE
        WHEN line = start_line THEN from_unixtime(startTimeMs / 1000)
    END
    ) as startTime,
MAX(
    CASE
        WHEN line = end_line THEN from_unixtime((startTimeMs + COALESCE(milliseconds, 0)) / 1000)
    END
    ) as endTime,
    speaker,
    start_line,
    end_line,
    conversationStartDateId,
    confidence,
    contribution,
    impact,
    impact_reason,
    emotion,
    difficulty,
    -- Use FIRST() to get the sentiment in GROUP BY
    FIRST(
        (
            SELECT AVG(fcts.sentiment)
            FROM gpc_{tenant}.fact_conversation_transcript_sentiments AS fcts
            WHERE fcts.conversationId = fta.conversationId
            AND fcts.phraseIndex BETWEEN fta.start_line AND fta.end_line
        )
    ) AS sentiment
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
                contact_reason,
                inquiries.main_inquiry,
                inquiries.root_cause,
                T.startTimeMs,
                T.milliseconds,
                speaker,
                start_line,
                end_line,
                T.conversationStartDateId,
                confidence,
                contribution,
                difficulty,
                impact,
                impact_reason,
                emotion,
                T.line
            FROM
            (
                SELECT
                    conversationId,
                    step.category,
                    step.action,
                    contact.contact_reason,
                    explode(contact.inquiries) inquiries,
                    step.speaker,
                    step.start_line,
                    step.end_line,
                    step.confidence,
                    step.contribution,
                    step.difficulty,
                    step.impact,
                    step.impact_reason,
                    step.emotion
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
                        ) line,
                        conversationStartDateId
                    FROM
                        gpc_{tenant}.fact_conversation_transcript_phrases TP
                        INNER JOIN dgdm_{tenant}.dim_conversations C ON TP.conversationId = c.conversationId
                ) T ON T.conversationid = P.conversationId
            and (
            start_line = T.line
            OR end_line = T.line
            )
        order by
            P.conversationId
    )
) fta
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
end_line,
conversationStartDateId,
confidence,
contribution,
difficulty,
impact,
impact_reason,
emotion
)
SELECT * FROM CTE b
    WHERE NOT EXISTS (
        SELECT 1 FROM dgdm_{tenant}.fact_transcript_actions a
        WHERE a.conversationId = b.conversationId
    )
from pyspark.sql import SparkSession

def dim_conversations(spark: SparkSession, extract_date: str):
    conversations = spark.sql(f"""
                                    select
                        distinct
                        conversationId,
                        conversationStart,
                        conversationEnd,
                        originatingDirection,
                        sessionId,
                        element_at(segments, 1).segmentStart as sessionStart,
                        element_at(segments, size(segments)).segmentEnd as sessionEnd,
                        sessionDirection,
                        element_at(segments, 1).queueId as queueId,
                        mediaType,
                        messageType,
                        agentId,
                        element_at(segments, size(segments)).wrapUpCode as wrapUpCode,
                        element_at(segments, size(segments)).wrapUpNote as wrapUpNote,
                        cast(conversationStart as date) conversationStartDate
                    from
                        (
                        select
                            conversationId, conversationStart, conversationEnd, originatingDirection,
                            sessions.mediaType, sessions.messageType, purpose, agentId, sessions.sessionId,
                            sessions.direction as sessionDirection, sessions.segments
                        from
                            (
                            select
                                conversationId, conversationStart, conversationEnd, originatingDirection,
                                participants.purpose, participants.userId as agentId,
                                explode(participants.sessions) as sessions
                            from
                                (
                                select
                                    conversationId, conversationStart, conversationEnd, originatingDirection,
                                    explode(participants) as participants
                                from
                                    raw_conversation_details where extractDate = '{extract_date}')
                                ) )
                                    """
                              )
    conversations.registerTempTable("conversations")

    upsert = spark.sql("""
                            merge into dim_conversations as target
                                using conversations as source
                                on source.conversationStartDate = target.conversationStartDate
                                    and source.conversationId = target.conversationId
                                    and source.sessionId = target.sessionId
                                WHEN MATCHED THEN
                                    UPDATE SET *
                                WHEN NOT MATCHED THEN
                                    INSERT *
                            """)

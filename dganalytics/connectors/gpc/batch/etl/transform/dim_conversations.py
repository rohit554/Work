from pyspark.sql import SparkSession

def dim_conversations(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    conversations = spark.sql(f"""
                                    select
                        distinct
                        conversationId,
                        conversationStart,
                        conversationEnd,
                        originatingDirection,
                        element_at(segments, 1).segmentStart as sessionStart,
                        element_at(segments, size(segments)).segmentEnd as sessionEnd,
                        element_at(segments, 1).queueId as queueId,
                        mediaType,
                        messageType,
                        agentId,
                        element_at(segments, size(segments)).wrapUpCode as wrapUpCode,
                        element_at(segments, size(segments)).wrapUpNote as wrapUpNote,
                        cast(conversationStart as date) conversationStartDate,
                        sourceRecordIdentifier, soucePartition
                    from
                        (
                        select
                            conversationId, conversationStart, conversationEnd, originatingDirection,
                            sessions.mediaType, sessions.messageType, purpose, agentId, sessions.sessionId,
                            sessions.direction as sessionDirection, sessions.segments, sourceRecordIdentifier, soucePartition
                        from
                            (
                            select
                                conversationId, conversationStart, conversationEnd, originatingDirection,
                                participants.purpose, participants.userId as agentId,
                                explode(participants.sessions) as sessions, sourceRecordIdentifier, soucePartition
                            from
                                (
                                select
                                    conversationId, conversationStart, conversationEnd, originatingDirection,
                                    explode(participants) as participants
                                    ,recordIdentifier as sourceRecordIdentifier,
                                    concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
                                from
                                    raw_conversation_details where extractDate = '{extract_date}'
                                        and  extractIntervalStartTime = '{extract_start_time}' and extractIntervalEndTime = '{extract_end_time}')
                                    where participants.purpose = 'agent'
                                ) )
                                    """
                              )
    conversations.registerTempTable("conversations")

    spark.sql("""delete from dim_conversations a where exists (
                        select 1 from conversations b 
                                where a.conversationId = b.conversationId
                                and a.conversationStartDate = b.conversationStartDate
    )""")

    spark.sql("""insert into dim_conversations select * from conversations""")

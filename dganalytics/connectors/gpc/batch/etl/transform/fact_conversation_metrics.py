from pyspark.sql import SparkSession


def fact_conversation_metrics(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    conversation_metrics = spark.sql(f"""
                                            select
                                        conversationId, agentId, mediaType, messageType, originatingDirection,
                            element_at(segments, 1).queueId as queueId,
                            element_at(segments, size(segments)).wrapUpCode as wrapUpCode,
                        max(element_at(segments, size(segments)).wrapUpNote) as wrapUpNote,
                    cast(concat(date_format(emitDate, 'yyyy-MM-dd HH:'),
                        format_string("%02d", floor(minute(emitDate)/15) * 15), ':00') as timestamp) as emitDateTime,
                    sum(case when coalesce(tAbandon,0) > 0 then 1 else 0 end) as nAbandon,
                    sum(case when coalesce(tAcd,0) > 0 then 1 else 0 end) as nAcd,
                    sum(case when coalesce(tAcw,0) > 0 then 1 else 0 end) as nAcw,
                    sum(case when coalesce(tAnswered,0) > 0 then 1 else 0 end) as nAnswered,
                    sum(coalesce(nBlindTransferred,0)) as nBlindTransferred,
                    sum(coalesce(nConnected,0)) as nConnected,
                    sum(coalesce(nConsult,0)) as nConsult,
                    sum(coalesce(nConsultTransferred,0)) as nConsultTransferred,
                    sum(coalesce(nError,0)) as nError,
                    sum(case when coalesce(tHandle,0) > 0 then 1 else 0 end) as nHandle,
                    sum(case when coalesce(tHeldComplete,0) > 0 then 1 else 0 end) as nHeldComplete,
                    sum(coalesce(nOffered,0)) as nOffered,
                    sum(coalesce(nOutbound,0)) as nOutbound,
                    sum(coalesce(nOutboundAbandoned,0)) as nOutboundAbandoned,
                    sum(coalesce(nOutboundAttempted,0)) as nOutboundAttempted,
                    sum(coalesce(nOutboundConnected,0)) as nOutboundConnected,
                    sum(coalesce(nOverSla,0)) as nOverSla,
                    sum(case when coalesce(tShortAbandon,0) > 0 then 1 else 0 end) as nShortAbandon,
                    sum(case when coalesce(tTalkComplete,0) > 0 then 1 else 0 end) as nTalkComplete,
                    sum(coalesce(nTransferred,0)) as nTransferred,
                    sum(coalesce(tAbandon,0))/1000.0 as tAbandon,
                    sum(coalesce(tAcd,0))/1000.0 as tAcd,
                    sum(coalesce(tAcw,0))/1000.0 as tAcw,
                    sum(coalesce(tAgentResponseTime,0))/1000.0 as tAgentResponse,
                    sum(coalesce(tAnswered,0))/1000.0 as tAnswered,
                    sum(coalesce(tContacting,0))/1000.0 as tContacting,
                    sum(coalesce(tDialing,0))/1000.0 as tDialing,
                    sum(coalesce(tHandle,0))/1000.0 as tHandle,
                    sum(coalesce(tHeldComplete,0))/1000.0 as tHeldComplete,
                    sum(coalesce(tIvr,0))/1000.0 as tIvr,
                    sum(coalesce(tNotResponding,0))/1000.0 as tNotResponding,
                    sum(coalesce(tShortAbandon,0))/1000.0 as tShortAbandon,
                    sum(coalesce(tTalkComplete,0))/1000.0 as tTalkComplete,
                    sum(coalesce(tVoicemail,0))/1000.0 as tVoicemail,
                    sum(coalesce(tWait,0))/1000.0 as tWait,
                    cast(cast(concat(date_format(emitDate, 'yyyy-MM-dd HH:'),
                    format_string("%02d", floor(minute(emitDate)/15) * 15), ':00') as timestamp) as date) as emitDate,
                    max(sourceRecordIdentifier), max(soucePartition)
                    from
                        (
                        select
                    *
                    from (
                    select
                        conversationId, agentId, originatingDirection, purpose, 
                        element_at(segments, 1).queueId, mediaType, messageType, 
                        metrics.emitDate, metrics.name, metrics.value, segments,
                        sourceRecordIdentifier, soucePartition
                    from
                        (
                        select
                            conversationId, conversationStart, conversationEnd, originatingDirection,
                            sessions.mediaType, sessions.messageType, purpose, agentId, sessions.sessionId,
                            sessions.direction as sessionDirection, sessions.segments,
                            explode(sessions.metrics) as metrics,
                            sourceRecordIdentifier, soucePartition
                        from
                            (
                            select
                                conversationId, conversationStart, conversationEnd, originatingDirection,
                                participants.purpose, participants.userId as agentId,
                                explode(participants.sessions) as sessions,
                                sourceRecordIdentifier, soucePartition
                            from
                                (
                                select
                                    conversationId, conversationStart, conversationEnd, originatingDirection,
                                    explode(participants) as participants,
                                    recordIdentifier as sourceRecordIdentifier,
                                    concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
                                from
                                    raw_conversation_details where extractDate = '{extract_date}'
                                    and  extractIntervalStartTime = '{extract_start_time}' and extractIntervalEndTime = '{extract_end_time}')
                           ) )
                    )
                    pivot (
                            sum(coalesce(value,0))
                        for name in ('nBlindTransferred',
                                    'nConnected', 'nConsult', 'nConsultTransferred', 'nError',
                                    'nOffered', 'nOutbound', 'nOutboundAbandoned',
                                    'nOutboundAttempted', 'nOutboundConnected', 'nOverSla',
                                    'nStateTransitionError', 'nTransferred',
                                    'oExternalMediaCount', 'oInteracting', 'oMediaCount', 'oServiceLevel',
                                    'oServiceTarget', 'oWaiting',
                                    'tAbandon', 'tAcd', 'tAcw', 'tAgentResponseTime', 'tAlert', 'tAnswered',
                                    'tContacting', 'tDialing',
                                    'tFlowOut', 'tHandle', 'tHeld', 'tHeldComplete', 'tIvr',
                                    'tMonitoring', 'tNotResponding',
                                    'tShortAbandon', 'tTalk', 'tTalkComplete', 'tUserResponseTime','tVoicemail',
                                    'tWait'
                                )
                            )
                    )
                    group by conversationId, agentId, mediaType, messageType, originatingDirection, 
                    element_at(segments, 1).queueId,
                            element_at(segments, size(segments)).wrapUpCode,
                    cast(concat(date_format(emitDate, 'yyyy-MM-dd HH:'),
                        format_string("%02d", floor(minute(emitDate)/15) * 15), ':00') as timestamp)
                                            """)

    conversation_metrics.createOrReplaceTempView("conversation_metrics")
    spark.sql("""delete from fact_conversation_metrics where conversationId in (
                            select distinct conversationId from conversation_metrics)""")
    spark.sql(
        "insert into fact_conversation_metrics select * from conversation_metrics")

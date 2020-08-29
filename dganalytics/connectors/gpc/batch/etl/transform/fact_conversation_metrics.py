from pyspark.sql import SparkSession


def fact_conversation_metrics(spark: SparkSession, extract_date: str):
    conversation_metrics = spark.sql(f"""
                                            select 
                    sessionId,  
                    cast(concat(date_format(emitDate, 'yyyy-MM-dd HH:'), format_string("%02d", floor(minute(emitDate)/15) * 15), ':00') as timestamp) as emitDateTime,
                    cast(cast(concat(date_format(emitDate, 'yyyy-MM-dd HH:'), format_string("%02d", floor(minute(emitDate)/15) * 15), ':00') as timestamp) as date) as emitDate,
                    sum(coalesce(tAbandonCount,0)) as nAbandon,
                    sum(coalesce(tAcdCount,0)) as nAcd,
                    sum(coalesce(tAcwCount,0)) as nAcw,
                    sum(coalesce(tAnsweredCount,0)) as nAnswered,
                    sum(coalesce(nBlindTransferred,0)) as nBlindTransferred,
                    sum(coalesce(nConnected,0)) as nConnected,
                    sum(coalesce(nConsult,0)) as nConsult,
                    sum(coalesce(nConsultTransferred,0)) as nConsultTransferred,
                    sum(coalesce(nError,0)) as nError,
                    sum(coalesce(tHandleCount,0)) as nHandle,
                    sum(coalesce(tHeldCompleteCount,0)) as nHeldComplete,
                    sum(coalesce(nOffered,0)) as nOffered,
                    sum(coalesce(nOutbound,0)) as nOutbound,
                    sum(coalesce(nOutboundAbandoned,0)) as nOutboundAbandoned,
                    sum(coalesce(nOutboundAttempted,0)) as nOutboundAttempted,
                    sum(coalesce(nOutboundConnected,0)) as nOutboundConnected,
                    sum(coalesce(nOverSla,0)) as nOverSla,
                    sum(coalesce(tShortAbandonCount,0)) as nShortAbandon,
                    sum(coalesce(tTalkComplete,0)) as nTalkComplete,
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
                    sum(coalesce(tWait,0))/1000.0 as tWait
                    from 
                    (
                    select 
                    *
                    from (
                    select
                        sessionId, metrics.emitDate, metrics.name, metrics.value
                    from
                        (
                        select
                            sessions.sessionId, explode(sessions.metrics) as metrics 
                        from
                            (
                            select
                                explode(participants.sessions) as sessions
                            from
                                (
                                select
                                    explode(participants) as participants
                                from
                                    raw_conversation_details where extractDate = '{extract_date}')
                            where
                                participants.purpose = 'agent') )
                    )
                    pivot (
                            sum(coalesce(value,0))
                        for name in ('nBlindTransferred', 'nConnected', 'nConsult', 'nConsultTransferred', 'nError', 'nOffered', 'nOutbound',
                                    'nOutboundAbandoned', 'nOutboundAttempted', 'nOutboundConnected', 'nOverSla', 'nTransferred', 'tAbandon',
                                    'tAbandonCount', 'tAcd', 'tAcdCount', 'tAcw', 'tAcwCount', 'tAgentResponseTime', 'tAnswered', 'tAnsweredCount',
                                    'tContacting', 'tDialing', 'tHandle', 'tHandleCount', 'tHeld', 'tHeldComplete', 'tHeldCompleteCount',
                                    'tHeldCount', 'tIvr', 'tNotResponding', 'tShortAbandon', 'tShortAbandonCount', 'tTalk', 'tTalkComplete',
                                    'tTalkCompleteCount', 'tVoicemail', 'tWait'
                                )
                            )
                        
                    )
                    group by sessionId, cast(concat(date_format(emitDate, 'yyyy-MM-dd HH:'), format_string("%02d", floor(minute(emitDate)/15) * 15), ':00') as timestamp)
                                            """)

    conversation_metrics.registerTempTable("conversation_metrics")
    spark.sql(f"""
                    merge into fact_conversation_metrics as target
                        using conversation_metrics as source
                    on source.sessionId = target.sessionId
                            and source.emitDateTime = target.emitDateTime
                            and source.emitDate = target.emitDate
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                """)

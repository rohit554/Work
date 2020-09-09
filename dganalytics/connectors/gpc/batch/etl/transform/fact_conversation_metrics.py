from pyspark.sql import SparkSession


def fact_conversation_metrics(spark: SparkSession, extract_date: str):
    conversation_metrics = spark.sql(f"""
                                            select distinct 
                    sessionId,  
                    cast(concat(date_format(emitDate, 'yyyy-MM-dd HH:'), format_string("%02d", floor(minute(emitDate)/15) * 15), ':00') as timestamp) as emitDateTime,
                    cast(cast(concat(date_format(emitDate, 'yyyy-MM-dd HH:'), format_string("%02d", floor(minute(emitDate)/15) * 15), ':00') as timestamp) as date) as emitDate,
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
                           ) )
                    )
                    pivot (
                            sum(coalesce(value,0))
                        for name in ('nBlindTransferred',
                                    'nConnected', 'nConsult', 'nConsultTransferred', 'nError', 'nOffered', 'nOutbound', 'nOutboundAbandoned',
                                    'nOutboundAttempted', 'nOutboundConnected', 'nOverSla', 'nStateTransitionError', 'nTransferred',
                                    'oExternalMediaCount', 'oInteracting', 'oMediaCount', 'oServiceLevel', 'oServiceTarget', 'oWaiting',
                                    'tAbandon', 'tAcd', 'tAcw', 'tAgentResponseTime', 'tAlert', 'tAnswered', 'tContacting', 'tDialing',
                                    'tFlowOut', 'tHandle', 'tHeld', 'tHeldComplete', 'tIvr', 'tMonitoring', 'tNotResponding',
                                    'tShortAbandon', 'tTalk', 'tTalkComplete', 'tUserResponseTime','tVoicemail',
                                    'tWait'
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

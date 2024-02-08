from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
from io import BytesIO
from dganalytics.utils.utils import get_secret, get_env
from dganalytics.helios.helios_utils import helios_utils_logger
import os

def helios_export(spark, tenant, extract_name, output_file_name):
    logger = helios_utils_logger(tenant,"helios")
    account_name = get_secret("storageaccountnameforprocessmap")
    account_key = get_secret("storageaccountkeyforprocessmap")
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(tenant)

        # Get a reference to the existing blob
        blob_client = container_client.get_blob_client(os.path.join(get_env(),output_file_name))

        df = spark.sql(f"""
                with conversations as (
                select * from dgdm_simplyenergy.dim_conversations
                WHERE conversationStart >= add_months(current_date(), -12)
                ),
                CTE AS (
                SELECT
                    conversationId, eventName,
                    (case WHEN eventType = 'authStatus' THEN COALESCE(eventStart, TIMESTAMPADD(MICROSECOND, 1000, LAG(eventEnd, 1) OVER (PARTITION BY conversationId ORDER BY level1))) 
                                                ELSE eventStart END)
                                                AS eventStart,
                    (case WHEN eventType = 'authStatus' THEN COALESCE(eventEnd, TIMESTAMPADD(MICROSECOND, 1000, LAG(eventEnd, 1) OVER (PARTITION BY conversationId ORDER BY level1))) 
                                    ELSE eventEnd END)
                                    AS eventEnd,
                    level1,
                    eventType,
                    conversationStartDateId
                FROM (
                    select conversationId,
                        conversationStart as eventStart,
                        conversationStart as eventEnd,
                        case when originatingDirectionId=1 then 'ANI' else 'DNIS'END  as eventName,
                        0 as level1,
                        'caller' eventType,
                        conversationStartDateId
                        FROM conversations
                        
                    UNION all

                    select conversationId,
                        TIMESTAMPADD(MICROSECOND, 1000, conversationStart) as eventStart,--add 1ms
                        TIMESTAMPADD(MICROSECOND, 1000, conversationStart) as eventEnd,--add ms
                        case when originatingDirectionId=1 then 'DNIS' else 'ANI'END  as eventName,
                        1 as level1,
                        'caller' eventType,
                        conversationStartDateId
                        FROM conversations
                        
                        UNION ALL

                        select 
                        as.conversationId,
                        null as eventStart,
                        null as eventEnd,
                        eventName || ": "|| eventValue eventName,
                        2 as level1,
                        'authStatus' eventType,
                        as.conversationStartDateId
                        from dgdm_simplyenergy.dim_conversation_ivr_events  as 
                        join conversations c
                        on
                        as.conversationStartDateId = c.conversationStartDateId 
                        and as.conversationId = c.conversationId
                        where eventName='AuthenticationStatus'

                    UNION ALL

                    select 
                        f.conversationId,
                        min(s.segmentStart) as eventStart,--add 1ms
                        max(s.segmentEnd) as eventEnd,
                        'Flow: ' ||flowName as eventName,
                        3 as level1,
                        'flow' eventType,
                        f.conversationStartDateId
                    from dgdm_simplyenergy.dim_conversation_session_flow f
                    join
                    dgdm_simplyenergy.dim_conversation_session_segments s 
                    on
                        f.conversationStartDateId = s.conversationStartDateId
                        and f.conversationId = s.conversationId
                        and f.sessionId = s.sessionId
                    join conversations c
                        on
                        f.conversationStartDateId = c.conversationStartDateId and
                        f.conversationId = c.conversationId
                    group by f.conversationId, f.flowName, f.conversationStartDateId


                    UNION ALL

                    SELECT conversationId, TIMESTAMPADD(MICROSECOND, 1000 * (pos + 25), eventStart) eventStart, TIMESTAMPADD(MICROSECOND, 1000 * (pos + 25), eventStart) eventStart, eventName, (4 + pos) level1 , eventType, conversationStartDateId
                    FROM(
                    SELECT
                        conversationId,
                        conversationStart AS eventStart,
                        conversationStart AS eventEnd,  -- Add eventEnd to the selection
                        posexplode(split(eventValue, ',')) as (pos, eventName),
                        -- 4 as level,
                        'menu' eventType,
                        conversationStartDateId
                        FROM (SELECT  e.*, c.conversationStart 
                    FROM dgdm_simplyenergy.dim_conversation_ivr_events e
                    INNER JOIN conversations c
                        ON e.conversationStartDateId = c.conversationStartDateId
                        AND e.conversationId = c.conversationId
                    WHERE e.eventName = 'MenuID' 
                    )
                    )
                    WHERE TRIM(eventName) != '' AND eventName is not NULL

                    UNION ALL

                    select  
                        s.conversationId,
                        min(segmentStart) as eventStart,
                        max(segmentEnd) as eventEnd,
                        'Queue: ' ||q.queueName as eventName,
                        100 as level1,
                        'queue' eventType,
                        s.conversationStartDateId
                        from
                        dgdm_simplyenergy.dim_conversation_session_segments s
                        join
                        dgdm_simplyenergy.dim_queues q
                            on s.queueId = q.queueId
                        join dgdm_simplyenergy.dim_conversation_participants p
                            on s.conversationStartDateId = p.conversationStartDateId
                            and s.conversationId = p.conversationId
                            and s.participantId = p.participantId
                        join conversations c
                            on
                            s.conversationStartDateId = c.conversationStartDateId and
                            s.conversationId = c.conversationId
                        where s.queueId is not null and p.purpose not in ('customer','external')
                        group by s.conversationId, queueName, s.conversationStartDateId

                    UNION ALL

                    select p.conversationId,
                        min(segmentStart) as eventStart,
                        max(segmentEnd)as eventEnd,
                        'Agent ' || ROW_NUMBER() OVER (PARTITION BY p.conversationId ORDER BY MIN(segmentStart)) AS eventName,
                        200 as level1,
                        'agent' eventType,
                        p.conversationStartDateId
                    from dgdm_simplyenergy.dim_conversation_participants p
                    join dgdm_simplyenergy.dim_conversation_session_segments s 
                        on p.participantId=s.participantId 
                        and p.conversationId=s.conversationId
                    join conversations c
                        on
                        p.conversationStartDateId = c.conversationStartDateId and
                        p.conversationId = c.conversationId
                    where p.userId is not null 
                    group by p.conversationId,userId, p.conversationStartDateId

                    UNION ALL

                    select 
                        b.conversationId,
                        eventTime eventStart,
                        eventTime eventEnd,
                        'Blind Transferred' as eventName,
                        250 as level1,
                        'blindTransfer' eventType,
                        b.conversationStartDateId
                    from dgdm_simplyenergy.fact_conversation_metrics b
                    join conversations c
                        on
                        b.conversationStartDateId = c.conversationStartDateId and
                        b.conversationId = c.conversationId
                    where name='nBlindTransferred' 

                    UNION ALL

                    select 
                        ct.conversationId,
                        eventTime eventStart,
                        eventTime eventEnd,
                        'Consult Transferred' as eventName,
                        300 as level1, 
                        'consultTransfer' eventType,
                        ct.conversationStartDateId
                    from dgdm_simplyenergy.fact_conversation_metrics ct
                    join conversations c
                        on
                        ct.conversationStartDateId = c.conversationStartDateId and
                        ct.conversationId = c.conversationId
                    where name='nConsultTransferred'

                    UNION ALL

                    select distinct 
                        cm.conversationId,
                        eventTime eventStart,
                        eventTime eventEnd,
                        'Consult' as eventName,
                        350 as level1,
                        'consult' eventType,
                        cm.conversationStartDateId
                    from dgdm_simplyenergy.fact_conversation_metrics cm
                    join conversations c
                        on
                        cm.conversationStartDateId = c.conversationStartDateId and
                        cm.conversationId = c.conversationId
                    where name='nConsult' 

                    UNION ALL

                    select 
                        h.conversationId,
                        eventTime eventStart,
                        eventTime eventEnd,
                        'Hold' as eventName,
                        400 as level1,
                        'hold' eventType,
                        h.conversationStartDateId
                    from dgdm_simplyenergy.fact_conversation_metrics h
                    join conversations c
                        on
                        h.conversationStartDateId = c.conversationStartDateId and
                        h.conversationId = c.conversationId
                    where name='tHeld'  

                )
                )


                SELECT a.conversationId,
                    a.category,
                    a.action,
                    a.action_label,
                    a.eventStart,
                    a.eventEnd,
                    a.contact_reason,
                    a.main_inquiry,
                    a.root_cause,
                    a.location,
                    a.originatingDirectionId,
                    a.mediatypeId,
                    ie.AuthenticationStatus,
                    (CASE WHEN f.resolved IS NOT NULL THEN f.resolved ELSE i.resolved END) resolved,
                    MAX(CASE WHEN fc.name = 'tHeld' THEN True ELSE False END) AS hashold,
                        MAX(CASE WHEN fc.name = 'nConsult' THEN True ELSE False END) AS hasconsult,
                        MAX(CASE WHEN fc.name = 'nConsultTransferred' THEN True ELSE False END) AS hasconsulttransfer,
                        MAX(CASE WHEN fc.name = 'nBlindTransferred' THEN True ELSE False END) AS hasblindtransfer
                FROM
                (
                    SELECT c.conversationId,
                        (CASE WHEN eventType = 'menu' THEN 'Menu: ' || eventName ELSE eventName END) as category,
                        '' action,
                        '' action_label,
                        eventStart,
                        eventEnd,
                        i.contactReason contact_reason,
                        i.mainInquiry main_inquiry,
                        i.rootCause root_cause,
                        dc.location,
                        dc.originatingDirectionId,
                        dc.initialSessionMediaTypeId mediatypeId
                FROM CTE c
                JOIN conversations dc
                    ON dc.conversationStartDateId = c.conversationStartDateId 
                    and dc.conversationId = c.conversationId
                LEFT JOIN dgdm_simplyenergy.fact_transcript_contact_reasons i
                    ON c.conversationId = i.conversationId

                UNION ALL

                    SELECT  a.conversationId,
                            category,
                            action,
                            action_label,
                            startTime eventStart,
                            endTime eventEnd,
                            contact_reason,
                            main_inquiry,
                            root_cause,
                            dc.location,
                        dc.originatingDirectionId,
                        dc.initialSessionMediaTypeId mediatypeId
                    FROM dgdm_simplyenergy.fact_transcript_actions a
                    JOIN conversations dc
                    ON 
                    dc.conversationId = a.conversationId
                ) a
                LEFT JOIN  dgdm_simplyenergy.fact_conversation_metrics fc
                ON fc.conversationId = a.conversationid
                LEFT JOIN 
                (SELECT eventValue AuthenticationStatus, conversationId FROM dgdm_simplyenergy.dim_conversation_ivr_events 
                    where eventName = 'AuthenticationStatus' ) ie
                ON  a.conversationId = ie.conversationId
                LEFT JOIN dgdm_simplyenergy.fact_transcript_insights i
                ON i.conversationId = a.conversationId
                LEFT JOIN (SELECT 'resolved' resolved, conversationId from dgdm_simplyenergy.dim_conversation_session_flow
                where exitReason = 'FLOW_DISCONNECT') f
                on f.conversationId = a.conversationId
                GROUP BY a.conversationId,
                    a.category,
                    a.action,
                    a.action_label,
                    a.eventStart,
                    a.eventEnd,
                    a.contact_reason,
                    a.main_inquiry,
                    a.root_cause,
                    a.location,
                    a.originatingDirectionId,
                    a.mediatypeId,
                    ie.AuthenticationStatus,
                    i.resolved,
                    f.resolved
                ORDER BY a.conversationId, a.eventStart, a.eventEnd
        """)
        df = df.toPandas()
        
        csv_content = df.to_csv(index=False).encode('utf-8')
        
        # Overwrite the data in the existing blob
        blob_client.upload_blob(BytesIO(csv_content), blob_type="BlockBlob", content_settings=ContentSettings(content_type="text/csv"),  overwrite=True)
    
    except Exception as e:
        logger.exception(f"An error occurred  in exporting {extract_name}: {e}")
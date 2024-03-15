from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
from io import BytesIO
from dganalytics.utils.utils import get_secret, get_env
from dganalytics.helios.helios_utils import helios_utils_logger
import os

def ivr_export(spark, tenant, extract_name, output_file_name):
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
         
        df=spark.sql(f"""
                with conversations as (
                        select * from dgdm_{tenant}.dim_conversations
                        WHERE conversationStartDateId > 20231231 and originatingDirectionId = 1 and initialSessionMediaTypeId = 1
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
                                conversationStart as eventStart,--add 1ms
                                TIMESTAMPADD(MICROSECOND, 1000, conversationStart) as eventEnd,--add ms
                                'Start'  as eventName,
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
                                cast(
                                    concat(
                                        from_unixtime((cast(eventTime as double) - (cast(value as double)/1000))),
                                            '.',
                                            FLOOR((cast(eventTime as double)*1000 - (cast(value as double)))%1000)
                                        ) as timestamp
                                ) as eventStart,--add 1ms
                                eventTime as eventEnd,
                                'Flow: ' ||flowName as eventName,
                                3 as level1,
                                'flow' eventType,
                                f.conversationStartDateId
                            from dgdm_simplyenergy.dim_conversation_session_flow f
                            join
                            dgdm_simplyenergy.fact_conversation_metrics m 
                            on
                                f.conversationStartDateId = m.conversationStartDateId
                                and f.conversationId = m.conversationId
                                and f.sessionId = m.sessionId
                            join conversations c
                                on
                                f.conversationStartDateId = c.conversationStartDateId and
                                f.conversationId = c.conversationId
                                where m.name = 'tFlow'


                            UNION ALL

                            SELECT conversationId, TIMESTAMPADD(MICROSECOND, 1000 * (pos + 35), eventStart) eventStart, TIMESTAMPADD(MICROSECOND, 1000 * (pos + 35), eventStart) eventEnd, eventName, (4 + pos) level1 , eventType, conversationStartDateId
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
                                where s.queueId is not null and p.purpose not in ('customer')
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

                        )
                    )


                    SELECT * FROM
                    (SELECT a.conversationId,
                        a.category,
                        a.eventStart,
                        a.eventEnd,
                        a.location,
                        a.originatingDirectionId,
                        a.mediatypeId,
                        ie.AuthenticationStatus,
                        f.conversationId IS NOT NULL isIVRSuccess,
                        fd.conversationId IS NOT NULL isIVRAbandon,
                        fe.conversationId IS NOT NULL isIVRError,
                        fq.conversationId IS NOT NULL hasIVRToQueueTransfer,
                        U.userNames,
                        UT.teamNames
                    FROM
                    (
                        SELECT c.conversationId,
                            (CASE WHEN eventType = 'menu' THEN 'Menu: ' || replace(eventName, '_', ' ') ELSE eventName END) as category,
                            eventStart,
                            eventEnd,
                            dc.location,
                            dc.originatingDirectionId,
                            dc.initialSessionMediaTypeId mediatypeId
                        FROM CTE c
                        JOIN conversations dc
                            ON dc.conversationStartDateId = c.conversationStartDateId 
                            and dc.conversationId = c.conversationId
                        GROUP BY c.conversationId,
                                eventType,
                                eventName,
                                eventStart,
                                eventEnd,
                                location,
                                originatingDirectionId,
                                initialSessionMediaTypeId

                    ) a
                    LEFT JOIN
                    (SELECT eventValue AuthenticationStatus, conversationId FROM dgdm_simplyenergy.dim_conversation_ivr_events 
                        where eventName = 'AuthenticationStatus' ) ie
                    ON  a.conversationId = ie.conversationId
                    LEFT JOIN dgdm_simplyenergy.dim_conversation_session_flow f
                    on f.conversationId = a.conversationId and f.exitReason = 'FLOW_DISCONNECT'
                    LEFT JOIN dgdm_simplyenergy.dim_conversation_session_flow fd
                    on fd.conversationId = a.conversationId and fd.exitReason = 'DISCONNECTED'
                    LEFT JOIN dgdm_simplyenergy.dim_conversation_session_flow fe
                    on fe.conversationId = a.conversationId and fe.exitReason = 'FLOW_ERROR_DISCONNECT'
                    LEFT JOIN dgdm_simplyenergy.dim_conversation_session_flow fq
                    on fq.conversationId = a.conversationId and fq.exitReason = 'TRANSFER' and fq.transferType in ('ACD', 'ACD_VOICEMAIL')
                    
                    LEFT JOIN (
                        SELECT p.conversationId, concat_ws(',', collect_list(distinct u.userFullName)) AS userNames FROM dgdm_simplyenergy.dim_conversation_participants p
                            JOIN dgdm_simplyenergy.dim_users u
                            ON u.userId = p.userId
                            JOIN dgdm_simplyenergy.dim_conversation_session_segments s
                                ON s.conversationStartDateId = p.conversationStartDateId
                                and s.conversationId = p.conversationId
                            join dgdm_simplyenergy.dim_queues q
                                on s.queueId = q.queueId
                            where p.userId IS NOT NULL and 1 = 1
                            GrOUP BY p.conversationId
                    ) U
                    ON u.conversationId = a.conversationId
                    LEFT JOIN (
                        SELECT p.conversationId, concat_ws(',', collect_list(distinct ut.TeamName)) AS teamNames FROM dgdm_simplyenergy.dim_conversation_participants p
                        JOIN dgdm_simplyenergy.dim_user_teams ut
                        ON ut.userId = p.userId
                        JOIN dgdm_simplyenergy.dim_conversation_session_segments s
                            ON s.conversationStartDateId = p.conversationStartDateId
                            and s.conversationId = p.conversationId
                        join dgdm_simplyenergy.dim_queues q
                            on s.queueId = q.queueId
                        where p.userId IS NOT NULL and 1 = 1
                        GrOUP BY p.conversationId
                    ) UT
                    ON UT.conversationId = a.conversationId
                    GROUP BY a.conversationId,
                    f.conversationId,
                    fd.conversationId,
                    fe.conversationId,
                    fq.conversationId,
                        a.category,
                        a.eventStart,
                        a.eventEnd,
                        a.location,
                        a.originatingDirectionId,
                        a.mediatypeId,
                        ie.AuthenticationStatus,
                        U.userNames,
                        UT.teamNames
                        HAVING eventEnd is NOT NULL)
                    
                    ORDER BY conversationId, eventStart, eventEnd          
        """)
        df = df.toPandas()
        
        csv_content = df.to_csv(index=False).encode('utf-8')
        
        # Overwrite the data in the existing blob
        blob_client.upload_blob(BytesIO(csv_content), blob_type="BlockBlob", content_settings=ContentSettings(content_type="text/csv"),  overwrite=True)
    except Exception as e:
        print(f"An error occurred  in exporting IVRPROCESSMAP:",e)
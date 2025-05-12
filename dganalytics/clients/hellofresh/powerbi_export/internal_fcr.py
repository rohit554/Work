from dganalytics.utils.utils import get_path_vars
from pyspark.sql import SparkSession
import os
import pandas as pd

def export_internal_fcr(spark: SparkSession, tenant: str, region: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    queue_timezones = pd.read_json(os.path.join(tenant_path, 'data', 'config', 'Queue_TimeZone_Mapping_v2.json'))
    queue_timezones = pd.DataFrame(queue_timezones['values'].tolist())
    header = queue_timezones.iloc[0]
    queue_timezones = queue_timezones[1:]
    queue_timezones.columns = header
    queue_mapping = spark.createDataFrame(queue_timezones)
    queue_mapping.createOrReplaceTempView("queue_mapping")

    internal_fcr = spark.sql(f"""
            WITH CTE AS (
                SELECT DISTINCT
                    customerId,
                    mediaType,
                    conversationId,
					userId,
                    from_utc_timestamp(conversationStart, trim(e.timeZone)) as conversationStart,
                    from_utc_timestamp(conversationEnd, trim(e.timeZone)) as conversationEnd,
                    originatingDirection,
                    a.queueId AS queueId, 
                    a.wrapupId AS wrapupId
                FROM (
                    SELECT DISTINCT 
                        conversationId,
                        CASE
                            WHEN mediaType = 'voice' THEN ani
                            WHEN mediaType = 'email' THEN addressFrom
                            WHEN mediaType = 'message' THEN remote
                        END AS customerId,
                        conversationStart,
                        conversationEnd,
                        wrapupId,
                        queueId, 
						userId,  
                        originatingDirection,
                        mediaType
                    FROM (
                        SELECT 
                            conversationId,
                            mediaType,
                            ani,
                            addressFrom,
                            remote,
                            originatingDirection,
                            conversationStart, 
                            conversationEnd,
                            segments.wrapUpCode AS wrapupId,
                            segments.queueId AS queueId,
							userId,
                            row_number() OVER (PARTITION BY conversationId ORDER BY segments.segmentEnd DESC) AS rn1
                        FROM (
                            SELECT 
                                conversationId,
                                conversationStart, 
                                conversationEnd,
                                originatingDirection,
								userId,
                                sessions.mediaType AS mediaType,
                                sessions.ani AS ani,
                                sessions.addressFrom AS addressFrom,
                                sessions.remote AS remote,
                                explode(sessions.segments) AS segments
                            FROM (
                                SELECT 
                                    conversationId,
                                    conversationStart,
                                    conversationEnd,
                                    originatingDirection,
									participants.userId,
                                    explode(participants.sessions) AS sessions
                                FROM (
                                    SELECT 
                                        conversationId,
                                        conversationStart,
                                        conversationEnd,
                                        originatingDirection,
                                        explode(participants) AS participants
                                    FROM (
                                        SELECT 
                                            conversationId,
                                            conversationStart,
                                            conversationEnd,
                                            participants,
                                            originatingDirection,
                                            row_number() OVER (PARTITION BY conversationId ORDER BY recordInsertTime) AS rn
                                        FROM gpc_hellofresh.raw_conversation_details
                                        where extractDate >= date_sub(current_date(), 6)
                                    ) AS raw_conversations
                                    WHERE rn = 1
                                ) AS exploded_participants
                                WHERE participants.purpose = 'agent'
                            ) AS sessions_with_agents
                            WHERE sessions.mediaType IN ('voice', 'email', 'message')
                        ) AS valid_sessions
						where segments.segmentType = 'wrapup'
                    ) AS distinct_conversations
                    WHERE rn1 = 1
                      AND wrapupId IS NOT NULL
                      AND wrapupId NOT IN ('06ec991d-2d1c-4f23-acd3-622896eebc12', '7fc72837-1922-4ec2-9b56-2b052b6adbe3')
                ) AS a
                JOIN gpc_hellofresh.dim_routing_queues d
                  ON a.queueId = d.queueId
                JOIN queue_mapping e
                  ON d.queueName = e.queueName
            )
            SELECT 
                customerId,
                mediaType,
                conversationId,
                conversationStart,
                conversationEnd,
                originatingDirection,
                queueId,
                wrapupId,
				userId,
                0 as FCR_Internal,
                CAST(conversationStart as DATE) conversationStartPart
            FROM CTE
    """)
    # Delete matching records from the internal_fcr table
    internal_fcr.createOrReplaceTempView("internal_fcr_temp")
    spark.sql(f"""
        DELETE FROM pbi_hellofresh.internal_fcr f
        WHERE EXISTS (SELECT 1 FROM internal_fcr_temp ft where ft.conversationStartPart = f.conversationStartPart and ft.conversationId = f.conversationId)
    """)

    # Save new records to the internal_fcr table
    internal_fcr.write.mode("append").insertInto("pbi_hellofresh.internal_fcr")

    # Return data of the last 6 months from the internal_fcr table
    last_6_months_data = spark.sql("""
        SELECT customerId,
                mediaType,
                conversationId,
                conversationStart,
                conversationEnd,
                originatingDirection,
                queueId,
                wrapupId,
                userId,
                CASE WHEN 
                    LEAD(conversationStart) OVER (PARTITION BY customerId, wrapupId ORDER BY conversationStart) IS NULL 
                    OR (DATEDIFF(MINUTE, conversationStart, 
                    LEAD(conversationStart) OVER (PARTITION BY customerId, wrapupId ORDER BY conversationStart)) > 7200) 
                    THEN 1 ELSE 0 
                END AS FCR_Internal
                
        FROM pbi_hellofresh.internal_fcr
        WHERE conversationStartPart >= date_sub(current_date(), 365)
    """)
    
    return last_6_months_data

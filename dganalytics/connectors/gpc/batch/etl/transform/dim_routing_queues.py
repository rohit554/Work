from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import transform_parser, get_dbname

if __name__ == "__main__":
    tenant, run_id, extract_date = transform_parser()
    spark = get_spark_session(app_name="dim_routing_queues", tenant=tenant, default_db=get_dbname(tenant))

    queues = spark.sql(f"""
                    select 
                        id as queueId,
                        name as queueName,
                        acwSettings.wrapupPrompt as wrapupPrompt,
                        mediaSettings.call.serviceLevel.durationMs/1000 as callSLDuration,
                        mediaSettings.call.serviceLevel.percentage * 100 as callSLPercentage,
                        mediaSettings.callback.serviceLevel.durationMs/1000 as callbackSLDuration,
                        mediaSettings.callback.serviceLevel.percentage * 100 as callbackSLPercentage,
                        mediaSettings.chat.serviceLevel.durationMs/1000 as chatSLDuration,
                        mediaSettings.chat.serviceLevel.percentage * 100 as chatSLPercentage,
                        mediaSettings.email.serviceLevel.durationMs/1000 as emailSLDuration,
                        mediaSettings.email.serviceLevel.percentage * 100 as emailSLPercentage,
                        mediaSettings.message.serviceLevel.durationMs/1000 as messageSLDuration,
                        mediaSettings.message.serviceLevel.percentage * 100 as messageSLPercentage
                        from raw_routing_queues 
	            """)
    queues.coalesce(1).write.format("delta").saveAsTable("dim_routing_queues", mode="overwrite")

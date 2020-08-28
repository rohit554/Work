from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger, transform_parser, get_dbname

if __name__ == "__main__":
    tenant, run_id, extract_date = transform_parser()
    app_name = "dim_routing_queues"
    spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=get_dbname(tenant))
    
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("Overwriting into dim_routing_queues")
        queues = spark.sql("""
                        select 
                            id as queueId,
                            name as queueName,
                            acwSettings.wrapupPrompt as wrapupPrompt,
                            cast(mediaSettings.call.serviceLevel.durationMs/1000 as float) as callSLDuration,
                            cast(mediaSettings.call.serviceLevel.percentage * 100 as float) as callSLPercentage,
                            cast(mediaSettings.callback.serviceLevel.durationMs/1000 as float) as callbackSLDuration,
                            cast(mediaSettings.callback.serviceLevel.percentage * 100 as float) as callbackSLPercentage,
                            cast(mediaSettings.chat.serviceLevel.durationMs/1000 as float) as chatSLDuration,
                            cast(mediaSettings.chat.serviceLevel.percentage * 100 as float) as chatSLPercentage,
                            cast(mediaSettings.email.serviceLevel.durationMs/1000 as float) as emailSLDuration,
                            cast(mediaSettings.email.serviceLevel.percentage * 100 as float) as emailSLPercentage,
                            cast(mediaSettings.message.serviceLevel.durationMs/1000 as float) as messageSLDuration,
                            cast(mediaSettings.message.serviceLevel.percentage * 100 as float) as messageSLPercentage
                            from raw_routing_queues 
                    """)
        queues.coalesce(1).write.format("delta").saveAsTable("dim_routing_queues", mode="overwrite")
    except Exception as e:
        logger.error(str(e))

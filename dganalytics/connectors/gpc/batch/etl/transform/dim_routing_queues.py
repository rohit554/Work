from pyspark.sql import SparkSession


def dim_routing_queues(spark: SparkSession, extract_date: str):
    routing_queues = spark.sql("""
                        insert overwrite dim_routing_queues
                        select distinct 
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

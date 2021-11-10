from pyspark.sql import SparkSession

def fact_sentiments(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    sentiment_metrics = spark.sql(f"""
        select
            conversation.id as conversationId, sentimentScore, sentimentTrend,
            participantMetrics.agentDurationPercentage, participantMetrics.customerDurationPercentage , participantMetrics.silenceDurationPercentage ,
            participantMetrics.ivrDurationPercentage , participantMetrics.acdDurationPercentage , participantMetrics.otherDurationPercentage
        from
            raw_sentiments where extractDate = '{extract_date}'
            and  extractIntervalStartTime = '{extract_start_time}' and extractIntervalEndTime = '{extract_end_time}'
    """)

    sentiment_metrics.registerTempTable("sentiment_metrics")
    spark.sql("""delete from fact_sentiments where conversationId in (
                            select distinct conversationId from sentiment_metrics)""")
    spark.sql(
        "insert into fact_sentiments select * from sentiment_metrics")

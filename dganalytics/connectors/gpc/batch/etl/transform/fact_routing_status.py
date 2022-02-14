from pyspark.sql import SparkSession


def fact_routing_status(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    routing_status = spark.sql(f"""
                                select distinct userId, routingStatus.startTime,
                                coalesce(routingStatus.endTime, cast('{extract_end_time}' as timestamp)) endTime, routingStatus.routingStatus,
                                cast(routingStatus.startTime as date) as startDate, 
                                sourceRecordIdentifier, soucePartition from (
    select userId, explode(routingStatus) as routingStatus,
        recordIdentifier as sourceRecordIdentifier,
        concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
         from raw_users_details where
                            extractDate = '{extract_date}'
                            and  extractIntervalStartTime = '{extract_start_time}' and extractIntervalEndTime = '{extract_end_time}')
                                """)
    routing_status.createOrReplaceTempView("routing_status")
    spark.sql("""
                delete from fact_routing_status a where exists (
                        select 1 from routing_status b where a.userId = b.userId
                        and a.startDate = b.startDate and a.startTime = b.startTime
                )
                """)
    spark.sql("insert into fact_routing_status select * from routing_status")


from pyspark.sql import SparkSession


def fact_primary_presence(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    primary_presence = spark.sql(f"""
                                    select distinct userId, primaryPresence.startTime,
                                    coalesce(primaryPresence.endTime, cast('{extract_end_time}' as timestamp)) endTime, primaryPresence.systemPresence,
            cast(primaryPresence.startTime as date) as startDate, sourceRecordIdentifier, soucePartition from (
        select userId, explode(primaryPresence) as primaryPresence,
            recordIdentifier as sourceRecordIdentifier,
            concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
         from raw_users_details
                    where extractDate = '{extract_date}'
                    and  extractIntervalStartTime = '{extract_start_time}' and extractIntervalEndTime = '{extract_end_time}')
                                    """)
    primary_presence.registerTempTable("primary_presence")
    spark.sql("""
                delete from fact_primary_presence a where exists (
                        select 1 from primary_presence b where a.userId = b.userId
                        and a.startDate = b.startDate and a.startTime = b.startTime
                )
                """)
    spark.sql("insert into fact_primary_presence select * from primary_presence")

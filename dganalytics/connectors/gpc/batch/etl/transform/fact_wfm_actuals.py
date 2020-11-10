from pyspark.sql import SparkSession


def fact_wfm_actuals(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    wfm_actuals = spark.sql(f"""
    select 
 userId, managementUnitId,
cast(replace(replace(startDate, 'Z', ''), 'z', '') as timestamp) + cast(concat("INTERVAL ", startOffsetSeconds, " SECONDS") as INTERVAL) as startDate,
cast(cast(replace(replace(startDate, 'Z', ''), 'z', '') as timestamp) + cast(concat("INTERVAL ", startOffsetSeconds, " SECONDS") as INTERVAL) as date) as startDatePart,
cast(replace(replace(startDate, 'Z', ''), 'z', '') as timestamp) + cast(concat("INTERVAL ", endOffsetSeconds, " SECONDS") as INTERVAL) as endDate,
actualActivityCategory, sourceRecordIdentifier, soucePartition
from (
select userId, managementUnitId, startDate, endDate, actualsEndDate, actuals.actualActivityCategory, 
actuals.startOffsetSeconds, actuals.endOffsetSeconds, sourceRecordIdentifier, soucePartition
from (
select 
userId, managementUnitId, startDate, endDate, actualsEndDate, explode(actuals) actuals, recordIdentifier as sourceRecordIdentifier,
        concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
	from raw_wfm_adherence where
                            extractDate = '{extract_date}'
                            and  extractIntervalStartTime = '{extract_start_time}' and extractIntervalEndTime = '{extract_end_time}'
) a
)
                """)

    wfm_actuals.registerTempTable("wfm_actuals")
    spark.sql("""
            merge into fact_wfm_actuals as target
                using wfm_actuals as source
                on source.userId = target.userId
                    and source.startDate = target.startDate
                    and source.startDatePart = target.startDatePart
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """)

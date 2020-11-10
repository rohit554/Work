from pyspark.sql import SparkSession


def fact_wfm_day_metrics(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    wfm_day_metrics = spark.sql(f"""
            select 
userId, managementUnitId,
cast(replace(replace(startDate, 'Z', ''), 'z', '') as timestamp) + cast(concat("INTERVAL ", dayStartOffsetSecs, " SECONDS") as INTERVAL) as startDate,
cast(cast(replace(replace(startDate, 'Z', ''), 'z', '') as timestamp) + cast(concat("INTERVAL ", dayStartOffsetSecs, " SECONDS") as INTERVAL) as date) as startDatePart,
coalesce(cast(replace(replace(startDate, 'Z', ''), 'z', '') as timestamp) + 
	cast(concat("INTERVAL ", nextdayStartOffsetSecs, " SECONDS") as INTERVAL), 
	cast(replace(replace(endDate, 'Z', ''), 'z', '') as timestamp)) as endDate,
actualLengthSecs,
adherenceScheduleSecs, conformanceActualSecs, conformanceScheduleSecs,
dayStartOffsetSecs, exceptionCount, exceptionDurationSecs,
impactSeconds, scheduleLengthSecs, sourceRecordIdentifier, soucePartition
from (
select userId, managementUnitId, startDate, endDate, actualsEndDate, dayMetrics.actualLengthSecs,
dayMetrics.adherenceScheduleSecs, dayMetrics.conformanceActualSecs, dayMetrics.conformanceScheduleSecs,
dayMetrics.dayStartOffsetSecs, dayMetrics.exceptionCount, dayMetrics.exceptionDurationSecs,
dayMetrics.impactSeconds, dayMetrics.scheduleLengthSecs,
lead(dayMetrics.dayStartOffsetSecs) over(partition by userId, startDate order by dayMetrics.dayStartOffsetSecs asc) as nextdayStartOffsetSecs,
sourceRecordIdentifier, soucePartition
from (
select 
userId, managementUnitId, startDate, endDate, actualsEndDate, explode(dayMetrics) dayMetrics, 
recordIdentifier as sourceRecordIdentifier,
concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
from raw_wfm_adherence
where extractDate = '{extract_date}'
    and  extractIntervalStartTime = '{extract_start_time}' and extractIntervalEndTime = '{extract_end_time}')
)
                                    """)
    wfm_day_metrics.registerTempTable("wfm_day_metrics")
    spark.sql("""
                merge into fact_wfm_day_metrics as target
                    using wfm_day_metrics as source
                    on source.userId = target.userId
                        and source.startDate = target.startDate and source.startDatePart = target.startDatePart
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
            """)

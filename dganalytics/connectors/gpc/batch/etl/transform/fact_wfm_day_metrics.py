from pyspark.sql import SparkSession


def fact_wfm_day_metrics(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    wfm_day_metrics = spark.sql(f"""
    select 
    wfm.userId, managementUnitId, wfm.startDate, wfm.startDatePart, wfm.endDate, 
        wfm.actualLengthSecs,
wfm.adherenceScheduleSecs, wfm.conformanceActualSecs, wfm.conformanceScheduleSecs,
wfm.dayStartOffsetSecs, wfm.exceptionCount, wfm.exceptionDurationSecs,
wfm.impactSeconds, wfm.scheduleLengthSecs, wfm.sourceRecordIdentifier, wfm.soucePartition
     from (
            select 
distinct userId, managementUnitId, 
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
rwa.userId, mu.managementId  as managementUnitId, rwa.startDate, rwa.endDate, rwa.actualsEndDate, explode(rwa.dayMetrics) dayMetrics, 
rwa.recordIdentifier as sourceRecordIdentifier,
concat(rwa.extractDate, '|', rwa.extractIntervalStartTime, '|', rwa.extractIntervalEndTime) as soucePartition
from raw_wfm_adherence rwa, raw_management_unit_users mu
where rwa.extractDate = '{extract_date}'
    and  rwa.extractIntervalStartTime = '{extract_start_time}' and rwa.extractIntervalEndTime = '{extract_end_time}'
    and rwa.userId = mu.id
    and rwa.managementUnitId = mu.managementId)
)
    ) wfm
    where wfm.startDate != wfm.endDate
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

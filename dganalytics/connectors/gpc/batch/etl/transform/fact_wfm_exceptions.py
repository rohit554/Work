from pyspark.sql import SparkSession


def fact_wfm_exceptions(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    wfm_exceptions = spark.sql(f"""
    select 
 userId, managementUnitId,
cast(replace(replace(startDate, 'Z', ''), 'z', '') as timestamp) + cast(concat("INTERVAL ", startOffsetSeconds, " SECONDS") as INTERVAL) as startDate,
cast(cast(replace(replace(startDate, 'Z', ''), 'z', '') as timestamp) + cast(concat("INTERVAL ", startOffsetSeconds, " SECONDS") as INTERVAL) as date) as startDatePart,
cast(replace(replace(startDate, 'Z', ''), 'z', '') as timestamp) + cast(concat("INTERVAL ", endOffsetSeconds, " SECONDS") as INTERVAL) as endDate,
actualActivityCategory, impact, routingStatus, scheduledActivityCategory,
scheduledActivityCodeId, systemPresence,
get_json_object(lookupIdToSecondaryPresenceId, concat('$.', secondaryPresenceLookupId)) secondaryPresenceId, 
sourceRecordIdentifier, soucePartition
from (
select userId, managementUnitId, startDate, endDate, actualsEndDate, exceptionInfo.actualActivityCategory, 
exceptionInfo.startOffsetSeconds, exceptionInfo.endOffsetSeconds,
exceptionInfo.impact, exceptionInfo.routingStatus, exceptionInfo.scheduledActivityCategory,
exceptionInfo.scheduledActivityCodeId, exceptionInfo.secondaryPresenceLookupId, 
exceptionInfo.systemPresence, lookupIdToSecondaryPresenceId, sourceRecordIdentifier, soucePartition
from (
select 
userId, managementUnitId, startDate, endDate, actualsEndDate, explode(exceptionInfo) as exceptionInfo , lookupIdToSecondaryPresenceId,
recordIdentifier as sourceRecordIdentifier,
concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
	from raw_wfm_adherence
     where extractDate = '{extract_date}'
    and  extractIntervalStartTime = '{extract_start_time}' and extractIntervalEndTime = '{extract_end_time}'
) a
)
                                    """)
    wfm_exceptions.registerTempTable("wfm_exceptions")
    spark.sql("""
                merge into fact_wfm_exceptions as target
                    using wfm_exceptions as source
                    on source.userId = target.userId
                        and source.startDate = target.startDate and source.startDatePart = target.startDatePart
                        and source.endDate = target.endDate
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
            """)

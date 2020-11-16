from pyspark.sql import SparkSession


def fact_wfm_exceptions(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    wfm_exceptions = spark.sql(f"""
    select 
    wfm.userId, managementUnitId, wfm.startDate, wfm.startDatePart, wfm.endDate,
    wfm.actualActivityCategory, wfm.impact, wfm.routingStatus, wfm.scheduledActivityCategory,
    wfm.scheduledActivityCodeId, wfm.systemPresence,
    wfm.secondaryPresenceId, wfm.sourceRecordIdentifier, wfm.soucePartition
     from (
    select 
 distinct userId, managementUnitId, 
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
rwa.userId, mu.managementId  as managementUnitId, rwa.startDate, rwa.endDate, rwa.actualsEndDate, explode(rwa.exceptionInfo) as exceptionInfo , rwa.lookupIdToSecondaryPresenceId,
rwa.recordIdentifier as sourceRecordIdentifier,
concat(rwa.extractDate, '|', rwa.extractIntervalStartTime, '|', rwa.extractIntervalEndTime) as soucePartition
	from raw_wfm_adherence rwa, raw_management_unit_users mu
     where rwa.extractDate = '{extract_date}'
    and  rwa.extractIntervalStartTime = '{extract_start_time}' and rwa.extractIntervalEndTime = '{extract_end_time}'
    and rwa.userId = mu.id
    and rwa.managementUnitId = mu.managementId
) a
)
    ) wfm
where wfm.startDate != wfm.endDate
                                    """)
    wfm_exceptions.registerTempTable("wfm_exceptions")
    spark.sql("""
                merge into fact_wfm_exceptions as target
                    using wfm_exceptions as source
                    on source.userId = target.userId
                        and source.startDate = target.startDate and source.startDatePart = target.startDatePart
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
            """)

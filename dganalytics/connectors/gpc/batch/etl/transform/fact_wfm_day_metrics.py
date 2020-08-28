from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger, transform_parser, get_dbname
from delta.tables import DeltaTable

if __name__ == "__main__":
    tenant, run_id, extract_date = transform_parser()
    app_name = "fact_wfm_day_metrics"
    spark = get_spark_session(
        app_name=app_name, tenant=tenant, default_db=get_dbname(tenant))
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("Upserting into fact_wfm_day_metrics")
        routing_status = spark.sql(f"""
                                    select userId, startDate, actualsEndDate, endDate, impact, 
    dayMetrics.actualLengthSecs,dayMetrics.adherencePercentage, dayMetrics.adherenceScheduleSecs,
    dayMetrics.conformanceActualSecs, dayMetrics.conformancePercentage, 
    dayMetrics.conformanceScheduleSecs, dayMetrics.dayStartOffsetSecs,
    dayMetrics.exceptionCount, dayMetrics.exceptionDurationSecs, dayMetrics.impactSeconds,
    dayMetrics.scheduleLengthSecs, 
    cast(startDate as date) startDatePart from (
    select data.userId, data.startDate, data.actualsEndDate, data.endDate,data.impact,
    explode(data.dayMetrics) as dayMetrics from (
    select explode(data) as data from raw_wfm_adherence where extractDate = '{extract_date}')
    ) 
                                    """)
        DeltaTable.forName(spark, "fact_wfm_day_metrics").alias("target").merge(routing_status.coalesce(2).alias("source"),
                                                                            """source.userId = target.userId
                and source.startDate = target.startDate and source.startDatePart = target.startDatePart
                and source.endDate = target.endDate""").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except Exception as e:
        logger.error(str(e))

from pyspark.sql import SparkSession


def fact_wfm_day_metrics(spark: SparkSession, extract_date: str):
    wfm_day_metrics = spark.sql(f"""
                                    select distinct userId, startDate, actualsEndDate, endDate, impact, 
    dayMetrics.actualLengthSecs,dayMetrics.adherencePercentage, dayMetrics.adherenceScheduleSecs,
    dayMetrics.conformanceActualSecs, dayMetrics.conformancePercentage, 
    dayMetrics.conformanceScheduleSecs, dayMetrics.dayStartOffsetSecs,
    dayMetrics.exceptionCount, dayMetrics.exceptionDurationSecs, dayMetrics.impactSeconds,
    dayMetrics.scheduleLengthSecs, 
    cast(startDate as date) startDatePart from (
    select userId, startDate, actualsEndDate, endDate, impact,
    explode(dayMetrics) as dayMetrics from (
    select * from raw_wfm_adherence where extractDate = '{extract_date}')
    ) 
                                    """)
    wfm_day_metrics.registerTempTable("wfm_day_metrics")
    spark.sql("""
                merge into fact_wfm_day_metrics as target
                    using wfm_day_metrics as source
                    on source.userId = target.userId
                        and source.startDate = target.startDate and source.startDatePart = target.startDatePart
                        and source.endDate = target.endDate
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
            """)

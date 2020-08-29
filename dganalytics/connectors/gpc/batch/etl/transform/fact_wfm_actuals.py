from pyspark.sql import SparkSession


def fact_wfm_actuals(spark: SparkSession, extract_date: str):
    wfm_actuals = spark.sql(f"""
        create temporary view actuals as 
        select userId,startDate,actualsEndDate, endDate, actuals.actualActivityCategory,actuals.endOffsetSeconds, actuals.startOffsetSeconds,
                                    cast(startDate as date) startDatePart
    from (
    select data.userId, data.startDate, data.actualsEndDate, data.endDate,data.impact,
    explode(data.actuals) as actuals from (
    select explode(data) as data from raw_wfm_adherence where extractDate = '{extract_date}')
    ) 
                            """)
    wfm_actuals.registerTempTable("wfm_actuals")
    spark.sql("""
            merge into fact_wfm_actuals as target
                using wfm_actuals as source
                on source.userId = target.userId
                    and ource.startDate = target.startDate
                    and source.startDatePart = target.startDatePart
                    and source.endDate = target.endDate
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """)

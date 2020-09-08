from pyspark.sql import SparkSession


def fact_wfm_actuals(spark: SparkSession, extract_date: str):
    orig_wfm_actuals = spark.sql(f"""
        select distinct userId,startDate,actualsEndDate, endDate, actuals.actualActivityCategory,actuals.endOffsetSeconds, actuals.startOffsetSeconds,
                                    cast(startDate as date) startDatePart
    from (
    select data.userId, data.startDate, data.actualsEndDate, data.endDate,data.impact,
    explode(data.actuals) as actuals from (
    select explode(data) as data from raw_wfm_adherence where extractDate = '{extract_date}')
    ) 
                            """)
    orig_wfm_actuals.registerTempTable("orig_wfm_actuals")
    wfm_actuals = spark.sql("""
    select userId, startDate, actualsEndDate, endDate, actualActivityCategory, endOffsetSeconds, startOffsetSeconds, startDatePart from (
            select *, row_number() over(partition by userId, startDate, actualsEndDate, startOffsetSeconds 
        order by startOffsetSeconds, endOffsetSeconds desc) as rnk from orig_wfm_actuals ) a
        where a.rnk = 1
                """)

    wfm_actuals.registerTempTable("wfm_actuals")
    spark.sql("""
            merge into fact_wfm_actuals as target
                using wfm_actuals as source
                on source.userId = target.userId
                    and source.startDate = target.startDate
                    and source.startDatePart = target.startDatePart
                    and source.endDate = target.endDate
                    and source.startOffsetSeconds = target.startOffsetSeconds
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """)

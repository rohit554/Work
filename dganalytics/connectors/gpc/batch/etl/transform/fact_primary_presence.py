from pyspark.sql import SparkSession


def fact_primary_presence(spark: SparkSession, extract_date: str):
    primary_presence = spark.sql(f"""
                                    select distinct userId, primaryPresence.startTime, primaryPresence.endTime, primaryPresence.systemPresence, 
            cast(primaryPresence.startTime as date) as startDate from (
        select userId, explode(primaryPresence) as primaryPresence from raw_users_details where extractDate = '{extract_date}')
                                    """)
    primary_presence.registerTempTable("primary_presence")
    spark.sql("""
                merge into fact_primary_presence as target
                        using primary_presence as source
                        on source.userId = target.userId
                and source.startTime = target.startTime and source.startDate = target.startDate
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """)

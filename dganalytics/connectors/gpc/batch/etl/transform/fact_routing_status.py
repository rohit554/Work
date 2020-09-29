from pyspark.sql import SparkSession


def fact_routing_status(spark: SparkSession, extract_date: str):
    routing_status = spark.sql(f"""
                                select distinct userId, routingStatus.startTime, routingStatus.endTime, routingStatus.routingStatus,
                                cast(routingStatus.startTime as date) as startDate from (
    select userId, explode(routingStatus) as routingStatus from raw_users_details where extractDate = '{extract_date}')
                                """)
    routing_status.registerTempTable("routing_status")
    spark.sql("""
                delete from fact_routing_status a where exists (
                        select 1 from routing_status b where a.userId = b.userId
                        and a.startDate = b.startDate and a.startTime = b.startTime
                        and a.endTime = b.endTime
                ) 
                """)
    spark.sql("insert into fact_routing_status select * from routing_status")

    '''
    spark.sql("""
                merge into fact_routing_status as target
                    using routing_status as source
                    on source.userId = target.userId
                        and source.startTime = target.startTime
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
            """)

    '''

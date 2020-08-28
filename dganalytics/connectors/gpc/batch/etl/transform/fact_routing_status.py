from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger, transform_parser, get_dbname
from delta.tables import DeltaTable

if __name__ == "__main__":
    tenant, run_id, extract_date = transform_parser()
    app_name = "fact_routing_status"
    spark = get_spark_session(
        app_name=app_name, tenant=tenant, default_db=get_dbname(tenant))
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("Upserting into fact_routing_status")
        routing_status = spark.sql(f"""
                                    select userId, routingStatus.startTime, routingStatus.endTime, routingStatus.routingStatus,
                                    cast(routingStatus.startTime as date) as startDate from (
        select userId, explode(routingStatus) as routingStatus from raw_users_details where extractDate = '{extract_date}')
                                    """)
        DeltaTable.forName(spark, "fact_routing_status").alias("target").merge(routing_status.coalesce(2).alias("source"),
                                                                            """source.userId = target.userId
                and source.startTime = target.startTime""").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    except Exception as e:
        logger.error(str(e))
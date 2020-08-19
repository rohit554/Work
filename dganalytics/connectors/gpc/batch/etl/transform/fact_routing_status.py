from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import transform_parser, get_dbname
from delta.tables import DeltaTable

if __name__ == "__main__":
    tenant, run_id, extract_date = transform_parser()
    spark = get_spark_session(
        app_name="fact_routing_status", tenant=tenant, default_db=get_dbname(tenant))

    routing_status = spark.sql(f"""
								select userId, routingStatus.startTime, routingStatus.endTime, routingStatus.routingStatus,
                                cast(routingStatus.startTime as date) as startDate from (
	select userId, explode(routingStatus) as routingStatus from raw_users_details where extractDate = '{extract_date}')
								""")
    DeltaTable.forName(spark, "fact_routing_status").alias("target").merge(routing_status.coalesce(2).alias("source"),
                                                                         """source.userId = target.userId
			and source.startTime = target.startTime""").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

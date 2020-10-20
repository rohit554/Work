from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

pipeline = [
    {
        "$project": {
            "_id": 0.0,
            "campaign_id": "$_id",
            "name": 1.0,
            "start_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$start_date"
                }
            },
            "end_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$end_date"
                }
            },
            "org_id": 1.0,
            "is_active": 1.0,
            "is_deleted": 1.0
        }
    }
]

schema = StructType([StructField('campaign_id', StructType([StructField('oid', StringType(), True)]), True),
                     StructField('end_date', StringType(), True), StructField(
                         'is_active', BooleanType(), True),
                     StructField('is_deleted', BooleanType(), True), StructField(
                         'name', StringType(), True),
                     StructField('org_id', StringType(), True), StructField('start_date', StringType(), True)])


def get_campaign(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema)
    df.registerTempTable("campaign")
    df = spark.sql("""
                    select  campaign_id.oid campaignId,
                            cast(start_date as date) start_date,
                            cast(end_date as date) endDate,
                            is_active isActive,
                            is_deleted isDeleted,
                            name name,
                            lower(org_id) orgId
                    from campaign
                """)
    '''
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.campaign")
    '''
    delta_table_partition_ovrewrite(df, "dg_performance_management.campaign", ['orgId'])

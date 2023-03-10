from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

pipeline = [
    {
        "$match": {
            "is_active": True,
            "is_deleted": False
        }
    }, 
    {
        "$unwind": {
            "path": "$kpi"
        }
    }, 
    {
        "$unwind": {
            "path": "$quartile"
        }
    }, 
    {
        "$match": {
            "$expr": {
                "$and": [
                    {
                        "$eq": [
                            "$kpi.kpi_name",
                            "$quartile.kpi_name"
                        ]
                    }
                ]
            }
        }
    }, 
    {
        "$project": {
#             "id": 0,
            "campaign_id": "$_id",
            "kpi_id": "$kpi._id",
            "name": "$kpi.kpi_name",
            "overall_aggr": "$kpi.agg_function",
            "entity_name": "$kpi.entity_name",
            "field_name": "$kpi.entity_col",
            "display_name": "$kpi.kpi_display_name",
            "unit": "$kpi.unit",
            "quartile": "$quartile.quartile",
            "target": "$quartile.quartile_target",
            "operator": "$quartile.quartile_operator",
            "orgId": "$org_id"
        }
    }
]

schema = StructType([StructField('campaign_id', StructType([StructField('oid', StringType(), True)]), True),
                     StructField('kpi_id', StructType([StructField('oid', StringType(), True)]), True),
                     StructField('name', StringType(), True),
                     StructField('overall_aggr', StringType(), True),
                     StructField('entity_name', StringType(), True),
                     StructField('field_name', StringType(), True),
                     StructField('display_name', StringType(), True),
                     StructField('unit', StringType(), True),
                     StructField('quartile', StringType(), True),
                     StructField('target', DoubleType(), True),
                     StructField('operator', StringType(), True),
                     StructField('orgId', StringType(), True)])


def get_campaign_kpis(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema)
    df.createOrReplaceTempView("campaign_kpis")
    df = spark.sql("""
                    select  distinct campaign_id.oid campaignId,
                            kpi_id.oid kpiId,
                            name name,
                            overall_aggr overallAggr,
                            entity_name entityName,
                            field_name fieldName,
                            display_name displayName,
                            unit unit,
                            quartile quartile,
                            target target,
                            operator operator,
                            lower(OrgId) orgId
                    from campaign_kpis
                """)
    
    delta_table_partition_ovrewrite(
        df, "dg_performance_management.campaign_kpis", ['orgId'])
    

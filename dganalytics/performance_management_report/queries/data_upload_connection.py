from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType

data_upload_connection_schema = StructType([
    StructField("connectionId", StructType([StructField('oid', StringType(), True)]), True),
    StructField("connection_name", StringType(), True),
    StructField("attr_display_name", StringType(), True),
    StructField("attr_dict_key", StringType(), True),
    StructField("datatype", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("overall_aggr", StringType(), True),
    StructField("formula", StringType(), True),
    StructField("num_aggr", StringType(), True),
    StructField("num", StringType(), True),
    StructField("denom_aggr", StringType(), True),
    StructField("denom", StringType(), True),
    StructField("orgId", StringType(), True)])

# Connections
def get_connections(spark):
    connectionPipeline = [
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {
                            "$eq": [
                                "$is_active",
                                True
                            ]
                        }
                    ]
                }
            }
        }, 
        {
            "$project": {
                "name": 1.0,
                "attributes": 1.0,
                "org_id": 1.0
            }
        }, 
        {
            "$unwind": {
                "path": "$attributes"
            }
        }, 
        {
            "$project": {
                "connectionId": "$_id",
                "connection_name": "$name",
                "attr_display_name": "$attributes.display_name",
                "attr_dict_key": "$attributes.dictionary_key",
                "datatype": "$attributes.datatype",
                "unit": "$attributes.unit",
                "overall_aggr": "$attributes.aggregation",
                "formula": "$attributes.kpi_formula.formula",
                "num_aggr": "$attributes.kpi_formula.num_aggr",
                "num": "$attributes.kpi_formula.num",
                "denom_aggr": "$attributes.kpi_formula.denom_aggr",
                "denom": "$attributes.kpi_formula.denom",
                "orgId": "$org_id"
            }
        }
    ]
    df = exec_mongo_pipeline(
        spark, connectionPipeline, 'Data_Upload_Connection',
        data_upload_connection_schema)

    df.createOrReplaceTempView("data_upload_connections")


    df = spark.sql("""
                    select  distinct connectionId.oid as connectionId,
                            connection_name,
                            attr_display_name,
                            attr_dict_key,
                            datatype,
                            unit,
                            overall_aggr,
                            formula,
                            num_aggr,
                            num,
                            denom_aggr,
                            denom,
                            lower(orgId) as orgId
                    from data_upload_connections
                """)

    delta_table_partition_ovrewrite(
        df, "dg_performance_management.data_upload_connections", ['orgId'])
from dganalytics.utils.utils import exec_mongo_pipeline, get_active_org
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime, timedelta
from pyspark.sql.functions import lower

data_upload_connection_schema = StructType([
    StructField("connectionId", StringType(), True),
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
    extract_start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    connectionPipeline = [
        {
            "$match": {
                "is_active" : True,
                "org_id" :  {
                    '$in' : get_active_org()
                  },
                "update_date":{
                    '$gte': { '$date': extract_start_time }
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
    df = exec_mongo_pipeline(spark, connectionPipeline, 'Data_Upload_Connection', data_upload_connection_schema)
    df = df.withColumn("orgId", lower(df["orgId"]))
    df = df.dropDuplicates()
    df.createOrReplaceTempView("data_upload_connections")

    spark.sql("""
        MERGE INTO dg_performance_management.data_upload_connections AS target
        USING data_upload_connections AS source
        ON target.orgId = source.orgId
        AND target.connectionId = source.connectionId
        AND target.attr_dict_key = source.attr_dict_key
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *        
        """)


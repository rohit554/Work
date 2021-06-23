import requests as rq
import pandas as pd
import json
import os
from pyspark.sql.functions import lit
from datetime import datetime
from pathlib import Path
from pyspark.sql.types import StructType, IntegerType, TimestampType, StructField, StringType
from pyspark.sql import SparkSession, DataFrame, Row
from dganalytics.utils.utils import get_spark_session, flush_utils, get_path_vars, export_powerbi_parquet
from dganalytics.connectors.gpc.gpc_utils import get_interval, get_api_url, get_dbname, authorize, gpc_request, \
    extract_parser, gpc_utils_logger
from pyspark.sql.functions import to_timestamp, lit, to_date

schema = StructType([StructField('conversationId', StringType(), True),
  StructField('fcr', IntegerType(), True),
  StructField('csat', IntegerType(), True),
  StructField('nps', IntegerType(), True)])

def create_fact_conversation_survey(spark: SparkSession, db_name: str):
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_name}.fact_conversation_survey
            (
                conversationId STRING,
                csat INT,
                nps INT,
                fcr INT,
                insertTimestamp TIMESTAMP
            )
            using delta
            PARTITIONED BY (insertTimestamp)
            LOCATION '{db_path}/{db_name}/fact_conversation_survey'
            """)
    return True

def merge_fact_conversation_survey(df, extract_start_time, extract_end_time, spark: SparkSession, db_name: str):
    current_timestamp = datetime.utcnow()
    df = df.withColumn("insertTimestamp", to_timestamp(lit(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))))
    df = df.drop_duplicates()
    
    df.createOrReplaceTempView("conversation_surveys")
    sdf = spark.sql("SELECT * FROM conversation_surveys")
    sdf.show()
    spark.sql(f"""  MERGE INTO {db_name}.fact_conversation_survey
                    USING conversation_surveys
                        ON conversation_surveys.conversationId = fact_conversation_survey.conversationId
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                """)

    return True


def get_conversations():
	conversations = spark.sql(f"""  SELECT DISTINCT conversationId
	                                FROM gpc_salmatcolesonline.dim_conversations
	                                WHERE   conversationEnd IS NOT NULL
	                                        AND conversationEnd BETWEEN '{extract_start_time}' AND '{extract_end_time}'
	    """)
	return conversations

def transform_conversation_surveys(convs, list):
    if convs != None and len(convs) > 0:
        for conv in convs:
            if conv != None and conv["participants"] != None and len(conv["participants"]) > 0:
                
                for participant in conv["participants"]:
                    has_survey = False
                    dict = { "csat": None, "fcr": None, "nps": None, "conversationId": conv["id"] }
                    if participant != None and participant["attributes"] != None and participant["attributes"] != {}:
                        if "Survey CSAT Agent" in participant["attributes"]:
                            csat = participant["attributes"]["Survey CSAT Agent"]
                            dict["csat"] = int(csat) if csat != "" and csat.isnumeric() else None
                            has_survey = True
                        if "Survey NPS" in participant["attributes"]:
                            nps = participant["attributes"]["Survey NPS"]
                            dict["nps"] = int(nps) if nps != "" and nps.isnumeric() else None
                            has_survey = True
                        if "Survey Resolution" in participant["attributes"]:
                            fcr = participant["attributes"]["Survey Resolution"]
                            dict["fcr"] = int(fcr) if fcr != "" and fcr.isnumeric() else None
                            has_survey = True
                    if has_survey:
                        if dict["fcr"] != None or dict["csat"] != None or dict["nps"] != None:
                            list.append(dict.copy())

    return list

if __name__ == '__main__':
    tenant, run_id, extract_start_time, extract_end_time, api_name = extract_parser()

    db_name = get_dbname(tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    app_name = "gpc_extract_" + api_name
    spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=db_name)

    global logger
    logger = gpc_utils_logger(tenant, app_name)

    try:
        create_fact_conversation_survey(spark, db_name)
        
        conversations = get_conversations()
        
        conv_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        list = []
        if conversations != None and not conversations.rdd.isEmpty():
            api_headers = authorize(tenant)
            
            for conversation in conversations.rdd.collect():
                
                # Extract
                # Conversation export API
                conv_export_resp = rq.get(f"{get_api_url(tenant)}/api/v2/conversationexport?ids={conversation['conversationId']}",
                    headers=api_headers)
                
                convs = conv_export_resp.json()
                
                # Transform
                list = transform_conversation_surveys(convs, list)
        
        conf_df = spark.createDataFrame(list,schema) 
        
        # Load
        if conf_df != None and not conf_df.rdd.isEmpty():
            #logic to insert data in the fact table
            merge_fact_conversation_survey(conf_df, extract_start_time, extract_end_time, spark, db_name)
            export_powerbi_parquet(tenant, spark.sql(f"""select * from {db_name}.fact_conversation_survey"""), "gFactConversationSurveys")
    except Exception as e:
        logger.exception(f"Error Occured in GPC Survey Extraction for {extract_start_time}_{extract_end_time}_{tenant}_{api_name}")
        logger.exception(e, stack_info=True, exc_info=True)
        raise Exception
    finally:
        flush_utils(spark, logger)
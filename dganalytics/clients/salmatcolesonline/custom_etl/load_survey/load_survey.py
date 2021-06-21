import requests as rq
import pandas as pd
import json
import os
from pyspark.sql.functions import lit
from datetime import datetime
from pathlib import Path
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, DataFrame
from dganalytics.utils.utils import get_spark_session, flush_utils, get_path_vars
from dganalytics.connectors.gpc.gpc_utils import get_interval, get_api_url, get_dbname, authorize, gpc_request, extract_parser, gpc_utils_logger
from dganalytics.connectors.gpc.gpc_setup import create_raw_table

def fact_conversation_survey(spark: SparkSession, db_name: str):
    
    select_query = f"""
                        SELECT  conversationId,
                                startTime AS conversationStart,
                                endTime AS conversationEnd,
                                explode(participants.attribute) attributes
                        (SELECT  id AS conversationId,
                                startTime AS conversationStart,
                                endTime AS conversationEnd,
                                explode(participants) as participants,
                                extractDate as soucePartition
                        FROM {db_name}/raw_conversation_export)
                        WHERE endTime BETWEEN (current_date() - 3) AND current_date()
    """
    
    return True

if __name__ == '__main__':
    #tenant, run_id, extract_start_time, extract_end_time, api_name = extract_parser()
    
    tenant = 'salmatcolesonline'
    extract_start_time = '2021-06-04T00:00:00.000'
    extract_end_time = '2021-06-05T00:00:00.000'
    api_name = "conversation_extract"

    db_name = get_dbname(tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    app_name = "gpc_extract_" + api_name
    spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=db_name)

    global logger
    logger = gpc_utils_logger(tenant, app_name)

    try:
        # Authorize
        api_headers = authorize(tenant)
        
        conversations = spark.sql(f"""
            SELECT DISTINCT conversationId
            FROM gpc_salmatcolesonline.dim_conversations
            WHERE   conversationEnd IS NOT NULL
                    AND conversationEnd BETWEEN (current_date() - 3) AND current_date()
                    LIMIT 10
        """)

        # Raw Table
        create_raw_table("conversation_export", spark, db_name)

        if conversations != None and not conversations.rdd.isEmpty():
            for conversation in conversations.rdd.collect():

                # Conversation export API
                conv_export_resp = rq.get(f"{get_api_url(tenant)}/api/v2/conversationexport?ids={conversation['conversationId']}",
                                 headers=api_headers)

                convs = conv_export_resp.json()

                #Transform 
                if convs != None and len(convs) > 0:
                    #df = sc.wholeTextFiles(convs).map(lambda x:ast.literal_eval(x[1]))\
                         #   .map(lambda x: json.dumps(x))
                    #conversation_export = spark.read.json(sc.parallelize(convs).map(lambda x: json.dumps(x)))
                    #print(conversation_export.columns)
                    #if("address" not in conversation_export.columns):
                     #   conversation_export = conversation_export.withColumn("address", lit(None))
                    
                    #now = datetime.now()
                    #conversation_export = conversation_export.withColumn("extractDate", lit(str(now)[:10]))
                    #conversation_export.registerTempTable("conversation_export")
                    #spark.sql(f"""MERGE INTO {db_name}.raw_conversation_export
                    #                    using conversation_export 
                    #                    on conversation_export.id = raw_conversation_export.id
                    #                   WHEN MATCHED THEN
                    #                        UPDATE SET *
                    #                    WHEN NOT MATCHED THEN
                    #                        INSERT *
                    #          """)
                        
                    for conv in convs:
                        if conv != None and conv["participants"] != None and len(conv["participants"]) > 0:
                            for participant in conv["participants"]:
                                csat = ""
                                nps = ""
                                fcr = ""
                                if participant != None and participant["attributes"] != None:
                                    if "Survey CSAT Agent" in participant["attributes"]:
                                        print("conversation " + conversation['conversationId'])
                                        csat = participant["attributes"]["Survey CSAT Agent"]
                                        print("csat " + csat)
                                    if "Survey NPS" in participant["attributes"]:
                                        nps = participant["attributes"]["Survey NPS"]
                                        print("conversation " + conversation['conversationId'])
                                        print("nps " + nps)
                                    if "Survey Resolution" in participant["attributes"]:
                                        fcr = participant["attributes"]["Survey Resolution"]
                                        print("conversation " + conversation['conversationId'])
                                        print("fcr " + fcr)
                                    
                                    #TODO: Save Dims in the DB
                                    #TODO: Save facts in the DB

                
                # Create Dimensions and Facts

    except Exception as e:
        logger.exception(f"Error Occured in GPC Survey Extraction for {extract_start_time}_{extract_end_time}_{tenant}_{api_name}")
        logger.exception(e, stack_info=True, exc_info=True)
        raise Exception
    finally:
        flush_utils(spark, logger)
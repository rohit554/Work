from pyspark.sql.functions import explode
from dganalytics.connectors.gpc.gpc_api_config import gpc_end_points
from dganalytics.connectors.gpc.gpc_utils import process_raw_data, gpc_utils_logger, get_schema
from dganalytics.connectors.gpc.gpc_utils import get_spark_partitions_num
import json
from pyspark.sql.functions import to_json, collect_list, from_json, to_json, explode
from dganalytics.utils.utils import env, get_path_vars, get_logger, delta_table_partition_ovrewrite, delta_table_ovrewrite
from pyspark.sql.types import StructType, StructField
from pyspark.sql.utils import AnalysisException
import os, shutil
from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import get_dbname, extract_parser

def get_entity_name(api_name):
    current_api = gpc_end_points[api_name]
    return current_api['entity_name']

def delete_files_from_dbfs(files):
    for file in files:
        os.remove(file)
        logger.info(f"Deleted file: {file}")

def datalake_to_sql(tenant, api_name, run_id, extract_start_time, extract_end_time):

    logger = gpc_utils_logger(tenant, api_name)
    
    try:
        #Step 1 : Read the data
        logger.info(f"API being processed : {api_name}")
        tenant_path, db_path, log_path = get_path_vars(tenant)

        tenant_folder_path = os.path.join(tenant_path,"data", "raw", api_name)
        # files = [os.path.join(tenant_folder_path, file_name) for file_name in os.listdir(tenant_folder_path)]
        files = [os.path.join(tenant_folder_path, file_name) for file_name in os.listdir(tenant_folder_path) if file_name.endswith('.json')]

        if len(files) == 0: 
            logger.info("No files in the folder.")
            return

        logger.info(tenant_folder_path)

        data = spark.read.option("mode", "FAILFAST").option("multiline", "true").json(tenant_folder_path.replace("/dbfs","dbfs:"))
        data.repartition(500)
        
        #Step 2 : Transfrom the data into a required format
        try:
            if api_name in ["users_details_job", "management_unit_users"]:
                raise KeyError
            logger.info(f"Entity to be exploded : {entity_name}")
            df = data.select(explode(data[entity_name]).alias("data_column")) 
            json_df = df.select(to_json("data_column").alias("data_column_json"))
            data_list = json_df.select("data_column_json").rdd.flatMap(lambda x: x).collect() # naming convention
        except KeyError:
            logger.info("Entity name absent.")
            json_list = data.toJSON()
            logger.info("No json data found. No need to explode.")
            data_list = json_list.collect()

        if len(data_list) == 0:
            logger.info("No data in the files.")
            return

        #Step 3 : Insert the data in a SQL Table
        process_raw_data(spark, "colgate", api_name, run_id, data_list, extract_start_time, extract_end_time, len(data_list))

        #Step 4 : Delete the files from datalake
        delete_files_from_dbfs(files)
        logger.info("-"*30 + "\n\n")
    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)


if __name__ == "__main__":
    tenant, run_id, extract_start_time, extract_end_time, api_name = extract_parser()
    db_name = get_dbname(tenant)
    app_name = "gpc_extract_" + api_name
    spark = get_spark_session(
        app_name=app_name, tenant=tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)
    datalake_to_sql(tenant, api_name, run_id, extract_start_time, extract_end_time)
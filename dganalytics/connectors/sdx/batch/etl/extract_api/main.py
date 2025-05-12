from dganalytics.utils.utils import get_spark_session, flush_utils
from dganalytics.connectors.sdx.sdx_utils import get_dbname, sdx_request, extract_parser, sdx_utils_logger
from dganalytics.connectors.sdx.batch.etl.extract_api.interactions import exec_interactions
from dganalytics.connectors.sdx.batch.etl.extract_api.interactions_history import exec_interactions_history

from pyspark.sql import SparkSession
from dganalytics.connectors.sdx.sdx_utils import process_raw_data
import os, shutil
from dganalytics.utils.utils import get_path_vars
from dganalytics.connectors.sdx.sdx_api_config import sdx_end_points
from pyspark.sql.functions import to_json, collect_list, from_json, explode

def get_entity_name(api_name):
    current_api = sdx_end_points[api_name]
    if 'entity_name' in current_api:
        return current_api['entity_name']
    return ''

def delete_files_from_dbfs(tenant: str, tenant_folder_path):
    logger = sdx_utils_logger(tenant,"sdx_delete_files")
    for filename in os.listdir(tenant_folder_path):
        file_path = os.path.join(tenant_folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
                logger.info(f"Unlinking file : {file_path}")
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
                logger.info(f"Removing folder : {file_path}")
        except Exception as e:
            logger.error(f'Failed to delete {file_path}. Reason: {e}')

def extract_sdx_files_from_blob(spark, tenant, api_name, run_id, extract_start_time, extract_end_time):
    try:
        logger.info(f"API being processed : {api_name}")
        tenant_path, db_path, log_path = get_path_vars(tenant)

        entity_name = get_entity_name('interactions')
        tenant_folder_path = os.path.join(tenant_path,"data", "raw", api_name)
        
        #Step 1 : Read the data
        files = [os.path.join(tenant_folder_path, file_name) for file_name in os.listdir(tenant_folder_path) if file_name.endswith('.json')]

        if len(files) == 0: 
            logger.info("No files in the folder.")
            return

        logger.info(tenant_folder_path)

        data = spark.read.option("mode", "FAILFAST").option("multiline", "true").json(tenant_folder_path.replace("/dbfs","dbfs:"))
        data.repartition(500)
        
        #Step 2 : Transfrom the data into a required format
        try:
            entity_name = get_entity_name(api_name)
            if entity_name == '':
                raise Exception
            logger.info(f"Entity to be exploded : {entity_name}")
            df = data.select(explode(data[entity_name]).alias("data_column")) 
            json_df = df.select(to_json("data_column").alias("data_column_json"))
            data_list = json_df.select("data_column_json").rdd.flatMap(lambda x: x).collect() # naming convention
        except Exception as e:
            logger.info("Entity name absent.")
            json_list = data.toJSON()
            logger.info("No json data found. No need to explode.")
            data_list = json_list.collect()
        
        if len(data_list) == 0:
            logger.info("No data in the files.")
            return
        
        #Step 3 : Insert the data in a SQL Table
        # process_raw_data(spark, tenant, api_name, run_id, data_list, extract_start_time, extract_end_time, 1)
        #Step 3 : Insert the data in a SQL Table
        try:
            logger.info("Data being transferred to process_raw_data:")
            for data in data_list[:10]:  # Log only the first 10 items to avoid overwhelming the logs
                logger.info(data)
            process_raw_data(spark, tenant, api_name, run_id, data_list, extract_start_time, extract_end_time, 1)
        except Exception as e:
            logger.exception("Error while logging data_list", exc_info=True)


        #Step 4 : Delete the files from datalake
        delete_files_from_dbfs(tenant, tenant_folder_path)

    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise e


if __name__ == "__main__":
    tenant, run_id, extract_start_time, extract_end_time, api_name = extract_parser()
    db_name = get_dbname(tenant)
    app_name = "sdx_extract_" + api_name
    spark = get_spark_session(
        app_name=app_name, tenant=tenant, default_db=db_name)
    logger = sdx_utils_logger(tenant, app_name)

    try:
        logger.info(f"Extracting Survey Dynamix API {api_name}")

        if api_name in ["interactions","questions", "surveys"]:
            extract_sdx_files_from_blob(spark, tenant, api_name, run_id, extract_start_time, extract_end_time)
        elif api_name in ["interactions_history"]:
            df = exec_interactions_history(spark, tenant, run_id, extract_start_time, extract_end_time)
        else:
            logger.exception("invalid api name")
            raise Exception


    except Exception as e:
        logger.exception(
            f"Error Occured in Survey Dynamix Extraction for {extract_start_time}_{extract_end_time}_{tenant}_{api_name}")
        logger.exception(e, stack_info=True, exc_info=True)
        raise
    finally:
        flush_utils(spark, logger)

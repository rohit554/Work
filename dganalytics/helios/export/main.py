from dganalytics.utils.utils import get_spark_session, flush_utils
from dganalytics.helios.helios_utils import export_parser, helios_utils_logger
from dganalytics.helios.export.helios_export import helios_export
from dganalytics.helios.export.helios_process_map import helios_process_map
from dganalytics.helios.export.ivr_export import ivr_export
from dganalytics.helios.export.hf_helios_export import hf_helios_export
from dganalytics.helios.export.conversation_map_export import conversation_map_export
from dganalytics.helios.export.hf_value_stream_export import hf_value_stream_export

if __name__ == "__main__":
    tenant, run_id, extract_name, output_file_name = export_parser()
    spark = get_spark_session(
        app_name=extract_name, tenant=tenant, default_db=f"dgdm_{tenant}")

    logger = helios_utils_logger(tenant, extract_name)
    try:
        logger.info(f"Exporting helios transformation")
        if extract_name == 'helios_process_map_export':    
            logger.info(f"Exporting helios transformation {extract_name}")        
            helios_export(spark, tenant, extract_name, output_file_name)  
        elif extract_name == 'hf_helios_export':    
            logger.info(f"Exporting helios transformation {extract_name}")        
            hf_helios_export(spark, tenant, extract_name, output_file_name)  
        elif  extract_name == 'load_helios_process_map_into_table':
            logger.info(f"Exporting helios transformation {extract_name}")       
            helios_process_map(spark,tenant)
        elif extract_name == 'ivr_export':
            logger.info(f"Exporting helios transformation {extract_name}")       
        elif extract_name == 'hf_conversation_map_export':
            logger.info(f"Exporting helios transformation {extract_name}")       
            conversation_map_export(spark, tenant, extract_name, output_file_name)
        elif extract_name == 'hf_value_stream_export':
            logger.info(f"Exporting helios transformation {extract_name}")       
            hf_value_stream_export(spark, tenant, extract_name, output_file_name)
        else:
            logger.exception("invalid export name")
            raise Exception
        
    except Exception as e:
        logger.exception(f"Error Occured in helios data export for {tenant}_{extract_name}")
        logger.exception(e, stack_info=True, exc_info=True)
        raise Exception
    finally:
        flush_utils(spark, logger)
    
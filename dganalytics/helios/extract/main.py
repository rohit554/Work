from dganalytics.utils.utils import get_spark_session, flush_utils
from dganalytics.helios.helios_utils import helios_utils_logger
from dganalytics.helios.extract.transcript_insights import get_transcript_insights
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--run_id', required=True)
    parser.add_argument('--extract_start_time', required=True)
    parser.add_argument('--extract_end_time', required=True)
    parser.add_argument('--api_name', required=True)

    args, unknown_args = parser.parse_known_args()

    tenant = args.tenant
    run_id = args.run_id
    extract_start_time = args.extract_start_time
    extract_end_time = args.extract_end_time
    api_name = args.api_name

    db_name = f"gpc_{tenant}"
    spark = get_spark_session(app_name=api_name, tenant=tenant, default_db=db_name)
    logger = helios_utils_logger(tenant, api_name)

    try:
        logger.info(f"Extracting {api_name}")
        if api_name == "transcript_insights":
            get_transcript_insights(spark, tenant, run_id, extract_start_time, extract_end_time)
        else:
            logger.exception("invalid api name")
            raise Exception
    except Exception as e:
        logger.exception(
            f"Error Occurred in Helios Extraction for {extract_start_time}_{extract_end_time}_{tenant}_{api_name}")
        logger.exception(e, stack_info=True, exc_info=True)
        raise Exception
    finally:
        flush_utils(spark, logger)
from dganalytics.utils.utils import get_spark_session, flush_utils
from dganalytics.helios.helios_utils import helios_utils_logger
from dganalytics.helios.extract.import_transcript_insights import process_files
from dganalytics.helios.extract.import_classified_labels import (
    process_classification_files,
)
import argparse
from dganalytics.helios.extract.import_classification_score import (
    process_classification_score_files,
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--api_name", required=True)
    parser.add_argument("--containerName", required=True)

    args, unknown_args = parser.parse_known_args()

    tenant = args.tenant
    run_id = args.run_id
    api_name = args.api_name
    containerName = args.containerName

    db_name = f"gpc_{tenant}"
    spark = get_spark_session(app_name=api_name, tenant=tenant, default_db=db_name)
    logger = helios_utils_logger(tenant, api_name)

    try:
        logger.info(f"Extracting {api_name}")
        if api_name in [
            "insights",
            "quality",
            "compliance",
            "conformance",
            "conversation_map",
            "value_stream",
            "apprehension",
			"objection",
			"summary"
        ]:
            process_files(spark, run_id, containerName, tenant, api_name, logger)
        elif api_name in ["classification"]:
            process_classification_files(
                spark, run_id, containerName, tenant, api_name, logger
            )
        elif api_name in ["classification_score"]:
            process_classification_score_files(
                spark, run_id, containerName, tenant, api_name, logger
            )
        else:
            logger.exception("invalid api name")
            raise Exception
    except Exception as e:
        logger.exception(f"Error Occurred in Helios Extraction for {tenant}_{api_name}")
        logger.exception(e, stack_info=True, exc_info=True)
        raise Exception
    finally:
        flush_utils(spark, logger)
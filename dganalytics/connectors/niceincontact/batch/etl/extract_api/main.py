from dganalytics.connectors.niceincontact.batch.etl.extract_api.contacts_contactId_email_transcript import get_master_contact_id_for_email_transcript
from dganalytics.connectors.niceincontact.batch.etl.extract_api.media_playback_chat_email_segment import fetch_media_segments
from dganalytics.connectors.niceincontact.batch.etl.extract_api.media_playback_contact import get_master_contact_id
from dganalytics.connectors.niceincontact.batch.etl.extract_api.segments_analyzed_transcript import analytics_api_call
from dganalytics.utils.utils import get_spark_session, flush_utils
from dganalytics.connectors.niceincontact.niceincontact_utils import (get_dbname, niceincontact_request, extract_parser, niceincontact_utils_logger )


if __name__ == "__main__":
    # Main function to execute the Nice In Contact API extraction.
    # This function initializes the Spark session, sets up logging, and calls the appropriate extraction functions
    # based on the API name provided in the command line arguments.
    tenant, run_id, extract_start_time, extract_end_time, api_name = extract_parser()
    db_name = get_dbname(tenant)
    app_name = "niceincontact_extract_" + api_name
    spark = get_spark_session(
        app_name=app_name, tenant=tenant, default_db=db_name)
    logger = niceincontact_utils_logger(tenant, app_name)

    try:
        logger.info("Starting extraction for Nice In Contact API's: %s", api_name)

        if api_name in ["agents", "teams", "teams_agents", "teams_performance",
                         "agents_performance", "agents_inetraction_history",
                         "agents_skills", "skills", "dispositions","dispositions_skills",
                         "skills_summary", "skills_sla_summary",
                         "contacts", "contacts_custom_data", "contacts_completed", "segments_analyzed", "wfm_data_agents",
                        "wfm_data_agents_schedule_adherence", "wfm_data_agents_scorecards", "wfm_data_agents_schedule_adherence",
                        "wfm_data_skills_contacts"]:
            niceincontact_request(spark, tenant, api_name, run_id,
                             extract_start_time, extract_end_time)
        elif api_name == "media_playback_chat_email_segment":
            fetch_media_segments(spark, tenant, api_name, run_id, extract_start_time, extract_end_time, logger)
        elif api_name == "media_playback_contact":
            get_master_contact_id(spark, tenant, api_name, run_id,
                             extract_start_time, extract_end_time, logger)
        elif api_name == "contacts_contactId_email_transcript":
            get_master_contact_id_for_email_transcript(spark, tenant, api_name, run_id, extract_start_time, extract_end_time, logger)
        elif api_name == "segments_analyzed_transcript":
            analytics_api_call(spark, tenant, api_name, run_id, extract_start_time, extract_end_time, logger)
        else:
            logger.error("Invalid API name provided: %s. No matching handler function.", api_name)
            raise ValueError("Invalid API name provided: %s. No matching handler function." % api_name)

    except Exception as e:
        logger.exception(
            "Error occurred during extraction: tenant=%s, api=%s, start_time=%s, end_time=%s, run_id=%s",
            tenant, api_name, extract_start_time, extract_end_time, run_id
        )
        logger.exception("Detailed Exception: %s", str(e), stack_info=True, exc_info=True)
        raise
    finally:
        flush_utils(spark, logger)

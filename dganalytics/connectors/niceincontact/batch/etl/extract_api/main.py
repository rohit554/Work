from dganalytics.utils.utils import get_spark_session, flush_utils
from dganalytics.connectors.niceincontact.niceincontact_utils import (get_dbname, niceincontact_request, extract_parser, niceincontact_utils_logger, 
                                                                      fetch_media_playback_data, generate_daily_date_ranges, fetch_media_segments,
                                                                      fetch_contacts_email_transcript, analytics_api_call, 
                                                                       generate_wfmdata_agents )


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

        if api_name in ["agents", "teams", "teams_agents", "agents_skills", "skills", "dispositions","dispositions_skills"]:
            niceincontact_request(spark, tenant, api_name, run_id,
                             extract_start_time, extract_end_time)
        elif api_name == "media_playback_chat_email_segment":
            fetch_media_segments(spark, tenant, api_name, run_id, extract_start_time, extract_end_time, skip_raw_load=True, base_url=True)
        elif api_name in ["contacts_custom_data", "contacts_completed", "contacts"]:
            generate_daily_date_ranges(spark, tenant, api_name, run_id,
                             extract_start_time, extract_end_time)
        elif api_name in ["segments_analyzed"]:
            df = niceincontact_request(spark, tenant, api_name, run_id,
                             extract_start_time, extract_end_time, base_url=True)
        elif api_name == "media_playback_contact":
            fetch_media_playback_data(spark, tenant, api_name, run_id,
                             extract_start_time, extract_end_time)
        elif api_name == "contacts_contactId_email_transcript":
            fetch_contacts_email_transcript(spark, tenant, api_name, run_id, extract_start_time, extract_end_time)
        elif api_name == "segments_analyzed_transcript":
            analytics_api_call(spark, tenant, api_name, run_id, extract_start_time, extract_end_time)
        elif api_name == "wfm_data_agents":
            generate_wfmdata_agents(spark, tenant, api_name, run_id, extract_start_time, extract_end_time)
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

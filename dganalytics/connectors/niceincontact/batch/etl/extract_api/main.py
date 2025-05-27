from dganalytics.utils.utils import get_spark_session, flush_utils
from dganalytics.connectors.niceincontact.niceincontact_utils import (get_dbname, niceincontact_request, extract_parser, niceincontact_utils_logger, 
                                                                      fetch_media_playback_data, generate_daily_date_ranges, fetch_media_segments,
                                                                      fetch_contacts_email_transcript)


if __name__ == "__main__":
    tenant, run_id, extract_start_time, extract_end_time, api_name = extract_parser()
    db_name = get_dbname(tenant)
    app_name = "niceincontact_extract_" + api_name
    spark = get_spark_session(
        app_name=app_name, tenant=tenant, default_db=db_name)
    logger = niceincontact_utils_logger(tenant, app_name)

    try:
        logger.info(f"Extracting Nice In Contact API {api_name}")

        if api_name in ["agents", "teams", "teams_agents", "agents_skills", "skills", "dispositions","dispositions_skills"]:
            df = niceincontact_request(spark, tenant, api_name, run_id,
                             extract_start_time, extract_end_time)
        elif api_name in ["interaction_analytics_gateway_v2_segments_analyzed"]:
            df = niceincontact_request(spark, tenant, api_name, run_id,
                             extract_start_time, extract_end_time, base_url=True)
        elif api_name == "media_playback_v1_segments_segmentId":
            fetch_media_segments(spark, tenant, api_name,None, extract_start_time, extract_end_time, skip_raw_load=True, base_url=True)
        elif api_name in ["contacts_custom_data", "contacts_completed", "contacts"]:
            generate_daily_date_ranges(spark, tenant, api_name, run_id,
                             extract_start_time, extract_end_time,)
        elif api_name in ["contacts", "interaction_analytics_gateway_v2_segments_analyzed"]:
            df = niceincontact_request(spark, tenant, api_name, run_id,
                             extract_start_time, extract_end_time, base_url=True)
        elif api_name == "media_playback_v1_contacts_acdContactId":
            fetch_media_playback_data(spark, tenant, api_name, run_id,
                             extract_start_time, extract_end_time)
        elif api_name == "contacts_contactId_email_transcript":
            fetch_contacts_email_transcript(spark, tenant, api_name, run_id, extract_start_time, extract_end_time)
        else:
            logger.exception("Invalid API name")
            raise Exception

    except Exception as e:
        logger.exception(
            f"Error Occured in Nice In Contact Extraction for {extract_start_time}_{extract_end_time}_{tenant}_{api_name}")
        logger.exception(e, stack_info=True, exc_info=True)
        raise Exception
    finally:
        flush_utils(spark, logger)

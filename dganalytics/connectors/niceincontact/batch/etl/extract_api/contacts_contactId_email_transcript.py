import json
import logging
from pyspark.sql import SparkSession
from dganalytics.connectors.niceincontact.niceincontact_api_config import niceincontact_end_points
from dganalytics.connectors.niceincontact.niceincontact_utils import (
    get_api_url, make_niceincontact_request, refresh_access_token, 
    authorize, process_raw_data, niceincontact_utils_logger
)
from dganalytics.connectors.niceincontact.batch.etl.extract_api.media_playback_contact import fetch_contacts


def extract_email_transcript(master_contact_id: str, auth_headers: dict, tenant: str,
                             niceincontact: dict, api_name: str) -> dict:
    """
    Fetch email transcript data for a given contact from NICE inContact API.

    Args:
        master_contact_id (str): The unique identifier of the contact.
        auth_headers (dict): Authorization headers for the API request.
        tenant (str): The tenant identifier used for logging and token refresh.
        niceincontact (dict): Dictionary of NICE inContact API configurations.
        api_name (str): Name of the specific API configuration to use.
        logger (logging.Logger): Logger instance for status and error logging.

    Returns:
        dict: Email transcript data if successful; otherwise, an empty dictionary.
    """
    logger = niceincontact_utils_logger(tenant, "niceincontact_extract_"+str(api_name))
    email_transcript_url = get_api_url(tenant)
    config = niceincontact[api_name]
    params = config.get('params', {})
    params.update({"acd-call-id": master_contact_id})

    req_type = config.get('request_type', 'GET')

    logger.info(f"Requesting email transcript for master_contact_id: {master_contact_id}")
    resp = make_niceincontact_request(req_type, email_transcript_url, params, auth_headers)

    if resp.status_code == 401:
        logger.warning(f"401 Unauthorized for email transcript. Refreshing token for tenant: {tenant}")
        auth_headers = refresh_access_token(tenant)
        resp = make_niceincontact_request(req_type, email_transcript_url, params, auth_headers)

    if resp.status_code == 200:
        logger.info(f"Successfully fetched email transcript for master_contact_id: {master_contact_id}")
        return resp.json()
    else:
        logger.error(
            f"Failed to fetch email transcript for master_contact_id: {master_contact_id}. "
            f"Status: {resp.status_code}, Response: {resp.text}"
        )
        return {}


def get_master_contact_id_for_email_transcript(
        spark: SparkSession,
        tenant: str,
        api_name: str,
        run_id: str,
        extract_start_time: str,
        extract_end_time: str) -> None:
    """
    Retrieve and process email transcript data for all master contact IDs within a date range.

    Args:
        spark (SparkSession): Active Spark session used for downstream processing.
        tenant (str): Tenant identifier.
        api_name (str): API configuration name used to fetch email transcript data.
        run_id (str): Unique identifier for this ETL run.
        extract_start_time (str): Start datetime for extracting data (ISO 8601 format).
        extract_end_time (str): End datetime for extracting data (ISO 8601 format).
        logger (logging.Logger): Logger instance for capturing process status and diagnostics.

    Returns:
        None
    """
    logger = niceincontact_utils_logger(tenant, "niceincontact_extract_" + str(api_name))
    logger.info(f"Starting get_master_contact_id_for_email_transcript for tenant: {tenant}, run_id: {run_id}")
    auth_headers = authorize(tenant)
    niceincontact = niceincontact_end_points

    logger.info(f"Fetching contact IDs between {extract_start_time} and {extract_end_time}")
    contact_ids = fetch_contacts(spark, extract_start_time, extract_end_time, logger)
    contact_ids = list(set(contact_ids))
    logger.info(f"Total unique contact IDs fetched: {len(contact_ids)}")

    email_transcript_data_list = []
    for count, contact_id in enumerate(contact_ids, start=1):
        logger.info(f"[{count}/{len(contact_ids)}] Fetching transcript for contact_id: {contact_id}")
        email_transcript_data = extract_email_transcript(contact_id, auth_headers, tenant, niceincontact, api_name)

        if email_transcript_data:
            email_transcript_data_list.append(json.dumps(email_transcript_data))
            logger.info(f"Email transcript data added for contact_id: {contact_id}")
        else:
            logger.warning(f"No email transcript data found for contact_id: {contact_id}")

        logger.info(f"Completed processing for contact_id: {contact_id}")

    logger.info("Initiating process_raw_data for all collected email transcripts.")
    process_raw_data(
        spark, tenant, api_name, run_id,
        email_transcript_data_list,
        extract_start_time, extract_end_time, 1
    )
    logger.info("Finished processing all email transcript data.")

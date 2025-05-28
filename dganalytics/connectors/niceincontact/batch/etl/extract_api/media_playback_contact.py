import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dganalytics.connectors.niceincontact.niceincontact_api_config import niceincontact_end_points
from dganalytics.connectors.niceincontact.niceincontact_utils import (
    get_api_url, make_niceincontact_request, refresh_access_token, 
     authorize, process_raw_data
)
import logging
from typing import List

def extract_media_playback(master_contact_id: str, auth_headers: dict, tenant: str,
                           niceincontact: dict, api_name: str, logger: logging.Logger) -> dict:
    """
    Fetch media playback data for a given contact from NICE inContact API.

    Args:
        master_contact_id (str): The unique identifier of the contact.
        auth_headers (dict): Authorization headers for the API request.
        tenant (str): The tenant identifier used for logging and token refresh.
        niceincontact (dict): Dictionary of NICE inContact API configurations.
        api_name (str): Name of the specific API configuration to use.
        logger (logging.Logger): Logger instance for logging status and errors.

    Returns:
        dict: Media playback data if successful; empty dict otherwise.
    """
    media_playback_url = get_api_url(tenant)
    config = niceincontact[api_name]
    params = config.get('params', {})
    params.update({"acd-call-id": master_contact_id})
    req_type = config.get('request_type', 'GET')

    logger.info(f"Requesting media playback for master_contact_id: {master_contact_id}")
    resp = make_niceincontact_request(req_type, media_playback_url, params, auth_headers)

    if resp.status_code == 401:
        logger.warning(f"401 Unauthorized for media playback. Refreshing token for tenant: {tenant}")
        auth_headers = refresh_access_token(tenant)
        resp = make_niceincontact_request(req_type, media_playback_url, params, auth_headers)

    if resp.status_code == 200:
        logger.info(f"Successfully fetched media playback for master_contact_id: {master_contact_id}")
        return resp.json()
    else:
        logger.error(f"Failed to fetch media playback for master_contact_id: {master_contact_id}. "
                     f"Status: {resp.status_code}, Response: {resp.text}")
        return {}


def fetch_contacts(
    spark: SparkSession,
    start_date: str,
    end_date: str,
    logger: logging.Logger
) -> List[str]:
    """
    Fetch masterContactId list from niceincontact_infobell.raw_contacts 
    filtered by agentStartDate between start_date and end_date.

    Args:
        spark (SparkSession): Spark session object.
        start_date (str): Start date in ISO 8601 format (e.g. '2024-01-01T00:00:00Z').
        end_date (str): End date in ISO 8601 format (e.g. '2024-01-31T23:59:59Z').
        logger (logging.Logger): Logger instance for logging progress and errors.

    Returns:
        List[str]: List of unique masterContactId values within the given date range.
    """
    try:
        logger.info(f"Loading table niceincontact_infobell.raw_contacts")
        df = spark.table("niceincontact_infobell.raw_contacts")

        logger.info(f"Filtering agentStartDate between {start_date} and {end_date}")
        filtered_df = df.filter(
            (col("agentStartDate") >= start_date) & (col("agentStartDate") <= end_date)
        ).select("masterContactId").dropna().dropDuplicates()

        contact_ids = [row["masterContactId"] for row in filtered_df.collect()]
        logger.info(f"Fetched {len(contact_ids)} unique masterContactIds")

        return contact_ids

    except Exception as e:
        logger.error(f"Failed to fetch contacts: {e}", exc_info=True)
        return []



def get_master_contact_id(spark: SparkSession, tenant: str, api_name: str, run_id: str,
                          extract_start_time: str, extract_end_time: str, logger: logging.Logger) -> None:
    """
    Retrieve media playback data for all master contact IDs within a date range and process it.

    Args:
        spark (SparkSession): Active Spark session for data processing.
        tenant (str): Tenant identifier for API and data routing.
        api_name (str): API configuration name for media playback.
        run_id (str): Identifier for the current pipeline run.
        extract_start_time (str): Start datetime (ISO 8601 format) for data extraction.
        extract_end_time (str): End datetime (ISO 8601 format) for data extraction.
        logger (logging.Logger): Logger instance for tracking progress.

    Returns:
        None
    """
    logger.info(f"Starting get_master_contact_id for tenant: {tenant} with run_id: {run_id}")
    auth_headers = authorize(tenant)
    niceincontact = niceincontact_end_points

    contact_ids = fetch_contacts(spark, extract_start_time, extract_end_time, logger)
    contact_ids = list(set(contact_ids))  # Remove duplicates
    logger.info(f"Total unique contact IDs to process: {len(contact_ids)}")

    media_playback_data_list = []
    for count, contact_id in enumerate(contact_ids, start=1):
        logger.info(f"[{count}/{len(contact_ids)}] Processing media playback for contact_id: {contact_id}")
        media_playback_data = extract_media_playback(contact_id, auth_headers, tenant, niceincontact, api_name, logger)

        if media_playback_data:
            media_playback_data_list.append(json.dumps(media_playback_data))
            logger.info(f"Appended media playback data for contact_id: {contact_id}")
        else:
            logger.warning(f"No media playback data found for contact_id: {contact_id}")

    logger.info("Completed fetching all media playback data. Starting processing via process_raw_data")
    process_raw_data(spark, tenant, api_name, run_id,
                     media_playback_data_list, extract_start_time, extract_end_time, 1)
    logger.info("Finished processing media playback data.")

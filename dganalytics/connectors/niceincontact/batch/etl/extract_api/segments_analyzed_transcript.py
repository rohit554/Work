import json
import logging
from pyspark.sql import SparkSession
from dganalytics.connectors.niceincontact.niceincontact_api_config import niceincontact_end_points
from dganalytics.connectors.niceincontact.niceincontact_utils import (
    niceincontact_request, make_niceincontact_request, refresh_access_token, 
    authorize, process_raw_data, get_api_url
)


def fetch_analyzed_transcript_by_segment(
        tenant: str,
        api_name: str,
        segmentId: str,
        logger: logging.Logger) -> dict:
    """
    Fetch analyzed transcript data for a given segment ID from NICE inContact API.

    Args:
        tenant (str): Tenant identifier.
        api_name (str): API configuration name.
        segmentId (str): The segment ID to retrieve transcript data for.
        logger (logging.Logger): Logger instance.

    Returns:
        dict: Transcript data if available; None otherwise.
    """
    logger.info(f"Authorizing tenant: {tenant} for API: {api_name}")
    auth_headers = authorize(tenant)
    niceincontact = niceincontact_end_points
    config = niceincontact[api_name]

    req_type = config.get('request_type', 'GET')
    url = f"{get_api_url(tenant)}{config['endpoint']}".format(segmentId=segmentId)
    params = config.get('params', {})

    logger.info(f"Fetching transcript for segmentId: {segmentId} from URL: {url}")
    resp = make_niceincontact_request(req_type, url, params, auth_headers)

    if resp.status_code == 401:
        logger.warning(f"Received 401 Unauthorized for {api_name}. Refreshing token for tenant: {tenant}")
        auth_headers = refresh_access_token(tenant)
        resp = make_niceincontact_request(req_type, url, params, auth_headers)

    if resp.status_code == 200:
        logger.info(f"Successfully retrieved transcript for segmentId: {segmentId}")
        return resp.json()
    else:
        logger.error(
            f"Failed to retrieve transcript for segmentId: {segmentId}. "
            f"Status Code: {resp.status_code}, URL: {url}, Response: {resp.text}"
        )
        return None


def analytics_api_call(
        spark: SparkSession,
        tenant: str,
        api_name: str,
        run_id: str,
        start_date: str,
        end_date: str,
        logger: logging.Logger) -> None:
    """
    Process transcript segments using NICE inContact Analytics API.

    Fetches segment IDs from the analytics summary endpoint and then
    queries transcript data for each segment. Results are processed using Spark.

    Args:
        spark (SparkSession): Active Spark session.
        tenant (str): Tenant identifier.
        api_name (str): API configuration name used to fetch transcript data.
        run_id (str): Unique identifier for this ETL run.
        start_date (str): Start date in ISO 8601 format.
        end_date (str): End date in ISO 8601 format.
        logger (logging.Logger): Logger instance.

    Returns:
        None
    """
    logger.info(f"Starting analytics API call for tenant: {tenant}, run_id: {run_id}, "
                f"date range: {start_date} to {end_date}")

    resp_list = niceincontact_request(
        spark, tenant, "segments_analyzed", run_id,
        start_date, end_date, skip_raw_load=True
    )
    
    segmentId_list = [json.loads(res).get('segmentId') for res in resp_list if res]
    logger.info(f"Total segments fetched: {len(segmentId_list)}")

    transcript_list = []
    for count, segmentId in enumerate(segmentId_list, start=1):
        logger.info(f"[{count}/{len(segmentId_list)}] Fetching analyzed transcript for segmentId: {segmentId}")
        transcript = fetch_analyzed_transcript_by_segment(tenant, api_name, segmentId, logger)

        if transcript:
            transcript_list.append(json.dumps(transcript))
            logger.info(f"Transcript data added for segmentId: {segmentId}")
        else:
            logger.warning(f"No transcript data found for segmentId: {segmentId}")

    logger.info(f"Processing raw data for {len(transcript_list)} transcripts.")
    process_raw_data(
        spark, tenant, api_name, run_id,
        transcript_list, start_date, end_date, 1
    )
    logger.info("Finished analytics API call and data processing.")

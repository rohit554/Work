import json
import logging
from pyspark.sql import SparkSession
from dganalytics.connectors.niceincontact.niceincontact_api_config import niceincontact_end_points
from dganalytics.connectors.niceincontact.niceincontact_utils import (
    get_api_url, make_niceincontact_request, refresh_access_token, 
    niceincontact_request, authorize, process_raw_data, niceincontact_utils_logger
)

def fetch_media_playback_segments(tenant: str, api_name: str,
                                  extract_start_time: str, extract_end_time: str,
                                  segmentId: int, auth_headers: dict) -> dict | None:
    """
    Fetches media playback segments from the NICE inContact API for a given segment ID.

    Args:
        tenant (str): Tenant identifier for constructing the API base URL.
        api_name (str): Key to retrieve specific API configuration from `niceincontact_end_points`.
        extract_start_time (str): Extraction start time (used for interval-based APIs).
        extract_end_time (str): Extraction end time (used for interval-based APIs).
        segmentId (int): The ID of the media segment to fetch.
        auth_headers (dict): Dictionary containing authentication headers for the API request.
        logger (logging.Logger): Logger instance for logging information and errors.

    Returns:
        dict or None: Parsed JSON response from the API if successful; None if request fails.
    """
    logger = niceincontact_utils_logger(tenant, "niceincontact_extract_"+str(api_name))
    niceincontact = niceincontact_end_points
    config = niceincontact[api_name]
    req_type = config.get('request_type', 'GET')
    url = f"{get_api_url(tenant)}{config['endpoint']}".format(segmentId=segmentId)
    logger.info(f"ur : {url}")
    params = config.get('params', {})
    cursor = config.get('cursor', False)
    paging = config.get('paging', False)
    interval = config.get('interval', False)
    cursor_param = ""

    if interval:
        params['startDate'] = extract_start_time
        params['endDate'] = extract_end_time

    if paging:
        if req_type == "GET":
            params['skip'] = 1
            params['top'] = int(params['pageSize'])
        else:
            params.update({
                "skip": 1,
                "top": int(params['pageSize'])
            })

    if cursor and cursor_param != "":
        params['cursor'] = cursor_param

    resp = make_niceincontact_request(req_type, url, params, auth_headers)

    if resp.status_code == 401:
        logger.warning(f"Received 401 Unauthorized for {api_name}. Attempting token refresh for tenant: {tenant}")
        auth_headers = refresh_access_token(tenant)
        resp = make_niceincontact_request(req_type, url, params, auth_headers)

    if resp.status_code == 200:
        return resp.json()
    else:
        logger.error(f"Failed to fetch media playback segments for segmentId {segmentId}. "
                     f"Status Code: {resp.status_code}, Response: {resp.text}")
        return None


def fetch_media_segments(spark: SparkSession, tenant: str, api_name: str, run_id: str,
                         extract_start_time: str, extract_end_time: str) -> None:
    """
    Retrieves media segments for the given time range, categorizes them by media type,
    and processes the data using NICE inContact utilities.

    Args:
        spark (SparkSession): Spark session for processing data.
        tenant (str): Tenant identifier for API and data processing.
        api_name (str): API name key used to fetch playback segments.
        run_id (str): Unique identifier for the data extraction run.
        extract_start_time (str): Start time for media segment extraction.
        extract_end_time (str): End time for media segment extraction.
        logger (logging.Logger): Logger instance for logging status and errors.

    Returns:
        None
    """
    logger = niceincontact_utils_logger(tenant, "niceincontact_extract_"+str(api_name))
    resp_list = niceincontact_request(
        spark, tenant, "segments_analyzed", None,
        extract_start_time, extract_end_time, skip_raw_load=True
    )
    segmentId_list = [json.loads(res)['segmentId'] for res in resp_list]
    auth_headers = authorize(tenant)
    media_playback_segments_list = []
    voice_only_data_list = []
    count = 1

    for segmentId in segmentId_list:
        logger.info(f"Processing count: {count}, Remaining: {len(segmentId_list) - count}")
        media_playback_segments_data = fetch_media_playback_segments(
            tenant, api_name, extract_start_time, extract_end_time,
            segmentId, auth_headers)
        if media_playback_segments_data:
            for interactions in media_playback_segments_data.get("interactions", []):
                if interactions.get("mediaType", {}) != "voice-only":
                    media_playback_segments_list.append(json.dumps(media_playback_segments_data))
                else:
                    voice_only_data_list.append(json.dumps(media_playback_segments_data))
        count += 1

    apis = ["media_playback_chat_email_segment", "media_playback_voice_segment"]
    for api_ in apis:
        if api_ == "media_playback_chat_email_segment":
            media_playback_segments_list = voice_only_data_list
        process_raw_data(
            spark, tenant, api_, run_id,
            media_playback_segments_list,
            extract_start_time, extract_end_time, 1
        )

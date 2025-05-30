import json
import time
import requests
from pyspark.sql import SparkSession
from dganalytics.connectors.niceincontact.niceincontact_utils import authorize, get_api_url, make_niceincontact_request, process_raw_data, refresh_access_token
import logging
import re
from dganalytics.connectors.niceincontact.niceincontact_api_config import niceincontact_end_points


def create_job(tenant, api_name, extract_start_time, extract_end_time, logger: logging.Logger):
    """
    Creates a job in the NICE inContact API for data extraction.

    Args:
        tenant (str): Tenant identifier.
        api_name (str): API name to extract data from.
        extract_start_time (str): Extraction start time.
        extract_end_time (str): Extraction end time.
        logger (logging.Logger): Logger instance for logging.

    Returns:
        dict: JSON response from the API if successful, None otherwise.
    """
    niceincontact = niceincontact_end_points
    config = niceincontact[api_name]
    logger.info(
        f"[{tenant}] Nice InContact Request Start for {api_name} with extract_date {extract_start_time}_{extract_end_time}"
    )
    auth_headers = authorize(tenant)
    url = get_api_url(tenant) + config['endpoint']
    params = config.get('params', {})
    interval = config.get('interval', False)
    req_type = config.get('request_type', 'GET')
    if interval:
        params['startDate'] = extract_start_time
        params['endDate'] = extract_end_time

    logger.debug(f"[{tenant}] Sending {req_type} request to {url} with params: {params}")
    resp = make_niceincontact_request(req_type, url, params, auth_headers)

    if resp.status_code == 401:
        logger.warning(f"[{tenant}] Received 401 Unauthorized for {api_name}. Attempting token refresh for tenant: {tenant}")
        auth_headers = refresh_access_token(tenant)
        resp = make_niceincontact_request(req_type, url, params, auth_headers)

    if resp.status_code != 202:
        logger.error(f"[{tenant}] Error: {resp.status_code} - {resp.text}")
        return None

    logger.info(f"[{tenant}] Job creation successful for {api_name}. Status code: {resp.status_code}")
    resp_json = resp.json()
    logger.debug(f"[{tenant}] Job creation response: {resp_json}")
    return resp_json

def get_extraction_url(tenant, api_name, job_id, retries=0, max_retries=40, wait_seconds=15, logger: logging.Logger):
    """
    Retrieves the extraction URL for a given job ID from the NICE inContact API.
    """
    niceincontact = niceincontact_end_points
    config = niceincontact[api_name]
    auth_headers = authorize(tenant)
    url =  get_api_url(tenant) + config['endpoint'].format(jobId=job_id)
    params = config.get('params', {})
    req_type = config.get('request_type', 'GET')

    resp = make_niceincontact_request(req_type, url, params, auth_headers)

    if resp.status_code == 401:
        logger.warning(f"Received 401 Unauthorized for {api_name}. Refreshing token for tenant: {tenant}")
        auth_headers = refresh_access_token(tenant)
        resp = make_niceincontact_request(req_type, url, params, auth_headers)

    if resp.status_code != 200:
        logger.error(f"Error: {resp.status_code} - {resp.text}")
        return None

    resp_json = resp.json()
    logger.debug(f"Job status response for {job_id}: {resp_json}")

    job_status = resp_json.get("jobStatus", {})
    status = job_status.get("status")
    result = job_status.get("result", {})
    error_msg = result.get("messageError", "No error message provided.")
    extraction_url = result.get("url", "")

    if status == "SUCCEEDED":
        if extraction_url:
            logger.info(f"[{api_name}] Job {job_id} succeeded with extraction URL.")
            return extraction_url
        else:
            logger.error(f"[{api_name}] Job {job_id} SUCCEEDED but no extraction URL was returned.")
            return None

    elif status == "FAILED":
        logger.error(f"[{api_name}] Job {job_id} failed: {error_msg}")
        return None

    elif status == "RUNNING":
        if retries >= max_retries:
            logger.error(f"[{api_name}] Job {job_id} timed out after {max_retries * wait_seconds} seconds.")
            return None
        logger.info(f"[{api_name}] Job {job_id} still running. Retrying in {wait_seconds} seconds... (Attempt {retries + 1})")
        time.sleep(wait_seconds)
        return get_extraction_url(tenant, api_name, job_id, retries=retries+1, max_retries=max_retries, wait_seconds=wait_seconds)

    else:
        logger.warning(f"[{api_name}] Unexpected job status '{status}' for job {job_id}. Retrying in {wait_seconds} seconds...")
        time.sleep(wait_seconds)
        return get_extraction_url(tenant, api_name, job_id, retries=retries+1, max_retries=max_retries, wait_seconds=wait_seconds)

def load_csv_from_response(spark, url: str, tenant: str, logger: logging.Logger):
    """
    Fetch CSV content from an HTTP endpoint and load it into a Spark DataFrame with cleaned column names.

    Parameters:
    - spark: SparkSession object
    - url: URL to fetch CSV from
    - tenant: Tenant identifier used for logging and token handling

    Returns:
    - List of dictionaries representing the rows of the cleaned DataFrame

    Raises:
    - RuntimeError if the response is unauthorized or if the resulting DataFrame is empty
    """
    logger.info(f"[{tenant}] Fetching CSV from URL: {url}")
    
    # Step 1: Request the CSV data
    req_type = "GET"
    payload = {}
    headers = {}

    response = requests.request(req_type, url, headers=headers, data=payload)

    if response.status_code == 401:
        logger.warning(f"[{tenant}] Received 401 Unauthorized. Attempting token refresh.")

        response = requests.request(req_type, url, headers=headers, data=payload)

    if response.status_code != 200:
        msg = f"[{tenant}] Failed to fetch CSV data. Status: {response.status_code}, Response: {response.text}"
        logger.error(msg)
        raise RuntimeError(msg)

    # Step 2: Convert CSV text to RDD
    lines_rdd = spark.sparkContext.parallelize(response.text.strip().splitlines())

    # Step 3: Read CSV into DataFrame
    df = spark.read.option("header", True).csv(lines_rdd)

    # Step 4: Clean column names
    def clean_column(col):
        col = re.sub(r'[ ,;{}()\n\t=/-]+', '_', col.strip())  # Replace invalid chars with _
        col = re.sub(r'_+$', '', col)  # Remove trailing underscores
        return col

    cleaned_columns = [clean_column(c) for c in df.columns]
    df_cleaned = df.toDF(*cleaned_columns)
    df_cleaned.display()

    logger.info(f"[{tenant}] Successfully loaded and cleaned CSV data with {df_cleaned.count()} rows")

    # Step 5: Convert DataFrame to list of dicts
    json_list = [json.dumps(row.asDict(recursive=True)) for row in df_cleaned.collect()]
    return json_list


def data_extract_jobs(
    spark: SparkSession,
    tenant: str,
    api_name: str,
    run_id: str,
    start_str: str,
    end_str: str,
    logger: logging.Logger
):
    """
    Executes the data extraction job lifecycle for a given API:
    - Creates an extraction job
    - Retrieves extraction URL
    - Downloads and parses data via Spark
    - Sends data to processing

    Raises:
        RuntimeError: If job creation fails, extraction URL is missing,
                      or no data is returned from the extraction.
    """
    logger.info(f"[{tenant}] Starting data extraction for API: {api_name} (run_id: {run_id})")

    job_id = create_job(tenant, api_name, start_str, end_str, logger)
    logger.info(f"[{tenant}] Created job with ID: {job_id}")

    if not job_id:
        msg = f"[{tenant}] Failed to create job for {api_name}"
        logger.error(msg)
        raise RuntimeError(msg)
    extraction_url = get_extraction_url(tenant, "data_extraction_jobid", job_id, logger)
    logger.info(f"[{tenant}] Extraction URL: {extraction_url}")

    if not extraction_url:
        msg = f"[{tenant}] No extraction URL returned for job ID: {job_id}"
        logger.error(msg)
        raise RuntimeError(msg)

    extraction_data = load_csv_from_response(spark, extraction_url, tenant, logger)
    if not extraction_data:
        msg = f"[{tenant}] No data returned from extraction URL: {extraction_url}"
        logger.error(msg)
        raise RuntimeError(msg)

    logger.info(f"[{tenant}] Extracted {len(extraction_data)} records from {api_name}")

    process_raw_data(
        spark,
        tenant,
        api_name,
        run_id,
        extraction_data,
        start_str,
        end_str, 1
    )

    logger.info(f"[{tenant}] Data processing completed for {api_name} (run_id: {run_id})")
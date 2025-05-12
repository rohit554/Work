import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.connectors.salesforce.sf_utils import (
    sf_request,
    authorize,
    process_raw_data,
    get_schema,
)
from dganalytics.connectors.salesforce.sf_utils import (
    get_interval,
    get_api_url,
    sf_utils_logger,
)
from io import StringIO
import pandas as pd
from pyspark.sql.functions import col


def exec_event(
    spark: SparkSession,
    tenant: str,
    api_name: str,
    run_id: str,
    extract_start_time: str,
    extract_end_time: str,
):
    logger = sf_utils_logger(tenant, "event_job")
    logger.info("event job inside")
    api_headers = authorize(tenant)
    body = {
        "operation": "query",
        "query": f"SELECT IsAllDayEvent, OwnerId, CreatedById, ActivityDate, Description, DurationInMinutes, EndDateTime, EventSubtype, LastModifiedById, Location, WhoId, IsPrivate, WhatId, IsReminderSet, IsRecurrence2, ServiceAppointmentId, ShowAs, StartDateTime, Subject, ActivityDateTime FROM Event where LastModifiedDate >= {extract_start_time} AND LastModifiedDate <= {extract_end_time}",
        "contentType": "CSV",
    }

    # Submit the job request
    job_resp = rq.post(
        f"{get_api_url(tenant)}/services/data/v58.0/jobs/query/",
        headers=api_headers,
        data=json.dumps(body),
    )

    if job_resp.status_code != 200:
        print("event Details Job Submit API Failed", job_resp.text)
        return

    job_id = job_resp.json().get("id")
    print(f"Job ID: {job_id}")

    # Poll the job status until it's complete
    # check_job_status(tenant, job_id, api_headers)
    job_status_resp = None
    while True:
        # Once the job is complete, fetch the results
        job_status_resp = rq.get(
            f"{get_api_url(tenant)}/services/data/v58.0/jobs/query/{job_id}/results",
            headers=api_headers,
        )
        # print(job_status_resp.status_code, job_status_resp.text)
        if job_status_resp.status_code == 200:
            break

    # print(type(job_status_resp.text))
    csv_data = StringIO(job_status_resp.text)

    # Read the CSV string into a Pandas DataFrame
    pandas_df = pd.read_csv(csv_data)

    if pandas_df.empty:  # Check if the Pandas DataFrame is empty
        logger.info("Got Empty Response : No Data for this interval")
        return
    else:
        pandas_df = pandas_df.fillna("").astype(str)

        schema = get_schema("event")

        # Convert the Pandas DataFrame to a PySpark DataFrame
        df = spark.createDataFrame(pandas_df, schema=schema)

        process_raw_data(
            spark, tenant, api_name, run_id, df, extract_start_time, extract_end_time
        )
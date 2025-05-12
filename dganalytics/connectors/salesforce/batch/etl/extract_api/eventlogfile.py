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


def exec_eventlogfile(
    spark: SparkSession,
    tenant: str,
    api_name: str,
    run_id: str,
    extract_start_time: str,
    extract_end_time: str,
):
    logger = sf_utils_logger(tenant, "eventlogfile_job")
    logger.info("eventlogfile job inside")
    api_headers = authorize(tenant)
    body = {
        "operation": "query",
        "query": f"SELECT Id FROM EventLogFile WHERE EventType in ('LightningInteraction', 'LightningPageView') AND LogDate >= {extract_start_time} AND LogDate < {extract_end_time} ",
        "contentType": "CSV",
    }

    # Submit the job request
    job_resp = rq.post(
        f"{get_api_url(tenant)}/services/data/v58.0/jobs/query/",
        headers=api_headers,
        data=json.dumps(body),
    )

    if job_resp.status_code != 200:
        print("task Details Job Submit API Failed", job_resp.text)
        return

    job_id = job_resp.json().get("id")
    # print(f"Job ID: {job_id}")

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

    lines = job_status_resp.text.splitlines()

    # Extract rows excluding the header
    ids = [
        line.strip('"') for line in lines[1:]
    ]  # Exclude the first line which is the header

    print("Extracted IDs:", ids)
    schema = get_schema(api_name)

    for id in ids:
        new_job_status_resp = None
        while True:
            # Once the job is complete, fetch the results
            new_job_status_resp = rq.get(
                f"{get_api_url(tenant)}/services/data/v53.0/sobjects/EventLogFile/{id}/LogFile",
                headers=api_headers,
            )
            # print(job_status_resp.status_code, job_status_resp.text)
            if new_job_status_resp.status_code == 200:
                break

        csv_data = StringIO(new_job_status_resp.text)
        pandas_df = pd.read_csv(csv_data)
        if pandas_df.empty:
            logger.info("Got Empty Response : No Data for this interval")
            continue
        else:
            pandas_df = pandas_df.fillna("").astype(str)

            # Convert the Pandas DataFrame to a PySpark DataFrame
            df = spark.createDataFrame(pandas_df, schema=schema)

            df = df.withColumn("extractDate", to_date(lit(extract_start_time[0:10])))
            df = df.withColumn("extractIntervalStartTime", to_timestamp(lit(extract_start_time)))
            df = df.withColumn("extractIntervalEndTime", to_timestamp(lit(extract_end_time)))
            print("lit start " + lit(extract_start_time))
            print("lit end " + lit(extract_end_time))
            df = df.withColumn("recordInsertTime", to_timestamp(
                lit(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))))
            df = df.withColumn("recordIdentifier", monotonically_increasing_id())
 
   

            db_name = f"sf_{tenant}"
            table_name = f"raw_eventlogfile"
            df.write.format("delta").mode("append").saveAsTable(
            f"{db_name}.{table_name}"
            )
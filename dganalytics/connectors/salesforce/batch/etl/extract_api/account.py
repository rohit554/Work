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
from pyspark.sql.functions import col, struct, array, lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


def exec_account(
    spark: SparkSession,
    tenant: str,
    api_name: str,
    run_id: str,
    extract_start_time: str,
    extract_end_time: str,
):
    logger = sf_utils_logger(tenant, "account_job")
    logger.info("account job inside")
    api_headers = authorize(tenant)
    body = {
        "operation": "query",
        "query": f"SELECT AccountNumber, AccountSource, AnnualRevenue, BillingCity, BillingCountry, BillingGeocodeAccuracy, BillingLatitude, BillingLongitude, BillingPostalCode, BillingState, BillingStreet, Name, Phone, Website, Industry, NumberOfEmployees, YearStarted, OwnerId, Site, CleanStatus, CreatedById, CustomerPriority__c, DandbCompanyId, DunsNumber, Jigsaw, Description, LastModifiedById, NaicsCode, Fax, NaicsDesc, NumberofLocations__c, OperatingHoursId, Ownership, ParentId, Rating, ShippingStreet, ShippingCity, ShippingState, ShippingPostalCode, ShippingCountry, ShippingLatitude, ShippingLongitude, Sic, SicDesc, SLA__c, SLAExpirationDate__c, SLASerialNumber__c, TickerSymbol, Tradestyle, Type, UpsellOpportunity__c FROM Account where LastModifiedDate >= {extract_start_time} AND LastModifiedDate <= {extract_end_time}",
        "contentType": "CSV",
    }

    # Submit the job request
    job_resp = rq.post(
        f"{get_api_url(tenant)}/services/data/v58.0/jobs/query/",
        headers=api_headers,
        data=json.dumps(body),
    )

    if job_resp.status_code != 200:
        print("account Details Job Submit API Failed", job_resp.text)
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

        # Convert the Pandas DataFrame to a PySpark DataFrame
        df = spark.createDataFrame(pandas_df)

        # Transform BillingAddress columns into an array of struct
        df = df.withColumn(
            "BillingAddress",
            array(
                struct(
                    col("BillingStreet"),
                    col("BillingCity"),
                    col("BillingState"),
                    col("BillingPostalCode"),
                    col("BillingCountry"),
                    col("BillingGeocodeAccuracy"),
                    col("BillingLatitude"),
                    col("BillingLongitude"),
                )
            ),
        ).drop(
            "BillingStreet",
            "BillingCity",
            "BillingState",
            "BillingPostalCode",
            "BillingCountry",
            "BillingGeocodeAccuracy",
            "BillingLatitude",
            "BillingLongitude",
        )

        # Transform ShippingAddress columns into an array of struct
        df = df.withColumn(
            "ShippingAddress",
            array(
                struct(
                    col("ShippingStreet"),
                    col("ShippingCity"),
                    col("ShippingState"),
                    col("ShippingPostalCode"),
                    col("ShippingCountry"),
                    col("ShippingLatitude"),
                    col("ShippingLongitude"),
                )
            ),
        ).drop(
            "ShippingStreet",
            "ShippingCity",
            "ShippingState",
            "ShippingPostalCode",
            "ShippingCountry",
            "ShippingLatitude",
            "ShippingLongitude",
        )

        process_raw_data(
            spark, tenant, api_name, run_id, df, extract_start_time, extract_end_time
        )
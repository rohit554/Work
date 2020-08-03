from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import gpc_request, parser


if __name__ == "__main__":
    tenant, run_id, extract_start_date, extract_end_date = parser()

    spark = get_spark_session(
        app_name="gpc_extract_users_details", tenant=tenant)

    print("Extracting users_details", tenant)
    df = gpc_request(spark, tenant, 'users_details', run_id, extract_start_date)

from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import gpc_request, parser


if __name__ == "__main__":
    tenant, run_id, extract_date = parser()

    spark = get_spark_session(
        app_name="gpc_extract_users", tenant=tenant)

    print("Extracting users", tenant)
    df = gpc_request(spark, tenant, 'users', run_id, extract_date)

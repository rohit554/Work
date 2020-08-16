from dganalytics.connectors.gpc.gpc_setup import db_name
from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import get_dbname, gpc_request, parser


if __name__ == "__main__":
    tenant, run_id, extract_date = parser()
    db_name = get_dbname(tenant)
    spark = get_spark_session(app_name="gpc_extract_users", tenant=tenant, default_db=db_name)

    print("Extracting users", tenant)
    df = gpc_request(spark, tenant, 'users', run_id, extract_date)

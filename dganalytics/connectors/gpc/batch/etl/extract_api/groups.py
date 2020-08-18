from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import get_dbname, gpc_request, parser


if __name__ == "__main__":
    tenant, run_id, extract_date = parser()
    db_name = get_dbname(tenant)
    spark = get_spark_session(app_name="gpc_extract_groups", tenant=tenant, default_db=db_name)

    print("Extracting groups", tenant)
    df = gpc_request(spark, tenant, 'groups', run_id, extract_date)

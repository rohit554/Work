from dganalytics.connectors.gpc.gpc_setup import db_name
from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import get_dbname, gpc_request, extract_parser


if __name__ == "__main__":
    tenant, run_id, extract_date, api_name = extract_parser()
    db_name = get_dbname(tenant)
    spark = get_spark_session(app_name="gpc_extract_" + api_name, tenant=tenant, default_db=db_name)

    print("Extracting " + api_name, tenant)
    df = gpc_request(spark, tenant, api_name, run_id, extract_date)

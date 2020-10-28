from dganalytics.utils.utils import get_spark_session, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import get_dbname
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--run_id', required=True)
    parser.add_argument('--export_name', required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    run_id = args.run_id
    export_name = args.export_name
    db_name = 'gpc_hellofresh'

    app_name = "genesys_powerbi_extract"
    spark = get_spark_session(app_name, tenant, default_db=db_name)
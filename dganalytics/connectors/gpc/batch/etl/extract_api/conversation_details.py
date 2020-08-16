from dganalytics.connectors.gpc.gpc_setup import db_name
from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import gpc_request, parser, get_dbname


if __name__ == "__main__":
    tenant, run_id, extract_date = parser()
    db_name = get_dbname(tenant)
    spark = get_spark_session(app_name="gpc_extract_conversation_details", tenant=tenant, default_db=db_name)

    print("Extracting Conversation Details", tenant)
    df = gpc_request(spark, tenant, 'conversation_details', run_id, extract_date)

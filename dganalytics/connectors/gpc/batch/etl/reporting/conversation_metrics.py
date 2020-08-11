from dganalytics.utils.utils import get_spark_session, get_path_vars
from dganalytics.connectors.gpc.gpc_utils import parser
import os
import shutil

if __name__ == "__main__":
    tenant, run_id, extract_start_date, extract_end_date = parser()
    spark = get_spark_session(app_name="gen_powerbi_dataset_conv_metrics", tenant=tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)

    df = spark.sql("""
                    select cast(metrics.emitDate as date) as emitDate, mediaType, participantId as agentName, metrics.name as metricName, sum(metrics.value) as metricValue from (
                        select sessions.mediaType, participantId, explode(sessions.metrics) as metrics from 
                        (select participants_e.participantId, explode(participants_e.sessions) as sessions from (
                        SELECT *, explode(participants) as participants_e FROM gpc_test.r_conversation_details
                        ) a
                        ) b
                        ) c
                        group by mediaType, participantId, cast(metrics.emitDate as date), metrics.name
                    """)

    op_path = os.path.join(f"{tenant_path}", "data", "pdatasets", "historical")
    os.makedirs(op_path, exist_ok=True)
    df.toPandas().to_csv(os.path.join(op_path, "conversation_metrics.csv"), index=False, header=True)

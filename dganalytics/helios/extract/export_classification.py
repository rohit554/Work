from dganalytics.utils.utils import get_spark_session, get_path_vars
import os
import argparse
from dganalytics.helios.helios_utils import (
    helios_utils_logger,
    get_expanded_regions,
    export_transcripts,
)
from datetime import datetime, timedelta
def load_classifications(spark, tenant: str, extract_start_time: str, extract_end_time: str):
    if tenant == "hellofresh":
        date_obj = datetime.strptime(extract_start_time, "%Y-%m-%dT%H:%M:%SZ")
        if date_obj.weekday() == 6 and date_obj.hour == 0:
            extract_start_time = (date_obj - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    df = spark.sql(
        f"""

            WITH CTE AS (
            SELECT distinct
                ic.tenant tenant_id,
                ic.call_type,
                lc.type,
                lc.phrase
            FROM
                (
                select
                    cr.phrase,
                    cr.type
                from
                    (
                    SELECT
                        root_cause_raw root_cause,
                        outcomeRaw outcome,
                        objectionRaw objection
                    FROM
                        dgdm_{tenant}.fact_transcript_contact_reasons c
                        join dgdm_{tenant}.dim_conversations DC
                            on c.conversationId = DC.conversationId
                            and c.conversationStartDateId = DC.conversationStartDateId
                            WHERE
                            conversationStart between cast('{extract_start_time}' as timestamp) and cast('{extract_end_time}' as timestamp)
                    ) src
                    UNPIVOT (phrase FOR type IN (root_cause, outcome, objection)) cr
                union
                select distinct
                    cm.summary as phrase,
                    'conversation_map' as type
                from
                    dgdm_{tenant}.fact_conversation_map cm
                        join dgdm_{tenant}.dim_conversations DC
                            on cm.conversationId = DC.conversationId
                            and cm.conversationStartDateId = DC.conversationStartDateId
                            WHERE
                            conversationStart between cast('{extract_start_time}' as timestamp) and cast('{extract_end_time}' as timestamp)
                union
                select
                    or.phrase,
                    or.type
                from
                    (
                    SELECT
                        actionSummary agent_response,
                        objectionSummary customer_objection
                    FROM
                        dgdm_hellofresh.fact_objection o
                        join dgdm_{tenant}.dim_conversations DC
                            on o.conversationId = DC.conversationId
                            and o.conversationStartDateId = DC.conversationStartDateId
                            WHERE
                            conversationStart between cast('{extract_start_time}' as timestamp) and cast('{extract_end_time}' as timestamp)                    ) src
                    UNPIVOT (phrase FOR type IN (agent_response, customer_objection)) or
                ) lc
                JOIN dg_datagamz.transcript_insights_config ic
                    ON ic.tenant = '{tenant}'
            )
            
            MERGE INTO dgdm_{tenant}.label_classification AS tgt USING CTE AS src ON tgt.tenant_id = src.tenant_id
            AND tgt.call_type = src.call_type
            AND tgt.type = src.type
            AND tgt.phrase = src.phrase
            WHEN NOT MATCHED THEN
            INSERT
            (tenant_id, call_type, type, phrase)
            VALUES
            (
                src.tenant_id,
                src.call_type,
                src.type,
                src.phrase
            ) """
    )

def get_classifications(spark, tenant: str, extract_start_time: str, extract_end_time: str):
    
    load_classifications(spark, tenant, extract_start_time, extract_end_time)
    return spark.sql(
        f"""
            select
            tenant_id,
            call_type,
            type,
            phrase raw_phrases,
            '' label,
            '' dictionary_list,                    
            ROW_NUMBER() OVER (
                ORDER BY
                tenant_id
            ) AS row_num
            from
            dgdm_{tenant}.label_classification
            where COALESCE(label, '') = ''
        """
    )

def prepare(spark, tenant: str, extract_start_time: str, extract_end_time: str):
    final_df = get_classifications(spark, tenant, extract_start_time, extract_end_time)
    if final_df is not None and final_df.count() >= 1:
        regions_df = get_expanded_regions(spark, final_df.count())

        final_df_with_regions = final_df.join(regions_df, on="row_num", how="inner")

        return final_df_with_regions.select(
            "tenant_id",
            "bedrock_region",
            "call_type",
            "type",
            "raw_phrases",
            "label",
            "dictionary_list",
            "row_num",
        )
    else:
        return None
def export(
    spark,
    tenant: str,
    extract_start_time: str,
    extract_end_time: str,
    batch_size: int,
    folder_name: str,
    file_suffix: str,
):
    df = prepare(spark, tenant, extract_start_time, extract_end_time)
    if df is not None and df.count() >= 1:
        tenant_path = get_path_vars(tenant)[0]
        batch_output_directory = os.path.join(
            tenant_path, "data", "classification_input"
        )
        export_transcripts(
            batch_output_directory,
            df,
            folder_name,
            tenant,
            file_suffix,
            batch_size,
            logger,
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--batch_size", required=True)
    parser.add_argument("--folder_name", required=True)
    parser.add_argument("--extract_start_time", required=True)
    parser.add_argument("--extract_end_time", required=True)

    parser.add_argument(
        "--file_suffix", required=True
    )  # file_suffix  = "classify"  

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    batch_size = int(args.batch_size)
    folder_name = args.folder_name
    file_suffix = args.file_suffix
    extract_start_time = args.extract_start_time
    extract_end_time = args.extract_end_time

    spark = get_spark_session(app_name="Label Classification", tenant=tenant)

    logger = helios_utils_logger(tenant, f"{tenant}_export_classification")

    export(spark, tenant, extract_start_time, extract_end_time, batch_size, folder_name, file_suffix)
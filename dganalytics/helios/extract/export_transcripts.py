from dganalytics.utils.utils import get_spark_session, get_path_vars
from datetime import datetime
import os
import argparse
from dganalytics.helios.helios_utils import (
    helios_utils_logger,
    export_transcripts,
    get_expanded_regions,
)
from pyspark.sql.functions import lit, col

def get_conversation_transcript(
    spark, tenant: str, extract_start_time: str, extract_end_time: str, primary
):
    checkTable = "fact_transcript_insights" if primary else "fact_transcript_conformance"
    df = spark.sql(
        f"""
            SELECT
            f.conversationId,
            concat_ws("\n", collect_list(f.phrase)) AS conversation,
            lower(l.label) scenario,
            ROW_NUMBER() OVER(
                ORDER BY
                f.conversationId
            ) AS row_num
            FROM
            (
                SELECT
                fta.conversationId,
                concat(
                    line,
                    '|',
                    participant,
                    '|',
                    action
                ) AS phrase,
                fta.conversationStartDateId
                FROM
                dgdm_{tenant}.fact_conversation_transcript_actions fta
                join dgdm_{tenant}.dim_conversations C
                on fta.conversationId = C.conversationId
                and fta.conversationStartDateId = C.conversationStartDateId
                WHERE
                 conversationStart between cast('{extract_start_time}' as timestamp) and cast('{extract_end_time}' as timestamp)
                 AND NOT EXISTS (
                            SELECT
                            1
                            FROM
                            dgdm_{tenant}.{checkTable} fti
                            WHERE
                            fti.conversationId = fta.conversationId
                            and fta.conversationStartDateId = fti.conversationStartDateId
                        )
            ) f 
            left join dgdm_{tenant}.fact_transcript_contact_reasons cr 
                ON f.conversationId = cr.conversationId   
                AND f.conversationStartDateId = cr.conversationStartDateId
            left join dgdm_{tenant}.label_classification l
                on cr.root_cause_raw = l.phrase
                and l.type = 'root_cause' 
            GROUP BY
            f.conversationId, l.label
    """
    )
    if tenant == "hellofresh":
        date_obj = datetime.strptime(extract_start_time, "%Y-%m-%dT%H:%M:%SZ")
        max_row = df.count()
        if date_obj.weekday() == 6 and date_obj.hour == 0:
            last_df = spark.sql(
                f"""
               SELECT
                    f.conversationId,
                    concat_ws("\n", collect_list(f.phrase)) AS conversation,
                    lower(l.label) scenario,
                    ROW_NUMBER() OVER(
                        ORDER BY
                        f.conversationId
                    ) + {max_row} AS row_num
                    FROM
                    (
                        SELECT
                        fta.conversationId,
                        concat(
                            line,
                            '|',
                            participant,
                            '|',
                            action
                        ) AS phrase,
                        conversationStartDateId
                        FROM
                        dgdm_{tenant}.fact_conversation_transcript_actions fta
                        join gpc_{tenant}.dim_last_handled_conversation fti 
                            on fti.conversationId = fta.conversationId
                            and fti.wrapUpCodeId = 'c0ad6c2a-a0fa-42c5-97c3-2e2d49712564'
                        WHERE
                            conversationStartDateId between (select dateId from dgdm_{tenant}.dim_date where dateVal = cast(DATEADD(DAY, -7, '{extract_start_time}') as date)) and (select dateId from dgdm_{tenant}.dim_date where dateVal = cast(DATEADD(DAY, -1, '{extract_end_time}') as date))
                           
                        )f 
                        left join dgdm_{tenant}.fact_transcript_contact_reasons cr 
                            ON f.conversationId = cr.conversationId   
                            AND f.conversationStartDateId = cr.conversationStartDateId
                        left join dgdm_{tenant}.label_classification l
                            on cr.root_cause_raw = l.phrase
                            and l.type = 'root_cause' 
                        GROUP BY
                        f.conversationId, l.label
                """
                )
            return df.union(last_df)
    return df

def get_insights_config(tenant: str):
    return spark.sql(
        f"""
       SELECT
            process_insight,
            process_action,
            process_quality,
            process_compliance,
            process_summary,
            tenant tenant_id,
            call_type,
            process_conformance,
            process_value_stream,
            process_conversation_map,
            process_apprehension,
            vstream_ref,process_objection
            FROM
            dg_datagamz.transcript_insights_config
            WHERE
            tenant = '{tenant}'
        """
    )

def prepare(spark, tenant: str, extract_start_time: str, extract_end_time: str, primary):
    df = get_conversation_transcript(
        spark, tenant, extract_start_time, extract_end_time, primary
    )
    if df is not None and df.count() >= 1:
        config_df = get_insights_config(tenant)

        final_df = df.join(config_df)

        regions_df = get_expanded_regions(spark, df.count())

        final_df_with_regions = final_df.join(regions_df, on="row_num", how="inner")

        return final_df_with_regions.select(
            "tenant_id",
            "bedrock_region",
            "call_type",
            "conversationId",
            "conversation",
            "process_insight",
            "process_action",
            "process_quality",
            "process_compliance",
            "process_summary",
            "process_conformance",
            "process_value_stream",
            "process_conversation_map",
            "process_apprehension",
            "process_objection",
            "vstream_ref",
            "scenario",
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
    logger,
    primary: bool
):
    df = prepare(spark, tenant, extract_start_time, extract_end_time, primary)
    if df is not None and df.count() >= 1:
        tenant_path = get_path_vars(tenant)[0]
        if primary:
            batch_output_directory = os.path.join(tenant_path, "data", "insights_input_new")
            
            # export for primary part - insights,quality,compliance and conformance
            export_transcripts(
                batch_output_directory, 
                df, 
                folder_name, 
                tenant, 
                file_suffix, 
                batch_size,logger
            )
        else:
            batch_output_directory = os.path.join(tenant_path, "data", "insights_input_value_stream")
            df = df.select("tenant_id", "bedrock_region", "call_type", "conversationId", "conversation", "process_conformance","scenario", "row_num")
            df = df.filter((col("scenario").isNotNull()) & (col("scenario") == "cooking preferences"))
            # export for secondary part - value_stream.conversation_map, and apprehension
            export_transcripts(
                batch_output_directory, 
                df, 
                folder_name, 
                tenant, 
                file_suffix, 
                batch_size,logger
            )
        insert_audit(spark, df.select("conversationId", "bedrock_region"), tenant)


def insert_audit(spark, df, tenant: str):
    df.createOrReplaceTempView("audit")
    spark.sql(
        f"""
        INSERT INTO dgdm_{tenant}.transcript_insights_audit (
            conversationId, conversationStartDateId, region, isExportedToLLM, exportedTimestamp
        )
        SELECT
            a.conversationId,
            conversationStartDateId,
            bedrock_region,
            true,
            current_timestamp()
        FROM audit a
        INNER JOIN dgdm_{tenant}.dim_conversations c
        ON a.conversationId = c.conversationId
    """
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--extract_start_time", required=True)
    parser.add_argument("--extract_end_time", required=True)
    parser.add_argument("--batch_size", required=True)
    parser.add_argument("--folder_name", required=True)
    parser.add_argument(
        "--file_suffix", required=True
    )  # file_suffix  = "transcript.dev"
    parser.add_argument("--primary", required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    extract_start_time = args.extract_start_time
    extract_end_time = args.extract_end_time
    batch_size = int(args.batch_size)
    folder_name = args.folder_name
    file_suffix = args.file_suffix
    primary = True if args.primary == 'True' else False

    spark = get_spark_session(app_name="GPC_RawTranscriptInsights", tenant=tenant)

    logger = helios_utils_logger(tenant, f"{tenant}_export_transcripts")

    export(
        spark,
        tenant,
        extract_start_time,
        extract_end_time,
        batch_size,
        folder_name,
        file_suffix, logger, primary
    )
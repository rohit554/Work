import argparse
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_spark_session, env, get_path_vars
from dganalytics.connectors.gpc.gpc_utils import get_schema, get_dbname
from dganalytics.connectors.gpc.batch.etl.extract_api.gpc_api_config import gpc_end_points, gpc_base_url
from inflection import camelize


def create_database(spark: SparkSession, path: str, db_name: str):
    spark.sql(f"create database if not exists {db_name}  LOCATION '{path}/{db_name}'")

    return True


def create_ingestion_stats_table(spark: SparkSession, db_name: str, db_path: str):
    print("Creating Ingestion stats table for genesys")
    spark.sql(
        f"""
                create table if not exists {db_name}.ingestionStats
                (
                    apiName string,
                    endPoint string,
                    pageCount int,
                    recordsFetched bigint,
                    rawDataFile_loc string,
                    adfRunId string,
                    extractDate date,
                    loadDateTime timestamp
                )
                    using delta
            LOCATION '{db_path}/{db_name}/ingestionStats'"""
    )
    return True


def create_dim_tables(spark: SparkSession, db_name: str):
    spark.sql(
        f"""create table {db_name}.gDimConversations
            (
                conversationId string,
                conversationStart timestamp,
                conversationEnd timestamp,
                originatingDirection string,
                sessionId string,
                sessionStart timestamp comment 'first segment start date',
                sessionEnd timestamp comment 'last segment end date',
                sessionDirection string,
                queueId string,
                mediaType string,
                messageType string,
                agentId string,
                wrapUpCode string,
                wrapUpNote string,
                conversationStartDate date
            )
            using delta
            PARTITIONED BY (conversationStartDate)
            LOCATION '{db_path}/{db_name}/gDimConversations'
            """
    )

    spark.sql(
        f"""create table {db_name}.gDimConversationMetrics
            (
                sessionId string,
                emitDateTime timestamp,
                nAbandon int,
                nAcd int,
                nAcw int,
                nAnswered int,
                nBlindTransferred int,
                nConnected int,
                nConsult int,
                nConsultTransferred int,
                nError int,
                nHandle int,
                nHeldComplete int,
                nOffered int,
                nOutbound int,
                nOutboundAbandoned int,
                nOutboundAttempted int,
                nOutboundConnected int,
                nOverSla int,
                nShortAbandon int,
                nTalkComplete int,
                nTransferred int,
                tAbandon float,
                tAcd float,
                tAcw float,
                tAgentResponse float,
                tAnswered float,
                tContacting float,
                tDialing float,
                tHandle float,
                tHeldComplete float,
                tIvr float,
                tNotResponding float,
                tShortAbandon float,
                tTalkComplete float,
                tVoicemail float,
                tWait float,
                emitDate date
                
            )
            using delta
            PARTITIONED BY (emitDate)
            LOCATION '{db_path}/{db_name}/gDimConversationMetrics'
            """
    )

    return True


def create_raw_table(api_name: str, spark: SparkSession, db_name: str):
    schema = get_schema(api_name, tenant_path)
    table_name = "raw" + camelize(f"{api_name}")
    print(table_name)
    if gpc_end_points[api_name]["raw_table_update"]["partition"] is not None:
        partition = "partitioned by (" + ",".join(gpc_end_points[api_name]["raw_table_update"]["partition"]) + ")"
    else:
        partition = ""
    spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=schema).registerTempTable(table_name)
    create_qry = f"""create table if not exists {db_name}.{table_name}
                        using delta {partition} LOCATION '{db_path}/{db_name}/{table_name}' as
                    select *, cast('1900-01-01' as date) extractDate from {table_name} limit 0"""
    spark.sql(create_qry)

    return True


def raw_tables(spark: SparkSession, db_name: str, db_path: str, tenant_path: str):
    create_raw_table("users", spark, db_name)
    create_raw_table("routing_queues", spark, db_name)
    create_raw_table("groups", spark, db_name)
    create_raw_table("users_details", spark, db_name)
    create_raw_table("conversation_details", spark, db_name)
    create_raw_table("wrapupcodes", spark, db_name)

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant

    db_name = get_dbname(tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)

    spark = get_spark_session(app_name="GPC_Setup", tenant=tenant)

    create_database(spark, db_path, db_name)
    create_ingestion_stats_table(spark, db_name, db_path)
    raw_tables(spark, db_name, db_path, tenant_path)

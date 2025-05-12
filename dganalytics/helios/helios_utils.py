from dganalytics.utils.utils import get_logger, get_path_vars, get_secret
from dganalytics.helios.transform.scripts import *
import os
import datetime
import argparse
from dganalytics.utils.aws_utils import push_file

def helios_utils_logger(tenant, app_name):
    global logger
    logger = get_logger(tenant, app_name)
    return logger

def get_sql_query(
    spark,
    transformation,
    tenant,
    extract_date,
    extract_start_time,
    extract_end_time,
    interaction_type,
):
    logger = helios_utils_logger(tenant, "helios")
    tenant_path, db_path, log_path = get_path_vars("")
    file_path = os.path.join(
        tenant_path,
        "code",
        "dganalytics",
        "dganalytics",
        "helios",
        "transform",
        "scripts",
    )
    if interaction_type == "insert":
        sql_file_path = os.path.join(file_path, interaction_type + "_query.sql")
    # elif interaction_type == "delete" and transformation.startswith("mv"):
    #     sql_file_path = os.path.join(file_path, "mv_" + interaction_type + ".sql")
    else:
        sql_file_path = os.path.join(
            file_path, transformation + "_" + interaction_type + ".sql"
        )
    logger.info(f"{transformation}_{interaction_type}")
    try:
        # Read SQL query from file
        with open(sql_file_path, "r") as file:
            sql_query = file.read()
        if (
            transformation == "dim_conversation_ivr_menu_selections"
            and interaction_type == "select"
        ):
            return sql_query
        else:
            formatted_sql_query = sql_query.format(
                tenant=tenant,
                extract_date=extract_date,
                extract_start_time=extract_start_time,
                extract_end_time=extract_end_time,
                transformation=transformation,
            )
            return formatted_sql_query
    except Exception as e:
        logger.exception(
            f"Error Occured in reading helios transformation SQL Query for {extract_start_time}_{extract_end_time}_{tenant}_{transformation}_{sql_file_path}"
        )
        logger.exception(e, stack_info=True, exc_info=True)
        raise Exception

def get_insert_overwrite_sql_query(spark, transformation, tenant):
    logger = helios_utils_logger(tenant, "helios")
    tenant_path, db_path, log_path = get_path_vars("")
    file_path = os.path.join(
        tenant_path,
        "code",
        "dganalytics",
        "dganalytics",
        "helios",
        "transform",
        "scripts",
    )
    sql_file_path = os.path.join(file_path, transformation + ".sql")

    try:
        # Read SQL query from file
        with open(sql_file_path, "r") as file:
            sql_query = file.read()
        formatted_sql_query = sql_query.format(tenant=tenant)
        return formatted_sql_query
    except Exception as e:
        logger.exception(
            f"Error Occured in reading helios transformation SQL Query for {tenant}_{transformation}_{sql_file_path}"
        )
        logger.exception(e, stack_info=True, exc_info=True)
        raise Exception

def get_update_sql_query(
    spark,
    transformation,
    tenant,
    extract_date,
    extract_start_time,
    extract_end_time,
    interaction_type,
):
    logger = helios_utils_logger(tenant, "helios")
    tenant_path, db_path, log_path = get_path_vars("")
    file_path = os.path.join(
        tenant_path,
        "code",
        "dganalytics",
        "dganalytics",
        "helios",
        "transform",
        "scripts",
    )
    sql_file_path = os.path.join(
        file_path, transformation + "_" + interaction_type + ".sql"
    )
    logger.info(f"{transformation}_{interaction_type}")
    try:
        # Read SQL query from file
        with open(sql_file_path, "r") as file:
            sql_query = file.read()
        formatted_sql_query = sql_query.format(
            tenant=tenant,
            extract_date=extract_date,
            extract_start_time=extract_start_time,
            extract_end_time=extract_end_time,
            transformation=transformation,
        )
        return formatted_sql_query
    except Exception as e:
        logger.exception(
            f"Error Occured in reading helios transformation SQL Query for {extract_start_time}_{extract_end_time}_{tenant}_{transformation}_{sql_file_path}"
        )
        logger.exception(e, stack_info=True, exc_info=True)
        raise Exception

def transform_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--run_id", required=True)
    parser.add_argument(
        "--extract_start_time",
        required=True,
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ"),
    )
    parser.add_argument(
        "--extract_end_time",
        required=True,
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ"),
    )
    parser.add_argument("--transformation", required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    run_id = args.run_id
    transformation = args.transformation
    extract_start_time = args.extract_start_time.strftime("%Y-%m-%dT%H:%M:%S")
    extract_end_time = args.extract_end_time.strftime("%Y-%m-%dT%H:%M:%S")
    extract_date = args.extract_start_time.strftime("%Y-%m-%d")

    return (
        tenant,
        run_id,
        extract_date,
        extract_start_time,
        extract_end_time,
        transformation,
    )

def export_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--extract_name", required=True)
    parser.add_argument("--output_file_name", required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    run_id = args.run_id
    extract_name = args.extract_name
    output_file_name = args.output_file_name

    return tenant, run_id, extract_name, output_file_name

def export_transcripts(
    batch_output_directory, df, folder_name, tenant, suffix, batch_size, logger
):
    try:
        os.makedirs(batch_output_directory, exist_ok=True)
        total_rows = df.count()
        # Loop through the DataFrame in batches
        for offset in range(0, total_rows, batch_size):
            # Filter the current batch
            batch_df = df.filter(
                (df.row_num > offset) & (df.row_num <= offset + batch_size)
            )
            batch_output_path = os.path.join(
                batch_output_directory, f"batch_{int(offset/batch_size)}.csv"
            )

            batch_df.toPandas().fillna("").to_csv(
                batch_output_path, header=True, index=False
            )
            logger.info(f"Batch {int(offset/batch_size)} saved to {batch_output_path}")
            push_file(folder_name, batch_output_path, tenant, suffix, logger)
            # dbutils.fs.rm(batch_output_path.replace("/dbfs", ""), True)
            # logger.info(f"{batch_output_path} deleted successfully from blob")
    except Exception as e:
        logger.exception(f"Error occurred while exporting batch file", e)
        raise Exception

def get_expanded_regions(spark, total_rows):
    regions = [
        region.strip("'") for region in get_secret("AWSBedrockRegions").split(",")
    ]
    expanded_regions = (
        regions * (total_rows // len(regions)) + regions[: total_rows % len(regions)]
    )
    # Create a DataFrame for the regions
    return spark.createDataFrame(
        [(i + 1, region) for i, region in enumerate(expanded_regions)],
        ["row_num", "bedrock_region"],
    )
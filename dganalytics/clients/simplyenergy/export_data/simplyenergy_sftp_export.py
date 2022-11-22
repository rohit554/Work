import argparse
import os
import pysftp
from datetime import datetime
from dganalytics.utils.utils import get_path_vars, get_secret, get_spark_session
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger
from dganalytics.clients.simplyenergy.export_data import simplyenergy_export_sql

if __name__ == "__main__":
    tenant = "simplyenergy"
    db_name = f"gpc_{tenant}"
    app_name = f"{tenant}_sftp_export"
    logger = gpc_utils_logger(tenant, app_name)
    logger.info(f"starting {tenant} sftp export")
    parser = argparse.ArgumentParser()
    parser.add_argument('--extract_start_time', required=True,
                      type=lambda s: datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ'))
    parser.add_argument('--extract_end_time', required=True, type=lambda s: datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ'))
    parser.add_argument('--export_name', required=True)
    parser.add_argument('--sftp_host', required=True)
    parser.add_argument('--sftp_username', required=True)
    parser.add_argument('--sftp_password', required=True)
    parser.add_argument('--sftp_folder_path', required=False)
    args, unknown_args = parser.parse_known_args()
    extract_start_time = args.extract_start_time.strftime('%Y-%m-%dT%H:%M:%S')
    extract_end_time = args.extract_end_time.strftime('%Y-%m-%dT%H:%M:%S')
    export_name = args.export_name
    sftp_host = args.sftp_host
    sftp_username = args.sftp_username
    sftp_password = args.sftp_password
    sftp_folder_path = args.sftp_folder_path

    fact_export_type = export_name.startswith("fact_")

    if export_name.startswith("fact_"):
        export_name_normalised = f"{export_name}_{args.extract_start_time.strftime('%Y%m%dT%H%M')}"
    else:
        export_name_normalised = export_name

    spark_session = get_spark_session(app_name, tenant, default_db=db_name)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    tenant_path_without_dbfs = tenant_path

    if tenant_path_without_dbfs.startswith("/dbfs"):
        tenant_path_without_dbfs = tenant_path_without_dbfs.replace("/dbfs/", "/")

    export_path_df = f"{tenant_path_without_dbfs}/data/temp/{export_name_normalised}"
    export_path_os = f"{tenant_path}/data/temp/{export_name_normalised}"

    try:
        logger.info(
            f"exporting {export_name} data from {extract_start_time} to {extract_end_time} to {export_path_df}...")

        if export_name.startswith("fact_"):
            df = spark_session.sql(simplyenergy_export_sql.__dict__[export_name](extract_start_time, extract_end_time))
        else:
            df = spark_session.sql(simplyenergy_export_sql.__dict__[export_name]).coalesce(1)

        df.write.option("escape", "\"").option("multiline", "true").csv(path=export_path_df, mode="overwrite", sep=",", header=True, dateFormat="yyyy-MM-dd",
                     timestampFormat="yyyy-MM-dd HH:mm:ss", encoding="utf-8")

        sftp_host = get_secret(sftp_host)
        sftp_username = get_secret(sftp_username)
        sftp_password = get_secret(sftp_password)
        sftp_cn_opts = pysftp.CnOpts()
        sftp_cn_opts.hostkeys = None

        logger.info(f"uploading exported files to sftp://{sftp_username}@{sftp_host}/...")

        with pysftp.Connection(host=sftp_host, username=sftp_username, password=sftp_password,
                               cnopts=sftp_cn_opts) as sftp_conn:
            export_part = 0
            for file_name in sorted(os.listdir(export_path_os)):
                file_path = f"{export_path_os}/{file_name}"
                if not os.path.isfile(file_path) or not file_path.lower().endswith(".csv"):
                    os.remove(file_path)
                    continue

                if export_name.startswith("fact_"):
                    export_part += 1
                    if sftp_folder_path == "./" or sftp_folder_path is None:
                        upload_name = f"./{export_name_normalised}_{export_part}.csv"
                    else: 
                      upload_name = f"./{sftp_folder_path}/{export_name_normalised}_{export_part}.csv"
                else:
                    if sftp_folder_path == "./" or sftp_folder_path is None:
                        upload_name = f"./{export_name_normalised}.csv"
                    else: 
                      upload_name = f"./{sftp_folder_path}/{export_name_normalised}.csv"

                logger.info(f"uploading {file_path} to {upload_name}...")
                sftp_conn.put(file_path, upload_name)
                logger.info(f"deleting {file_path}...")
                os.remove(file_path)

        logger.info(f"deleting {export_path_os}...")
        os.removedirs(export_path_os)
    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise

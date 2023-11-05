import argparse
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_spark_session, get_path_vars

def create_database(spark: SparkSession, path: str, db_name: str):
    spark.sql(
        f"create database if not exists {db_name}  LOCATION '{path}/{db_name}'")

    return True

def create_jira_project_issues_table(spark: SparkSession, db_name: str, db_path: str):
    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS dg_{tenant}.mtd_metrics
            (
                Name string,
                Emp_ID integer,
                Sentiment double,
                Composite double,
                NTT double,
                XSR double,
                AICR string,
                tNPS string,
                CPR double,
                Hold string,
                ACW string,
                Calls double,
                AHT string,
                Talk string,
                Showrate double,
                Xfer double,
                `SC%` double,
                CPC string,
                OB double,
                MTD_DT date,
                Tier string,
                orgId string
            )
            USING delta
            PARTITIONED BY (orgId)
            LOCATION ''{db_path}/dg_{tenant}/mtd_metrics'
        """)
    return True
  
if __name__ == "__main__":
    tenant = "comcast"
    db_name = f"dg_{tenant}"
    tenant_path, db_path, log_path = get_path_vars(tenant)
    spark = get_spark_session(app_name="comcast_mtd_metrics_table_setup",
                              tenant=tenant, default_db=db_name)
    create_database(spark, db_path, db_name)
    create_jira_project_issues_table(spark, db_name, db_path)
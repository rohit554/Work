from dganalytics.utils.utils import get_spark_session, get_path_vars, push_gamification_data, export_powerbi_csv
import argparse
import pandas as pd
import os
import numpy as np
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DateType, DoubleType, IntegerType, DateType
import openpyxl
from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat, lit, isnan, to_date, date_format, coalesce, when, current_date

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'datagamz'
    customer = 'comcastprod'
    db_name = f"dg_{customer}"
    spark = get_spark_session('mtd_metrics_data', tenant)
    tenant_path, db_path, log_path = get_path_vars(customer)

    if input_file.endswith(".xlsx"):
        kpi = pd.read_excel(os.path.join(tenant_path, "data", "raw", "mtd_metrics", input_file), engine='openpyxl', skiprows=10, usecols=lambda col: col != 'Unnamed: 0')
    elif input_file.endswith(".csv"):
        kpi = pd.read_csv(os.path.join(tenant_path, "data", "raw", "mtd_metrics", input_file))

    if input_file.endswith(".xlsx"):
        kpi1 = pd.read_excel(os.path.join(tenant_path, "data", "raw", "mtd_metrics", input_file), engine='openpyxl', skiprows=9, usecols=lambda col: col != 'Unnamed: 0')
    elif input_file.endswith(".csv"):
        kpi1 = pd.read_csv(os.path.join(tenant_path, "data", "raw", "mtd_metrics", input_file))

    column_renaming = {
              "Employee ID ": "Emp_ID","Sentiment %": "Sentiment","XSR %": "XSR","AICR Queue": "AICR","Ave. Hold (Comcast)": "Hold","ACW (Comcast)": "ACW","NCH (Comcast)": "Calls","AHT (Comcast)": "AHT","Ave. Talk (Comcast)": "Talk","Show Rate": "Showrate","Transfer % (Comcast)": "Xfer","Short Calls %": "SC%","CpC": "CPC","OB % (Comcast)": "OB"
          }

    kpi.rename(columns=column_renaming, inplace=True)

    columns = [
              "Name","Emp_ID","Sentiment","Composite","NTT","XSR","AICR","tNPS","CPR","Hold","ACW","Calls","AHT","Talk","Showrate","Xfer","SC%","CPC", "OB"
          ]
    
    kpi = kpi[columns]
    grand_total_index = kpi[kpi["Name"] == "Grand Total"].index[0]
    kpi["Name"] = kpi["Name"].iloc[:grand_total_index]
    kpi = kpi.iloc[:grand_total_index]
    kpi['Tier'] = 'T1'

    percentage_columns = ["Sentiment", "Composite", "NTT", "XSR", "CPR", "Showrate", "Xfer", "SC%", "OB", "AICR"]
    for column in percentage_columns:
        kpi[column] = (kpi[column] * 100).astype(float)
    
    kpi = kpi.fillna('')

    kpi1 = kpi1.iloc[:, 0:1]
    kpi1.insert(0, "MTD_DT", kpi1.columns[0])
    kpi1 = kpi1[["MTD_DT"]]
    kpi1["MTD_DT"] = pd.to_datetime(kpi1["MTD_DT"], format="%d-%m-%Y")
    date_value = kpi1["MTD_DT"].values[0]
    kpi["MTD_DT"] = date_value
    kpi1 = kpi1.drop_duplicates(subset=['MTD_DT'])
    kpi['orgId'] = customer

    merged_data = pd.concat([kpi, kpi1], axis=0, ignore_index=True)
    merged_data["MTD_DT"] = merged_data["MTD_DT"].dt.date
    kpi_spark = spark.createDataFrame(merged_data)
    kpi_spark = kpi_spark.withColumn("Emp_ID", col("Emp_ID").cast("integer"))
    kpi_spark = kpi_spark.dropDuplicates()
    kpi_spark = kpi_spark.filter(~(isnan(col("Name")) | (col("Name").isNull())))
    double_columns = ["Sentiment", "Composite", "NTT", "XSR", "CPR", "Showrate", "Xfer", "SC%", "OB", "AICR"]
    for column in percentage_columns:
        kpi_spark = kpi_spark.withColumn(column, col(column).cast("double"))
    kpi_spark = kpi_spark.createOrReplaceTempView("mtd_metrics")
    mtd_metrics_data = spark.sql(f"""
                                select
                                    Emp_ID as `User Id`,
                                    date_format(MTD_DT, 'dd-MM-yyyy') as `Date`,
                                    coalesce(NTT, "") as NTT,
                                    coalesce(tNPS, "") as tNPS,
                                    coalesce(AICR, "") as `AICR Seven Days`,
                                    coalesce(XSR, "") as XSR,
                                    coalesce(AHT, "") as AHT,
                                    coalesce(Showrate, "") as `SHOWRATE`,
                                    coalesce(CPR, "") as CPR
                                from mtd_metrics
                            """)
    
    push_gamification_data(
            mtd_metrics_data.toPandas(), customer.upper(), 'Comcast_MTD_Connection')

    spark.sql(f"""
                INSERT INTO {db_name}.mtd_metrics
                SELECT
                Name,Emp_ID,Sentiment,Composite,NTT,XSR,AICR,tNPS,CPR,Hold,ACW,Calls,AHT,Talk,Showrate,Xfer,`SC%`,CPC,OB,MTD_DT,Tier,orgId
                FROM mtd_metrics A
            """)
    
    pm_mtd_metrics_data = spark.sql(f"""
                                    select DISTINCT *
                                    from {db_name}.mtd_metrics
                                    """)

    export_powerbi_csv(customer, pm_mtd_metrics_data, f"pm_mtd_metrics")
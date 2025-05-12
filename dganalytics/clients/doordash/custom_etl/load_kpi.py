from dganalytics.utils.utils import get_spark_session, get_path_vars, push_gamification_data
import argparse
import pandas as pd
import os
import numpy as np
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DateType, DoubleType, IntegerType, DateType

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'datagamz'
    customer = 'doordashprod'
    spark = get_spark_session(f'{tenant}_load_kpi', tenant)
    tenant_path, db_path, log_path = get_path_vars(customer)

    schema = StructType([
                StructField("HRMS ID", IntegerType(), True),
                StructField("Date", DateType(), True),
                StructField("Chat Handled", DoubleType(), True),
                StructField("Chat Handled Time (Hours)", DoubleType(), True),
                StructField("Total Survery", DoubleType(), True),
                StructField("CSAT Count", DoubleType(), True),
                StructField("PKT Score", DoubleType(), True),
                StructField("Scheduled Count", IntegerType(), True),
                StructField("Present Count", DoubleType(), True),
                StructField("Total TX FCR Count", DoubleType(), True),
                StructField("TX FCR Count", DoubleType(), True),
                StructField("Total Staff Time (Hours)", DoubleType(), True),
                StructField("Net Staff Time (Hours)", DoubleType(), True),
                StructField("TP_Downtime (Hours)", DoubleType(), True),
                StructField("Client_Downtime (Hours)", DoubleType(), True),
                StructField("Quality Score", DoubleType(), True)
            ])

    kpi = pd.DataFrame()
    if input_file.endswith(".xlsx"):
        kpi = pd.read_excel(os.path.join(tenant_path, "data", "raw", "kpi", input_file), engine='openpyxl', sheet_name = "Sep'23")
    elif input_file.endswith(".csv"):
        kpi = pd.read_csv(os.path.join(tenant_path, "data", "raw", "kpi", input_file))
    elif input_file.endswith(".xlsb"):
        xls = pd.ExcelFile(os.path.join(tenant_path, "data", "raw", "kpi", input_file))
        frames = []
        
        for sheet in xls.sheet_names:
            if sheet == "Calculation":
                continue
            sheetDf = pd.read_excel(xls, sheet)
            sheetDf['Date'] = pd.to_datetime(sheetDf['Date'], unit='d', origin='1899-12-30')
            frames.append(sheetDf)

        kpi = pd.concat(frames) 

    def try_convert_to_float(x):
        try:
            return float(x)
        except (ValueError, TypeError):
            return x

    kpi['Chat Handled'] = kpi['Chat Handled'].astype(float)
    kpi['Chat Handled Time (Hours)'] = kpi['Chat Handled Time (Hours)'].astype(float)
    kpi['Total Survery'] = kpi['Total Survery'].astype(float)
    kpi['CSAT Count'] = kpi['CSAT Count'].astype(float)
    kpi['PKT Score'] = kpi['PKT Score'].astype(float)
    kpi['Scheduled Count'] = kpi['Scheduled Count'].astype(int)
    kpi['Present Count'] = kpi['Present Count'].astype(float)
    kpi['Total TX FCR Count'] = kpi['Total TX FCR Count'].apply(try_convert_to_float)
    kpi['TX FCR Count'] = kpi['TX FCR Count'].apply(try_convert_to_float)
    kpi['Total Staff Time (Hours)'] = kpi['Total Staff Time (Hours)'].apply(try_convert_to_float)
    kpi['Net Staff Time (Hours)'] = kpi['Net Staff Time (Hours)'].apply(try_convert_to_float)
    kpi['TP_Downtime (Hours)'] = kpi['TP_Downtime (Hours)'].astype(float)
    kpi['Client_Downtime (Hours)'] = kpi['Client_Downtime (Hours)'].astype(float)
    kpi['Quality Score'] = np.where(kpi['Quality Score'] == 0, np.nan, kpi['Quality Score'] * 100)
    kpi['PKT Score'] = np.where(kpi['PKT Score'] == 0, np.nan, kpi['PKT Score'] * 100)

    kpi = kpi.fillna('')
    kpi = spark.createDataFrame(kpi)
    kpi.createOrReplaceTempView("kpi")

    kpi = spark.sql(f"""
        SELECT  `HRMS ID` UserID,
                date_format(`Date`, 'dd-MMM-yyyy') AS Date,
                `Chat Handled`,
                `Chat Handled Time (Hours)` `Chat Handled Time Hour`,
                `Total Survery`,
                `CSAT Count`,
                `PKT Score` `Process Knowledge Test`,
                `Scheduled Count`,
                `Present Count`,
                `Total TX FCR Count` AS `Total FCR Count`,
                `TX FCR Count` AS `Total Count`,
                `Total Staff Time (Hours)` AS `Total Staff Time Hours`,
                `Net Staff Time (Hours)` AS `Net Staff Time Hours`,
                `TP_Downtime (Hours)` AS `TP Downtime Hours`,
                `Client_Downtime (Hours)` AS `Client Downtime Hours`,
                `Quality Score` AS `Quality Score`
        FROM kpi
        WHERE Date > (current_date() - 20)
    """)

    push_gamification_data(
            kpi.toPandas(), customer.upper(), 'DD_Connection')
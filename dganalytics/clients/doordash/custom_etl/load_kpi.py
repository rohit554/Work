from dganalytics.utils.utils import get_spark_session, get_path_vars, push_gamification_data
import argparse
import pandas as pd
import os
import numpy as np

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'datagamz'
    customer = 'doordashprod'
    spark = get_spark_session(f'{tenant}_load_kpi', tenant)
    tenant_path, db_path, log_path = get_path_vars(customer)
    kpi = pd.DataFrame()
    if input_file.endswith(".xlsx"):
        kpi = pd.read_excel(os.path.join(tenant_path, "data", "raw", "kpi", input_file), engine='openpyxl')
    elif input_file.endswith(".csv"):
        kpi = pd.read_csv(os.path.join(tenant_path, "data", "raw", "kpi", input_file))
    elif input_file.endswith(".xlsb"):
        xls = pd.ExcelFile(os.path.join(tenant_path, "data", "raw", "kpi", input_file))
        
        for sheet in xls.sheet_names:
            if sheet == "Calculation":
                continue
            sheetDf = pd.read_excel(xls, sheet)
            sheetDf['Date'] = pd.to_datetime(sheetDf['Date'], unit='d', origin='1899-12-30')
            frames = [kpi, sheetDf]

            kpi = pd.concat(frames)

    kpi['Chat Handled Time (Hours)'] = np.where(kpi['Chat Handled'] == 0, np.nan, kpi['Chat Handled Time (Hours)'])
    kpi['CSAT Count'] = np.where(kpi['Total Survery'] == 0, np.nan, kpi['CSAT Count'])
    kpi['Total TX FCR Count'] = np.where(kpi['Total TX FCR Count'] == 0, np.nan, kpi['Total TX FCR Count'])
    kpi['Chat Handled'] = np.where(kpi['Chat Handled'] == 0, np.nan, kpi['Chat Handled'])
    kpi['Total Survery'] = np.where(kpi['Total Survery'] == 0, np.nan, kpi['Total Survery'])
    kpi['TX FCR Count'] = np.where(kpi['TX FCR Count'] == 0, np.nan, kpi['TX FCR Count'])
    kpi['Total Survery'] = np.where(kpi['Total Survery'] == 0, np.nan, kpi['Total Survery'])
    kpi['Quality Score'] = np.where(kpi['Quality Score'] == 0, np.nan, kpi['Quality Score']*100)
    kpi['PKT Score'] = np.where(kpi['PKT Score'] == 0, np.nan, kpi['PKT Score']*100)

    kpi = spark.createDataFrame(kpi)
    kpi.createOrReplaceTempView("kpi")

    kpi = spark.sql(f"""
        SELECT  `HRMS ID` UserID,,
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
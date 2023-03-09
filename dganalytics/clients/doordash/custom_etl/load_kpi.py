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
    if input_file.endswith(".xlsx"):
        kpi = pd.read_excel(os.path.join(tenant_path, "data", "raw", "kpi", input_file), engine='openpyxl')
    elif input_file.endswith(".csv"):
        kpi = pd.read_csv(os.path.join(tenant_path, "data", "raw", "kpi", input_file))

    kpi['Chat Handled Time (Hours)'] = np.where(kpi['Chat Handled'] == 0, np.nan, kpi['Chat Handled Time (Hours)'])
    kpi['CSAT Count'] = np.where(kpi['Total Survery'] == 0, np.nan, kpi['CSAT Count'])
    kpi['Total FCR Count'] = np.where(kpi['Total Count'] == 0, np.nan, kpi['Total FCR Count'])
    kpi['Chat Handled'] = np.where(kpi['Chat Handled'] == 0, np.nan, kpi['Chat Handled'])
    kpi['Total Survery'] = np.where(kpi['Total Survery'] == 0, np.nan, kpi['Total Survery'])
    kpi['Total Count'] = np.where(kpi['Total Count'] == 0, np.nan, kpi['Total Count'])
    kpi['Total Survery'] = np.where(kpi['Total Survery'] == 0, np.nan, kpi['Total Survery'])
    kpi['Quality - Soft Skills'] = np.where(kpi['Quality - Soft Skills'] == 0, np.nan, kpi['Quality - Soft Skills']*100)
    kpi['Quality - Execution'] = np.where(kpi['Quality - Execution'] == 0, np.nan, kpi['Quality - Execution']*100)
    kpi['PKT Score'] = np.where(kpi['PKT Score'] == 0, np.nan, kpi['PKT Score']*100)

    kpi = spark.createDataFrame(kpi)
    kpi.createOrReplaceTempView("kpi")

    kpi = spark.sql(f"""
        SELECT  `Date`,
                `HRMS ID`,
                `Chat Handled`,
                `Chat Handled Time (Hours)`,
                `Total Survery`,
                `CSAT Count`,
                `PKT Score`,
                `Scheduled Count`,
                `Present Count`,
                `Total FCR Count`,
                `Total Count`,
                `Total Staff Time (Hours)`,
                `Net Staff Time (Hours)`,
                `TP_Downtime (Hours)`,
                `Client_Downtime (Hours)`,
                `Quality - Soft Skills`,
                `Quality - Execution`
        FROM kpi
    """)

    push_gamification_data(
            kpi.toPandas(), customer, 'DD_Connection')
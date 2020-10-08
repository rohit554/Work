from dganalytics.utils.utils import get_spark_session, get_path_vars
import argparse
import pandas as pd

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'salmatcolesonline'
    spark = get_spark_session('colesonline_load_wfm', tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    print("start loading WFM data for colesonline" + input_file)

    df = pd.read_csv(
        "/dbfs/mnt/datagamz/salmatcolesonline/data/raw/verint_wfm/" + input_file)

    df = df.rename(columns={
        "Date": "Date",
        "Organisation": "Organisation",
        "Employee": "Employee",
        "Supervisor": "Supervisor",
        "Time Adhering to Schedule (Hours)": "TimeAdheringToScheduleHours",
        "Time Not Adhering to Schedule (Hours)": "TimeNotAdheringToScheduleHours",
        "Total Time Scheduled (Hours)": "TotalTimeScheduledHours",
        "Time Adhering to Schedule (%)": "TimeAdheringToSchedule",
        "Time Not Adhering to Schedule (%)": "TimeNotAdheringToSchedule",
        "Adherence Violations": "AdherenceViolations"
    }, errors="raise")
    df = df.dropDuplicates()
    df['Employee'] = df['Employee'].astype('str').str.strip()
    df = df[['Date', 'Organisation', 'Employee', 'Supervisor', 'TimeAdheringToScheduleHours',
             'TimeNotAdheringToScheduleHours', 'TotalTimeScheduledHours', 'TimeAdheringToSchedule',
             'TimeNotAdheringToSchedule', 'AdherenceViolations']]
    sdf = spark.createDataFrame(df)
    sdf.registerTempTable("wfm")
    spark.sql(f"""merge into dg_{tenant}.wfm_verint_export
                using wfm
                on wfm.Date = wfm_verint_export.Date
                and wfm.Employee = wfm_verint_export.Employee
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """)
    print("loading finished")
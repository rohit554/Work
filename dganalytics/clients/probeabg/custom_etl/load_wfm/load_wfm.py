from dganalytics.utils.utils import get_spark_session, get_path_vars
import argparse
import pandas as pd

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'probeabg'
    spark = get_spark_session('probeabg_load_wfm', tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    print("start loading WFM data for probeabg" + input_file)

    '''
    df = pd.read_csv(
        "/dbfs/mnt/datagamz/probeabg/data/raw/verint_wfm/" + input_file)
    '''
    df = pd.read_excel(
        "/dbfs/mnt/datagamz/probeabg/data/raw/verint_wfm/" + input_file, engine='openpyxl')

    df = df.rename(columns={
        "Date": "Date",
        "Employee_ID": "Employee_ID",
        "Organisation": "Organisation",
        "Employee": "Employee",
        "Username": "Username",
        "Supervisor": "Supervisor",
        "Time Adhering to Schedule (Hours)": "TimeAdheringToScheduleHours",
        "Time Not Adhering to Schedule (Hours)": "TimeNotAdheringToScheduleHours",
        "Total Time Scheduled (Hours)": "TotalTimeScheduledHours",
        "Time Adhering to Schedule (%)": "TimeAdheringToSchedule",
        "Time Not Adhering to Schedule (%)": "TimeNotAdheringToSchedule",
        "Adherence Violations": "AdherenceViolations"
    }, errors="raise")
    df = df.drop_duplicates()

    df['Employee'] = df['Employee'].astype('str').str.strip()
    df = df[['Date', 'Organisation', 'Employee', 'Supervisor', 'TimeAdheringToScheduleHours',
             'TimeNotAdheringToScheduleHours', 'TotalTimeScheduledHours', 'TimeAdheringToSchedule',
             'TimeNotAdheringToSchedule', 'AdherenceViolations', 'Employee_ID', 'Username']]
    
    sdf = spark.createDataFrame(df)
    sdf.registerTempTable("wfm_pre")
    wfm_username_match = spark.sql(f"""select distinct a.Date, a.Organisation, a.Employee, a.Supervisor, a.TimeAdheringToScheduleHours,
             a.TimeNotAdheringToScheduleHours, a.TotalTimeScheduledHours, a.TimeAdheringToSchedule,
             a.TimeNotAdheringToSchedule, a.AdherenceViolations, a.Employee_ID, a.Username,
             b.userId as genesysUserId, b.state
                            from 
                            wfm_pre a left join gpc_{tenant}.dim_users b
                            on lower(trim(a.Username)) = trim(lower(b.userName))
                """)
    wfm_username_match.registerTempTable("wfm_username_match")

    wfm_simple_match = spark.sql(f"""select * from wfm_username_match where genesysUserId is not null
                        """)

    wfm_fuzzy_match = spark.sql(f"""select 
                                distinct a.Date, a.Organisation, a.Employee, a.Supervisor, a.TimeAdheringToScheduleHours,
                                a.TimeNotAdheringToScheduleHours, a.TotalTimeScheduledHours, a.TimeAdheringToSchedule,
                                a.TimeNotAdheringToSchedule, a.AdherenceViolations, a.Employee_ID, a.Username,
                                b.userId as genesysUserId, b.state
                             from wfm_username_match a , gpc_{tenant}.dim_users b
                                        where a.genesysUserId is null and 
                                        ( concat(trim(element_at(split(trim(a.Employee), ","),2)) , ' ' , trim(element_at(split(trim(a.Employee), ","),1))) = b.userFullName 
                                            or lower(concat(trim(element_at(split(trim(a.Employee), ","),2)) , ' ' , trim(element_at(split(trim(a.Employee), ","),1)))) = lower(b.userFullName)
                                            or (concat(element_at(split(trim(element_at(split(trim(a.Employee), ","),2)), ' '),1) , ' ' , trim(element_at(split(trim(a.Employee), ","),1)))) = b.userFullName
                                            or lower(concat(trim(element_at(split(trim(a.Employee), ","),2)) , ' ' , trim(element_at(split(trim(a.Employee), ","),1)))) = lower(b.userFullName)
                                            or lower(concat(element_at(split(trim(element_at(split(trim(a.Employee), ","),2)), ' '),1) , ' ' , trim(element_at(split(trim(a.Employee), ","),1)))) = lower(b.userFullName)
                                            or lower(concat(trim(element_at(split(trim(a.Employee), ","),2)) , '.', trim(element_at(split(trim(a.Employee), ","),1)))) = lower(ELEMENT_at(split(b.userName, "@"),1))
                                            or lower(concat(element_at(split(trim(element_at(split(trim(a.Employee), ","),2)), ' '),1) , '.' , trim(element_at(split(trim(a.Employee), ","),1)))) = lower(ELEMENT_at(split(b.userName, "@"),1))
                                            or lower(concat(element_at(split(trim(element_at(split(trim(a.Employee), ","),2)), ' '),2) , '.' , trim(element_at(split(trim(a.Employee), ","),1)))) = lower(ELEMENT_at(split(b.userName, "@"),1))
                                            or lower(concat(replace(trim(element_at(split(trim(a.Employee), ","),2)), ' ', '') , ' ' , replace(trim(element_at(split(trim(a.Employee), ","),1)), ' ', ''))) = lower(b.userFullName)

                                            )
                        """)
    wfm = wfm_simple_match.union(wfm_fuzzy_match)
    wfm.registerTempTable("wfm_temp")
    wfm = spark.sql("""select 
                                    distinct Date, Organisation, Employee, Supervisor, TimeAdheringToScheduleHours,
                                TimeNotAdheringToScheduleHours, TotalTimeScheduledHours, TimeAdheringToSchedule,
                                TimeNotAdheringToSchedule, AdherenceViolations, Employee_ID, Username,
                                genesysUserId
                                     from (select *, 
                                    row_number() over(partition by Employee, `Date` order by (case when coalesce(state, ' ') = 'active' then 1 else 2 end)) as rnk 
                                    from wfm_temp
                                    ) a where rnk = 1
                                """)

    
    wfm.registerTempTable("wfm")
    spark.sql(f"""merge into dg_{tenant}.wfm_verint_export
                using wfm 
                on wfm.Date = wfm_verint_export.Date
                and trim(wfm.Employee) = trim(wfm_verint_export.Employee)
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """)
    print("loading finished")

from dganalytics.utils.utils import get_spark_session, get_path_vars, push_gamification_data

if __name__ == "__main__":
    org_id = 'airbnbprod'
    back_days = 30

    kpi = spark.sql(f"""
    WITH User_data AS (
        SELECT user_id, Emp_code, LDAP_ID, CCMS_ID, airbnb_id, date_format(Dates, "dd-MM-yyyy") DataDate
        FROM dg_airbnbprod.airbnb_user_data AS u
        CROSS JOIN (SELECT explode(sequence(current_date()-{back_days}, current_date(), interval 1 day)) AS Dates) AS D
    )

    SELECT U.user_id AS `UserID`,
        U.DataDate AS `Date`,
        coalesce(DW.Escalation_rate, '') As `Escalation Rate`,
        coalesce(DW.NPS,''),
        coalesce(DW.Reopen_Rate,'') AS `Reopen Rate`,
        coalesce(DW.Resolution_Rate,'') AS `Resolution Rate`,
        coalesce(DW.Schedule_Adherence,'') AS `Adherence Percent`,
        coalesce(DW.SPD_logged,'') AS `SPD`,
        coalesce(DW.Ticket_Occupancy,'') AS `Ticket Occupancy Percent`,
        coalesce(DW.Total_Crewbie_Love_Score,'') AS `Love Score`,
        coalesce(BS.Behavioral_Score,'') AS `Behavior Scores`, 
        CASE WHEN coalesce(C.Total_Target,'') IS NOT NULL
            THEN round(coalesce(C.Total_Login_Hrs,'') * 100 /coalesce(C.Total_Target,''), 2) 
        END `Login Percent`,
        coalesce(Attendance_Absenteesim,'')
    FROM User_data U
    LEFT JOIN dg_airbnbprod.airbnb_day_wise DW
    ON U.LDAP_ID  = DW.UserID
    AND U.DataDate = DW.`Date`
    LEFT JOIN dg_airbnbprod.day_wise_behaviour_score BS
    ON U.airbnb_id  = BS.airbnb_agent_id
    AND U.DataDate = BS.Recorded_Date
    LEFT JOIN dg_airbnbprod.ambassador_wise_conformance C
    ON U.Emp_code  = C.ECN
    AND U.DataDate = C.IST_Date
    LEFT JOIN (
            SELECT A.ECN,
                A._date,
                (count(A.Shift) - sum(A.isPresent)) * 100 / count(A.Shift) Attendance_Absenteesim
            FROM(
            SELECT last_day(to_date(IST_Date, 'dd-MM-yyyy')) _date, ECN, Shift, CASE WHEN Attendance_Status = 'P' THEN 1 ELSE 0 END isPresent
            FROM dg_airbnbprod.ambassador_wise_conformance C
            JOIN User_data U
            ON U.Emp_code = C.ECN
                AND U.DataDate = C.IST_Date  
            WHERE CAST(replace(Shift, '-', '') AS INT) iS NOT NULL
            ) AS A
            GROUP BY A.ECN, A._date
        ) conf
    ON conf.ECN = U.Emp_Code
        AND date_format(cast(conf._date as date) = U.DataDate
    WHERE NOT (
    DW.NPS IS NULL
        AND  DW.Reopen_Rate IS NULL
        AND  DW.Escalation_rate IS NULL
        AND  DW.Resolution_Rate IS NULL
        AND  DW.SPD_logged IS NULL
        AND  DW.Schedule_Adherence IS NULL
        AND  DW.Ticket_Occupancy IS NULL
        AND  DW.Total_Crewbie_Love_Score IS NULL
        AND  BS.Behavioral_Score IS NULL
        AND CASE WHEN C.Total_Target IS NOT NULL
            THEN C.Total_Login_Hrs * 100 /C.Total_Target
        END IS NULL
        AND Attendance_Absenteesim IS NULL
    )
    """)

    push_gamification_data(
                kpi.toPandas(), org_id.upper(), 'AirBnBDevConnection')
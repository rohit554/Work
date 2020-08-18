from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import parser, get_dbname, env

if __name__ == "__main__":
    tenant, run_id, extract_date = parser()
    spark = get_spark_session(
        app_name="fact_wfm_actuals", tenant=tenant, default_db=get_dbname(tenant))
    from delta.tables import DeltaTable
    actuals = spark.sql(f"""
    create temporary view actuals as 
    select userId,startDate,actualsEndDate, endDate, actuals.actualActivityCategory,actuals.endOffsetSeconds, actuals.startOffsetSeconds,
                                cast(startDate as date) startDatePart
from (
select data.userId, data.startDate, data.actualsEndDate, data.endDate,data.impact,
 explode(data.actuals) as actuals from (
select explode(data) as data from raw_wfm_adherence where extractDate = '{extract_date}')
) 
                        """)
    
    if env != 'local':
        actuals_delete = spark.sql(f"""
        delete from fact_wfm_actuals target where exists (
                                    select actuals.userId
    from actuals where actuals.userId = target.userId
                and actuals.startDate = target.startDate and cast(actuals.startDate as date) = target.startDatePart)
                                    """)
    
    actuals_update = spark.sql(f"""
                                insert into fact_wfm_actuals
								select userId,startDate,actualsEndDate, endDate, actuals.actualActivityCategory,actuals.endOffsetSeconds, actuals.startOffsetSeconds,
                                cast(startDate as date) startDatePart
from (
select data.userId, data.startDate, data.actualsEndDate, data.endDate,data.impact,
 explode(data.actuals) as actuals from (
select explode(data) as data from raw_wfm_adherence where extractDate = '{extract_date}')
)
								""")
    

from pytz import utc as tz_utc
from datetime import datetime
aggr_pipeline = [
    {
        "$match": {
            "report_date": {
                '$gte' : tz_utc.localize(datetime.strptime('2020-02-25','%Y-%m-%d') )
            }
        }
    },
    {
        "$project": {
            '_id': {
                "$toString": '$_id'
            },
            'Exception':'$exception', 
            'Agent_Name':'$agent_name', 
            'UPA':'$upa',
            'Non_UPA': '$non_upa', 
            'IEX_ID':'$iex_id', 
            'Time':'$time', 
            'Total_Time_mins':'$total_time', 
            'Shot_Date':'$report_date', 
            'User_ID':'$user_id', 
            'Break':'$break',
            'Lunch': '$lunch'
        }
    }
]
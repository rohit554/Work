from dganalytics.utils.utils import get_path_vars
from pyspark.sql import SparkSession
import os
import pandas as pd

def export_conversation_summary(spark: SparkSession, tenant: str, region: str):

    df = spark.sql(f"""
                WITH datetable AS
                (
                SELECT _date AS intervalStart, (_date + INTERVAL 15 MINUTE) IntervalEnd
                FROM (SELECT EXPLODE(dates) _date FROM
                (SELECT sequence(ADD_MONTHS(CURRENT_TIMESTAMP(), -1), CURRENT_TIMESTAMP(), Interval 15 minute) dates))
                )

                SELECT 
                        intervalStart,
                        count(1) ConversationCount
                FROM(
                    SELECT 
                        conversationId, 
                        b.Intervalstart,
                        ROW_NUMBER() OVER(PARTITION BY conversationId ORDER BY recordInsertTime DESC) AS rn
                    FROM gpc_hellofresh.raw_conversation_details a, 
                        datetable b
                    WHERE  CAST(a.conversationStart AS DATE) = CAST(b.Intervalstart AS DATE) 
                    AND a.conversationStart BETWEEN b.Intervalstart AND b.IntervalEnd
                )
                WHERE rn = 1
                GROUP BY 
                    Intervalstart
            """)

    return df
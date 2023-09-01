from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv, push_gamification_data
import os
from pyspark.sql.functions import col, date_format, to_timestamp, regexp_replace, coalesce, lit
from pyspark.sql.types import DoubleType
import pandas as pd
from datetime import datetime
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'datagamz'
    spark = get_spark_session('outbound_data', tenant)
    customer = 'skynzob'
    db_name = "dg_skynz"
    tenant_path, db_path, log_path = get_path_vars(customer)
    
    if input_file.endswith(".json"):
        outbound = pd.read_json(os.path.join(tenant_path, 'data', 'raw', 'quality_outbound_data', input_file))

    outbound = pd.DataFrame(outbound['values'].tolist())
    header = outbound.iloc[0]
    outbound = outbound[1:]
    outbound.columns = header
    outbound['orgId'] = 'skynzob'

    outbound = outbound[['EID', 'Timestamp', 'Overall Score', 'Score: Confirm Need', 'Score: Add Value','orgId']]
    outbound.rename(columns={'Overall Score': 'Overall_Score', 'Score: Confirm Need': 'Score_Confirm_Need', 'Score: Add Value': 'Score_Add_Value'}, inplace=True)

    outbound = spark.createDataFrame(outbound)

    outbound = outbound.filter(col('EID') != '#REF!')
    outbound = outbound.dropDuplicates(['Timestamp', 'EID'])

    outbound = outbound.withColumn('Overall_Score', regexp_replace('Overall_Score', '%', ''))
    outbound = outbound.withColumn('Overall_Score', col('Overall_Score').cast(DoubleType()))
    outbound = outbound.withColumn('Overall_Score', coalesce(col('Overall_Score'), lit('')))

    outbound = outbound.withColumn('Score_Confirm_Need', regexp_replace('Score_Confirm_Need', '%', ''))
    outbound = outbound.withColumn('Score_Confirm_Need', col('Score_Confirm_Need').cast(DoubleType()))
    outbound = outbound.withColumn('Score_Confirm_Need', coalesce(col('Score_Confirm_Need'), lit('')))

    outbound = outbound.withColumn('Score_Add_Value', regexp_replace('Score_Add_Value', '%', ''))
    outbound = outbound.withColumn('Score_Add_Value', col('Score_Add_Value').cast(DoubleType()))
    outbound = outbound.withColumn('Score_Add_Value', coalesce(col('Score_Add_Value'), lit('')))

    outbound = outbound.withColumn('Timestamp', date_format(to_timestamp('Timestamp', 'M/d/yyyy H:mm:ss'), 'MM-dd-yyyy HH:mm:ss'))
    outbound.createOrReplaceTempView("outbound_data")

    spark.sql(f"""MERGE into {db_name}.outbound_quality_data DB
                    USING outbound_data A
                    ON A.EID = DB.EID
                    and A.Timestamp = DB.Timestamp
                    WHEN MATCHED THEN
                    UPDATE SET *
                    WHEN NOT MATCHED THEN
                    INSERT *
                    """)

    df = spark.sql("""
                    SELECT
                    DISTINCT
                        b.Sky_ID as `userId`,
                        a.`Timestamp` AS `Date`,
                        coalesce(a.`Overall_Score`, '') AS `QA Score`,
                        coalesce(a.`Score_Confirm_Need`, '') AS `QA Score - Confirm Needs`,
                        coalesce(a.`Score_Add_Value`, '') AS `QA Score - Add Value`    
                    FROM
                        dg_skynz.outbound_quality_data a
                    JOIN
                        dg_skynz.user_data b
                    ON
                        a.EID = b.Probe_ID
                    WHERE
                        b.Sky_ID IS NOT NULL
                        AND length(trim(b.Sky_ID)) > 0
                """)
    
    push_gamification_data(
            df.toPandas(), customer.upper(), 'SKYOB_QA_Connection')
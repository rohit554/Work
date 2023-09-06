from dganalytics.utils.utils import get_spark_session, get_path_vars, export_powerbi_csv, push_gamification_data
import os
from pyspark.sql.functions import col, date_format, to_timestamp, regexp_replace, coalesce, lit
from pyspark.sql.types import DoubleType
import pandas as pd
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True)

    args, unknown_args = parser.parse_known_args()
    input_file = args.input_file

    tenant = 'skynzob'
    db_name = f"dg_{tenant}"
    spark = get_spark_session('outbound_evaluations', tenant, default_db=db_name)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    
    if input_file.endswith(".json"):
        evaluations = pd.read_json(os.path.join(tenant_path, 'data', 'raw', 'outbound_evaluations', input_file))

    else:
      raise Exception("File is not in the correct format. It should have a '.json' extension.")

    evaluations = pd.DataFrame(evaluations['values'].tolist())
    header = evaluations.iloc[0]
    evaluations = evaluations[1:]
    evaluations.columns = header
    evaluations['orgId'] = tenant

    evaluations = evaluations[['EID', 'Timestamp', 'Overall Score', 'Score: Confirm Need', 'Score: Add Value','orgId']]
    evaluations.rename(columns={'Overall Score': 'Overall_Score', 'Score: Confirm Need': 'Score_Confirm_Need', 'Score: Add Value': 'Score_Add_Value'}, inplace=True)

    evaluations = spark.createDataFrame(evaluations)

    evaluations = evaluations.filter(col('EID') != '#REF!')
    evaluations = evaluations.dropDuplicates(['Timestamp', 'EID'])

    evaluations = evaluations.withColumn('Overall_Score', regexp_replace('Overall_Score', '%', ''))
    evaluations = evaluations.withColumn('Overall_Score', col('Overall_Score').cast(DoubleType()))
    evaluations = evaluations.withColumn('Overall_Score', coalesce(col('Overall_Score'), lit('')))

    evaluations = evaluations.withColumn('Score_Confirm_Need', regexp_replace('Score_Confirm_Need', '%', ''))
    evaluations = evaluations.withColumn('Score_Confirm_Need', col('Score_Confirm_Need').cast(DoubleType()))
    evaluations = evaluations.withColumn('Score_Confirm_Need', coalesce(col('Score_Confirm_Need'), lit('')))

    evaluations = evaluations.withColumn('Score_Add_Value', regexp_replace('Score_Add_Value', '%', ''))
    evaluations = evaluations.withColumn('Score_Add_Value', col('Score_Add_Value').cast(DoubleType()))
    evaluations = evaluations.withColumn('Score_Add_Value', coalesce(col('Score_Add_Value'), lit('')))

    evaluations = evaluations.withColumn('Timestamp', date_format(to_timestamp('Timestamp', 'M/d/yyyy H:mm:ss'), 'MM-dd-yyyy HH:mm:ss'))
    evaluations.createOrReplaceTempView("outbound_evaluations")

    spark.sql(f"""MERGE into {db_name}.outbound_evaluations DB
                    USING outbound_evaluations A
                    ON A.EID = DB.EID
                    and A.Timestamp = DB.Timestamp
                    WHEN MATCHED THEN
                    UPDATE SET *
                    WHEN NOT MATCHED THEN
                    INSERT *
                    """)

    df = spark.sql(f"""
                    SELECT
                        b.Sky_ID as `userId`,
                        a.`Timestamp` AS `Date`,
                        coalesce(a.`Overall_Score`, '') AS `QA Score`,
                        coalesce(a.`Score_Confirm_Need`, '') AS `QA Score - Confirm Needs`,
                        coalesce(a.`Score_Add_Value`, '') AS `QA Score - Add Value`    
                    FROM
                        {db_name}.outbound_evaluations a
                    JOIN
                        {db_name}.users b
                    ON
                        a.EID = b.Probe_ID
                    WHERE
                        b.Sky_ID IS NOT NULL
                        AND length(trim(b.Sky_ID)) > 0
                """)
    
    push_gamification_data(df.toPandas(), customer.upper(), 'SKYOB_QA_Connection')
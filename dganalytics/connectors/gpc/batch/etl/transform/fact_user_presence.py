from pyspark.sql import SparkSession


def fact_user_presence(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    user_presence = spark.sql(f"""
        SELECT userId,
               startTime,
               endTime,
               UD.systemPresence,
               organizationPresenceId,
               pd.name presenceDefinitionName,
              pd.systemPresence presenceDefinitionSystemPresence,
              pd.deactivated presenceDefinitionDeactivated,
              pd.primary presenceDefinitionPrimary,
               coalesce(get_json_object(pd.languageLabels, '$.en'), get_json_object(pd.languageLabels, '$.en_US')) AS presenceDefinitionLabel,
               startDate,
               sourceRecordIdentifier,
               soucePartition
        FROM (
            SELECT 
                  DISTINCT userId,
                  primaryPresence.startTime startTime,
                  coalesce(primaryPresence.endTime, cast('{extract_start_time}' as timestamp)) endTime,
                  primaryPresence.systemPresence,
                  primaryPresence.organizationPresenceId,
                  CAST(primaryPresence.startTime AS date) AS startDate,
                  sourceRecordIdentifier,
                  soucePartition,
                  row_number() OVER(PARTITION BY userId,
                                      primaryPresence.startTime
                                  ORDER BY coalesce(primaryPresence.endTime, cast('2021-11-07T00:00:00Z' as timestamp)) DESC) endRank
              FROM (
                  SELECT  userId,
                          primaryPresence,
                          MAX(sourceRecordIdentifier) sourceRecordIdentifier,
                          soucePartition
                  FROM (
                      SELECT 
                          userId, 
                          explode(primaryPresence) AS primaryPresence,
                          recordIdentifier AS sourceRecordIdentifier,
                          concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) AS soucePartition
                      FROM 
                          gpc_hellofresh.raw_users_details
                      WHERE 
                          extractDate = '{extract_date}'
                          AND extractIntervalStartTime = '{extract_start_time}'
                          AND extractIntervalEndTime = '{extract_end_time}') 
                      GROUP BY userId, primaryPresence, soucePartition
                  )
        ) UD
        LEFT JOIN
            gpc_hellofresh.raw_presence_definitions AS pd
        ON
            organizationPresenceId = pd.id
        WHERE endRank = 1
    """)

    user_presence.registerTempTable("user_presence")

    spark.sql("""
                MERGE INTO fact_user_presence as target
                USING user_presence as source
                    ON  source.userId = target.userId
                        AND source.startDate = target.startDate
                        AND source.startTime = target.startTime
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
            """)

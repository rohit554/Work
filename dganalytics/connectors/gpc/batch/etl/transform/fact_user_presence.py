from pyspark.sql import SparkSession


def fact_user_presence(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    user_presence = spark.sql(f"""
        SELECT 
            DISTINCT userId,
            primaryPresence.startTime,
            coalesce(primaryPresence.endTime, cast('{extract_end_time}' as timestamp)) endTime,
            primaryPresence.systemPresence,
            primaryPresence.organizationPresenceId,
            pd.name presenceDefinitionName,
            pd.systemPresence presenceDefinitionSystemPresence,
            pd.deactivated presenceDefinitionDeactivated,
            pd.primary presenceDefinitionPrimary,
            coalesce(get_json_object(pd.languageLabels, '$.en'), get_json_object(pd.languageLabels, '$.en_US')) AS presenceDefinitionLabel,
            CAST(primaryPresence.startTime AS date) AS startDate,
            sourceRecordIdentifier,
            soucePartition
        FROM (
            SELECT 
                userId, 
                explode(primaryPresence) AS primaryPresence,
                recordIdentifier AS sourceRecordIdentifier,
                concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) AS soucePartition
            FROM 
                raw_users_details
            WHERE 
                extractDate = '{extract_date}'
                AND extractIntervalStartTime = '{extract_start_time}'
                AND extractIntervalEndTime = '{extract_end_time}'
        )
        LEFT JOIN
            raw_presence_definitions AS pd
        ON
            primaryPresence.organizationPresenceId = pd.id
    """)

    user_presence.registerTempTable("user_presence")

    spark.sql("""
        DELETE FROM 
            fact_user_presence a 
        WHERE exists (
            SELECT
                1 
            FROM 
                user_presence b 
            WHERE 
                a.userId = b.userId
                AND a.startDate = b.startDate 
                AND a.startTime = b.startTime
        )
    """)

    spark.sql("INSERT INTO fact_user_presence SELECT * FROM user_presence")

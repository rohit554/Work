from dganalytics.utils.utils import get_path_vars
from pyspark.sql import SparkSession
import os
import pandas as pd
from pyspark.sql.functions import col, unix_timestamp, lit, expr, date_trunc, explode
from pyspark.sql.functions import concat_ws, date_format, when, lead, lag
from pyspark.sql.window import Window

def export_conversation_final_segment(spark: SparkSession, tenant: str, region: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    queue_timezones = pd.read_json(
        os.path.join(tenant_path, "data", "config", "Queue_TimeZone_Mapping.json")
    )
    queue_timezones = pd.DataFrame(queue_timezones["values"].tolist())
    queue_timezones.columns = queue_timezones.iloc[0]
    queue_timezones = queue_timezones[1:]
    queue_timezones_spark = spark.createDataFrame(queue_timezones)
    queue_timezones_spark.createOrReplaceTempView("queue_timezones")

    outbound_queues_path = os.path.join(
        tenant_path, "data", "config", "HF_outbound_Queues"
    )
    outbound_queues = pd.read_json(outbound_queues_path)
    outbound_queues = pd.DataFrame(outbound_queues["values"].tolist())
    outbound_queues.columns = outbound_queues.iloc[0]
    outbound_queues = outbound_queues[1:]
    outbound_queues_spark = spark.createDataFrame(outbound_queues)
    outbound_queues_spark.createOrReplaceTempView("outbound_Queues")

    df = spark.sql(
        """
        SELECT DISTINCT conversationId,
                agentId,
                c.queueId,
                wrapUpCode,
                from_utc_timestamp(conversationStart, trim(e.timeZone)) conversationStart,
                from_utc_timestamp(conversationEnd, trim(e.timeZone)) conversationEnd,
                DATE(from_utc_timestamp(conversationStart, trim(e.timeZone))) AS conversationDate,
                originatingDirection,
                mediaType
    FROM (SELECT conversationId,
                agentId,
                co.queueId,
                wrapUpCode,
                conversationStart,
                conversationEnd,
                originatingDirection,
                mediaType,
                row_number() OVER(PARTITION BY co.conversationId ORDER BY co.sessionEnd DESC) rn
    FROM gpc_hellofresh.dim_conversations co
    WHERE conversationStartDate >= date_sub(current_date(), 5)
    AND co.wrapUpCode IS NOT NULL) C
    JOIN gpc_hellofresh.dim_routing_queues q 
        ON c.queueId = q.queueId
    JOIN queue_timezones e 
        ON q.queueName = e.queueName
    JOIN outbound_Queues oq 
        ON oq.`Queue Name` = q.queueName
    WHERE rn = 1

    UNION ALL

    SELECT DISTINCT conversationId,
                agentId,
                c.queueId,
                wrapUpCode,
                from_utc_timestamp(conversationStart, trim(e.timeZone)) conversationStart,
                from_utc_timestamp(conversationEnd, trim(e.timeZone)) conversationEnd,
                DATE(from_utc_timestamp(conversationStart, trim(e.timeZone))) AS conversationDate,
                originatingDirection,
                mediaType
    FROM
    (SELECT conversationId,
        agentId,
        co.queueId,
        wrapUpCode,
        conversationStart,
        conversationEnd,
        originatingDirection,
        mediaType,
        row_number() OVER (PARTITION BY co.conversationId ORDER BY co.sessionEnd DESC) rn
    FROM gpc_hellofresh.dim_conversations co
    WHERE conversationStartDate >= date_sub(current_date(), 5)
    and co.conversationId in (SELECT conversationId FROM gpc_hellofresh.dim_conversations
    WHERE conversationStartDate >= date_sub(current_date(), 5)
    GROUP BY conversationId
    having count(wrapUpCode) = 0))
    C
    JOIN gpc_hellofresh.dim_routing_queues q 
        ON c.queueId = q.queueId
    JOIN queue_timezones e 
        ON q.queueName = e.queueName
    JOIN outbound_Queues oq 
        ON oq.`Queue Name` = q.queueName
    where rn = 1
    """
    )

    # Adding time intervals
    df = df.withColumn("end", date_trunc("hour", df["conversationEnd"]))
    df = df.withColumn("start", date_trunc("hour", df["conversationstart"]))
    df = df.withColumn("diff", (unix_timestamp("end") - unix_timestamp("start")) / 3600)
    df = df.withColumn("hour_interval", expr("sequence(0, cast(diff as int))"))

    df = df.withColumn("hour_interval", explode("hour_interval"))

    df = df.withColumn(
        "new_start", expr("start + make_interval(0, 0, 0, 0, hour_interval + 1, 0, 0)")
    )

    window_spec = Window.partitionBy("conversationId").orderBy("new_start")

    df = df.withColumn("next_new_start", lead("new_start").over(window_spec))

    df = df.withColumn("prev_new_start", lag("new_start").over(window_spec))

    df = df.withColumn(
        "TimeIntervalStart",
        when(
            col("hour_interval") == 0,
            concat_ws(
                " - ",
                date_format(col("start"), "HH:mm:ss"),
                date_format(col("new_start"), "HH:mm:ss"),
            ),
        )
        .when(
            col("next_new_start").isNull(),
            concat_ws(
                " - ",
                date_format(col("prev_new_start"), "HH:mm:ss"),
                date_format(col("new_start"), "HH:mm:ss"),
            ),
        )
        .otherwise(
            concat_ws(
                " - ",
                date_format(col("new_start"), "HH:mm:ss"),
                date_format(col("next_new_start"), "HH:mm:ss"),
            )
        ),
    )
    df = df.drop(
        "conversationEnd",
        "hour_interval",
        "start",
        "end",
        "diff",
        "new_start",
        "next_new_start",
        "prev_new_start",
    )

    # Delete old data from the table for the last 5 days before inserting new data
    spark.sql(
        f"""
        DELETE FROM pbi_hellofresh.conversation_final_segment 
        WHERE conversationDate >= date_sub(current_date(), 5)
    """
    )

    # Write new data to the target table
    df.write.mode("append").saveAsTable("pbi_hellofresh.conversation_final_segment")

    return spark.sql(
        """SELECT * FROM pbi_hellofresh.conversation_final_segment
                        WHERE conversationDate > add_months(current_date(), -12)"""
    )
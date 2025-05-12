from dganalytics.utils.utils import get_path_vars
from pyspark.sql import SparkSession
import os
import pandas as pd


def dim_last_handled_conversation(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    df = spark.sql(
        """
        SELECT conversationId,
                conversationStart,
                conversationEnd,
                originatingDirection,
                agentId userId,
                mediaType,
                c.queueId,
                wrapUpCode wrapUpCodeId,
                CAST(conversationStart AS DATE) AS conversationStartDate
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
    WHERE conversationStartDate >= date_sub(current_date(), 3)
    AND co.wrapUpCode IS NOT NULL) C
    WHERE rn = 1

    UNION ALL

    SELECT conversationId,
    conversationStart,
                conversationEnd,
                originatingDirection,
                agentId userId,
                mediaType,
                c.queueId,
                wrapUpCode wrapUpCodeId,
                CAST(conversationStart AS DATE) AS conversationStartDate
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
    WHERE conversationStartDate >= date_sub(current_date(), 3)
    and co.conversationId in (SELECT conversationId FROM gpc_hellofresh.dim_conversations
    WHERE conversationStartDate >= date_sub(current_date(), 3)
    GROUP BY conversationId
    having count(wrapUpCode) = 0))
     C
    where rn = 1
    """
    )

    print(f"Number of Rows to Insert: {df.count()}")

    # Delete old data from the table for the last 5 days before inserting new data
    deleted_df = spark.sql(f"""
        DELETE FROM gpc_hellofresh.dim_last_handled_conversation
        WHERE conversationStartDate >= date_sub(current_date(), 3)
    """)
    print(f"Number of Rows Deleted: {deleted_df.collect()[0]['num_affected_rows']}")
    # Write new data to the target table
    inserted_df = df.write.mode("append").saveAsTable("gpc_hellofresh.dim_last_handled_conversation")
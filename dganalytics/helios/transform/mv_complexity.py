import pandas as pd
from collections import defaultdict
from pyspark.sql.window import Window
from pyspark.sql.functions import col, max

def steps_config(spark, tenant):
    df = spark.sql(
        f"SELECT stepName, counterStep, value FROM dgdm_{tenant}.step_config"
    ).toPandas()
    steps = df.set_index("stepName").to_dict(orient="index")
    return steps

def analyze_conversation(conv_data: pd.DataFrame, step_mapping) -> pd.DataFrame:
    expected_steps = list(step_mapping.keys())
    results = []

    records = conv_data.to_dict("records")
    step_counts = defaultdict(int)
    steps_done = {step: False for step in expected_steps}
    current_hold = 0
    current_transfer = 0
    current_violation = 0
    distinct_violation_steps = set()  # Track distinct violation steps
    last_expected_step = None
    
    last_steps_by_conv = {}
    last_before_close_by_conv = {}
    
    for conv_id, group in conv_data.groupby("conversationId"):
        expected_steps_in_conv = group[group["stepName"].isin(expected_steps)]
        
        if not expected_steps_in_conv.empty:
            last_step = expected_steps_in_conv.iloc[-1]["stepName"]
            last_steps_by_conv[conv_id] = last_step
        
        close_steps = expected_steps_in_conv[expected_steps_in_conv["stepName"] == "Close and Offer Additional Assistance"]
        if not close_steps.empty:
            first_close_time = close_steps.iloc[0]["startTime"]
            steps_before_close = expected_steps_in_conv[expected_steps_in_conv["startTime"] < first_close_time]
            if not steps_before_close.empty:
                last_before = steps_before_close.iloc[-1]["stepName"]
                last_before_close_by_conv[conv_id] = last_before
    
    for i, row in enumerate(records):
        step = row["stepName"]
        duration = (row["endTime"] - row["startTime"]).total_seconds()
        sentiment = row.get("sentiment", 0)
        conv_id = row["conversationId"]

        if step in expected_steps:
            if last_expected_step:
                # Find the last result for this step and update its metrics
                for result in reversed(results):
                    if result['steps'] == last_expected_step:
                        result['hold'] = current_hold
                        result['transfer'] = current_transfer
                        result['violation'] = current_violation
                        result['distinct_violation_count'] = len(distinct_violation_steps)
                        break
            current_hold = 0
            current_transfer = 0
            current_violation = 0
            distinct_violation_steps = set()
            last_expected_step = step
            step_counts[step] += 1
            steps_done[step] = True
            results.append(
                {
                    "conversation_id": conv_id,
                    "steps": step,
                    "step_count": step_counts[step],
                    "step_done": 1 if int(step_counts[step]) > 0 else 0,
                    "hold": current_hold,
                    "transfer": current_transfer,
                    "violation": current_violation,
                    "distinct_violation_count": len(distinct_violation_steps),
                    "next_step_expected": int(i != len(records) - 1 and records[i + 1]["stepName"] in expected_steps),
                    "duration": duration,
                    "sentiment": sentiment,
                    "counter_step": step_mapping[step]["counterStep"],
                    "value": step_mapping[step]["value"],
                    "conversationStartDateId": row["conversationStartDateId"],
                    "is_last_step": step == last_steps_by_conv.get(conv_id, None),
                    "is_last_before_close": step == last_before_close_by_conv.get(conv_id, None),
                }
            )
        elif step == 'Hold':
            current_hold += 1
        elif step == 'Transfer':
            current_transfer += 1
        elif step not in ['Hold', 'Transfer']:
            current_violation += 1
            distinct_violation_steps.add(step)
    
    for step in expected_steps:
        if not steps_done[step]:
            results.append(
                {
                    "conversation_id": records[0]["conversationId"],
                    "steps": step,
                    "step_count": 0,
                    "step_done": 0,
                    "hold": 0,
                    "transfer": 0,
                    "violation": 0,
                    "distinct_violation_count": 0,
                    "next_step_expected": 0,
                    "duration": 0,
                    "sentiment": 0,
                    "counter_step": step_mapping[step]["counterStep"],
                    "value": step_mapping[step]["value"],
                    "conversationStartDateId": records[0]["conversationStartDateId"],
                    "is_last_step": False,
                    "is_last_before_close": False,
                }
            )
    df_first = pd.DataFrame(results)

    # Second Iteration - Aggregate results
    df_second = df_first.groupby(['conversation_id', 'steps']).agg({
        'hold': 'sum',
        'transfer': 'sum',
        'violation': 'sum',
        'step_count': 'sum',
        'step_done': 'max',
        'next_step_expected': 'mean',
        'duration': 'sum',
        'distinct_violation_count': 'sum',
        'sentiment': 'first',
        'counter_step': 'first',
        'value': 'first',
        'conversationStartDateId': 'first',
        'is_last_step': 'max',
        'is_last_before_close': 'max',
    }).reset_index()
    return df_second

def process_conversations(spark, tenant, extract_date):
    step_mapping = steps_config(spark, tenant)

    df = spark.sql(
        f"""
            select 
                a.conversationId,
                a.conversationStartDateId,
                coalesce(a.startTime,a.endTime) startTime,
                coalesce(a.endTime,a.startTime) endTime,
                b.label stepName,
                coalesce(cast(a.sentiment as int),0) sentiment 
            from dgdm_{tenant}.fact_conversation_map a
            join dgdm_{tenant}.label_classification b
                on a.summary = b.phrase 
                and b.type = 'conversation_map' 
                and b.label <> '' 
                and b.label is not null  
                and (a.startTime is not null and a.endTime is not null)
            join dgdm_{tenant}.dim_date d 
                on d.dateVal = cast('{extract_date}' as date)
                and d.dateId = a.conversationStartDateId

            ORDER BY conversationId, startTime"""
    )
    # df.display()

    schema = """
        conversation_id STRING,
        steps STRING,
        step_count INT,
        step_done INT,
        hold INT,
        transfer INT,
        violation INT,
        distinct_violation_count INT,
        next_step_expected INT,
        duration DOUBLE,
        sentiment DOUBLE,
        counter_step INT,
        value DOUBLE,
        conversationStartDateId INT,
        is_last_step BOOLEAN,
        is_last_before_close BOOLEAN
    """

    df_results = df.groupBy("conversationId").applyInPandas(
        lambda group: analyze_conversation(group, step_mapping), schema=schema
    )

    # Define window specification partitioned by conversation_id and ordered by existing row order
    window_spec = Window.partitionBy("conversation_id").orderBy("conversation_id").rowsBetween(Window.unboundedPreceding, 0)

    # Apply a cumulative max operation to carry forward '1' once it's encountered
    df_results = df_results.withColumn("sentiment", max(col("sentiment")).over(window_spec))
    # df_results.display()
    df_results.createOrReplaceTempView("df_results")
    spark.sql(
        f"""
            delete from dgdm_{tenant}.mv_complexity a 
              where exists (select 1 from df_results b where a.conversation_id = b.conversation_id and a.steps = b.steps and a.conversationStartDateId = b.conversationStartDateId)
            """
    )
    spark.sql(
        f"""
            insert into dgdm_{tenant}.mv_complexity
            select * from df_results
            """
    )

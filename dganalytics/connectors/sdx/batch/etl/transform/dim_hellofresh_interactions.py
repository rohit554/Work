from pyspark.sql import SparkSession
from pyspark.sql.functions import first


def dim_hellofresh_interactions(spark: SparkSession, extract_date, extract_start_time, extract_end_time, tenant):
    interaction_list = spark.sql(f"""
                        select agent_id, call_type, completed_at,created_at, email, external_ref,
                            interaction_id, restricted, scheduled_at, 
                                  statusDescription, survey_id, survey_type, updated_at,
                                  respondent_language, interaction_data, responses,
                                  created_at_part
                             from (
                        select agent_id, call_type, completed_at,created_at, email, external_ref,
                                        id as interaction_id, restricted, scheduled_at, 
                                  status as statusDescription, survey_id, survey_type, updated_at,
                                  respondent_language, interaction_data, responses,
                                  cast(created_at as date) as created_at_part,
                                  row_number() over(partition by id order by updated_at desc) as rn
                               from raw_interactions
                               where extractDate = '{extract_date}'
                            and  extractIntervalStartTime = '{extract_start_time}'
                             and extractIntervalEndTime = '{extract_end_time}' 
                             ) where rn = 1""")
    interaction_list.createOrReplaceTempView("interaction_list")
    interaction_data = spark.sql(
        """select col.* from (select explode(interaction_data) from interaction_list)""")

    interaction_data = interaction_data.groupby('interaction_id').pivot("custom_key",
                                                                        ['Country', 'AgentEmail', 'AgentGroup',
                                                                         'ContactChannel',
                                                                         'WrapUpName']).agg(first('custom_value'))
    questions = spark.sql(
        "select id as question_id, question_label, max_response from raw_questions")
    responses = spark.sql(
        """select col.* from (select explode(responses) from interaction_list)""")
    responses = responses.join(questions, on=['question_id', 'question_label'])
    responses = responses.groupby('interaction_id').pivot('question_label', ['Comments', 'CSAT',
                                                                             'Improvement Categories', 'Agent CSAT',
                                                                             'FCR', 'CC NPS', 'CES', 'Self-serve CSAT',
                                                                             'Open text', 'US CSAT']
                                                          ).agg(first('response'),
                                                                first('max_response'))
    responses = responses.selectExpr("interaction_id", "substring(replace(replace(replace(replace(`Comments_first(response)`, '\\n', ' '), '""',''), '\\r', ' ') , '\\r\\n', ' '),0,3900) as Comments", "`CSAT_first(response)` as csat",
                                     "`CSAT_first(max_response)` as csatMaxResponse", "`Improvement Categories_first(response)` as improvementCategories",
                                     "`Agent CSAT_first(response)` as agentCsat", "`Agent CSAT_first(max_response)` as agentCsatMaxResponse", "`FCR_first(response)` as fcr",
                                     "`FCR_first(max_response)` as fcrMaxResponse", "`CC NPS_first(response)` as nps", "`CC NPS_first(max_response)` as npsMaxResponse",
                                     "`CES_first(response)` as ces", "`CES_first(max_response)` as cesMaxResponse",
                                     "substring(replace(replace(replace(replace(`Open text_first(response)`, '\\n', ' '), '""',''), '\\r', ' ') , '\\r\\n', ' '), 0, 3900) as openText",
                                     "`Self-serve CSAT_first(response)` as selServerCsat", "`Self-serve CSAT_first(max_response)` as selServerCsatMaxResponse",
                                     "`US CSAT_first(response)` as usCsat", "`US CSAT_first(max_response)` as usCsatMaxResponse")
    interactions = interaction_list.join(interaction_data, on=[
                                         'interaction_id'], how="left").join(responses, on=['interaction_id'], how='left')

    interactions = interactions.drop("interaction_data", "responses")

    '''
    spark.sql("""delete from dim_hellofresh_interactions a where exists (
                        select 1 from interactions b
                                where a.surveyId = b.interaction_id
                                and a.surveySentDatePart = b.scheduled_at
    )""")
    '''

    interactions.createOrReplaceTempView("temp_interactions")
    # insert into dim_hellofresh_interactions
    interactions = spark.sql("""
            select
                interaction_id surveyId,
                agent_id agentId,
                call_type callType,
                cast((case when completed_at = '-0001-11-30 00:00:00' then NULL else completed_at end) as timestamp) surveyCompletionDate,
                cast((case when created_at = '-0001-11-30 00:00:00' then NULL else created_at end) as timestamp) createdAt,
                email email,
                external_ref externalRef,
                external_ref conversationId,
                external_ref conversationKey,
                NULL queueKey,
                NULL userKey,
                trim(lower(case when trim(lower(contactChannel)) = 'web' then 'chat'  else contactChannel end)) mediaType,
                NULL wrapUpCodeKey,
                restricted restricted,
                cast((case when scheduled_at = '-0001-11-30 00:00:00' then NULL else scheduled_at end) as timestamp) surveySentDate,
                statusDescription statusDescription,
                split(statusDescription, ' ')[0] status,
                survey_id surveyTypeId,
                survey_type surveyType,
                cast((case when updated_at = '-0001-11-30 00:00:00' then NULL else updated_at end) as timestamp) updatedAt,
                respondent_language respondentLanguage,
                Country country,
                AgentEmail agentEmail,
                AgentGroup agentGroup,
                ContactChannel contactChannel,
                WrapUpName wrapUpName,
                Comments comments,
                csat OcsatAchieved,
                csatMaxResponse OcsatMax,
                improvementCategories improvement_categories,
                (case when call_type like 'US%' then usCsat else agentCsat end) csatAchieved,
                (case when call_type like 'US%' then usCsatMaxResponse else agentCsatMaxResponse end) csatMax,
                fcr fcr,
                fcrMaxResponse fcrMaxResponse,
                nps npsScore,
                npsMaxResponse npsMaxResponse,
                ces ces,
                cesMaxResponse cesMaxResponse,
                openText openText,
                selServerCsat selServerCsat,
                selServerCsatMaxResponse selServerCsatMaxResponse,
                usCsat usCsat,
                usCsatMaxResponse usCsatMaxResponse,
                NULL originatingDirection,
                cast((case when scheduled_at = '-0001-11-30 00:00:00' then NULL else scheduled_at end) as date) surveySentDatePart
             from temp_interactions""")
    interactions.createOrReplaceTempView("interactions")

    spark.sql("""
            merge into dim_hellofresh_interactions as target  
                using interactions as source 
                on source.surveyId = target.surveyId
                and source.surveySentDatePart = target.surveySentDatePart
                when matched then update set *
                when not matched then insert *
            """)

    spark.sql(f"""
            merge into dim_hellofresh_interactions as target
                using gpc_hellofresh.dim_wrapup_codes as source
                    on source.wrapupCode = target.wrapUpName
                    and target.wrapUpCodeKey is null
                    and target.surveySentDatePart >= (cast('{extract_date}' as date) - 35)
                when matched then 
                    update set target.wrapUpCodeKey = source.wrapupId
            """)
    spark.sql(f"""
            merge into dim_hellofresh_interactions as target
                using gpc_hellofresh.dim_users as source
                    on source.userEmail = target.agentEmail
                    and target.userKey is null
                    and target.surveySentDatePart >= (cast('{extract_date}' as date) - 35)
                when matched then 
                    update set target.userKey = source.userId
            """)
    spark.sql(f"""
            merge into dim_hellofresh_interactions as target
                using gpc_hellofresh.dim_users as source
                    on split(source.userEmail, '@')[0] = split(target.agentEmail, '-')[1]
                    and target.userKey is null
                    and target.surveySentDatePart >= (cast('{extract_date}' as date) - 35)
                when matched then 
                    update set target.userKey = source.userId
            """)

    spark.sql(f"""
            merge into dim_hellofresh_interactions as target
                using gpc_hellofresh.dim_routing_queues as source
                    on source.queueName = target.callType
                    and target.queueKey is null
                    and target.surveySentDatePart >= (cast('{extract_date}' as date) - 35)
                when matched then 
                    update set target.queueKey = source.queueId
            """)
    
    spark.sql(f"""
            merge into dim_hellofresh_interactions as target
                using (select distinct conversationId, originatingDirection from gpc_hellofresh.dim_conversations 
                        where conversationStartDate >= (cast('{extract_date}' as date) - 40)
                        ) as source
                    on source.conversationId = target.conversationId
                    and target.originatingDirection is null
                    and target.surveySentDatePart >= (cast('{extract_date}' as date) - 35)
                when matched then
                    update set target.originatingDirection = source.originatingDirection
            """)

    return True

from dganalytics.utils.utils import get_spark_session, flush_utils
from dganalytics.connectors.niceincontact.niceincontact_utils import transform_parser, get_dbname, niceincontact_utils_logger
from dganalytics.connectors.niceincontact.batch.etl import transform

transform_to_method = {
    "dim_agents": transform.dim_agents,
    "dim_agents_skills": transform.dim_agents_skills,
    "fact_agents_skills": transform.fact_agents_skills,
    "fact_agents_performance": transform.fact_agents_performance,
    "fact_agents_interaction_history": transform.fact_agents_interaction_history,
    "dim_teams": transform.dim_teams,
    "fact_teams_performance": transform.fact_teams_performance,
    "dim_skills": transform.dim_skills,
    "fact_skills_summary": transform.fact_skills_summary,
    "fact_skills_sla_summary": transform.fact_skills_sla_summary,
    "dim_dispositions": transform.dim_dispositions,
    "dim_dispositions_skills": transform.dim_dispositions_skills,
    "dim_contacts": transform.dim_contacts,
    "fact_contacts": transform.fact_contacts,
    "dim_contacts_completed": transform.dim_contacts_completed,
    "fact_contacts_completed": transform.fact_contacts_completed,
    "dim_contacts_custom_data": transform.dim_contacts_custom_data,
    "fact_media_playback_contacts": transform.fact_media_playback_contacts,
}

# "fact_segments_analyzed": transform.fact_segments_analyzed,
#     "fact_wfm_agents": transform.fact_wfm_agents,
#     "fact_wfm_agents_schedule_adherence": transform.fact_wfm_agents_schedule_adherence,
#     "fact_wfm_agents_scorecards": transform.fact_wfm_agents_scorecards,
#     "fact_wfm_agents_schedule_adherence": transform.fact_wfm_agents_schedule_adherence,
#     "fact_wfm_skills_contacts": transform.fact_wfm_skills_contacts,


if __name__ == "__main__":
    tenant, run_id, extract_date, extract_start_time, extract_end_time, transformation = transform_parser()
    spark = get_spark_session(
        app_name=transformation, tenant=tenant, default_db=get_dbname(tenant))

    logger = niceincontact_utils_logger(tenant, transformation)
    try:
        logger.info(f"Applying transformation {transformation}")
        transform_to_method[transformation](spark, extract_date, extract_start_time, extract_end_time)
    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
    finally:
        flush_utils(spark, logger)

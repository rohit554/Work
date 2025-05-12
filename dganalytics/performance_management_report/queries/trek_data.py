from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_active_organization_timezones
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime, timedelta, timezone

schema = StructType(
    [
        StructField(
            "team_id", StructType([StructField("oid", StringType(), True)]), True
        ),
        StructField("_id", StructType([StructField("oid", StringType(), True)]), True),
        StructField(
            "agent_id", StructType([StructField("oid", StringType(), True)]), True
        ),
        StructField(
            "team_leader_id", StructType([StructField("oid", StringType(), True)]), True
        ),
        StructField(
            "kpi_id", StructType([StructField("oid", StringType(), True)]), True
        ),
        StructField("kpi_name", StringType(), True),
        StructField(
            "campaign_id", StructType([StructField("oid", StringType(), True)]), True
        ),
        StructField("trek_global_state", IntegerType(), True),
        StructField("trek_date", StringType(), True),
        StructField("discoveryStart", StringType(), True),
        StructField("discoveryEnd", StringType(), True),
        StructField("actionLMSStart", StringType(), True),
        StructField("actionLMSEnd", StringType(), True),
        StructField("actionLMSLink", StringType(), True),
        StructField("actionGoalTarget", StringType(), True),
        StructField("actionArticleStart", StringType(), True),
        StructField("actionArticleEnd", StringType(), True),
        StructField("actionArticleLink", StringType(), True),
        StructField("actionRecordedCallStart", StringType(), True),
        StructField("actionRecordedCallEnd", StringType(), True),
        StructField("actionRecordedCallLink", StringType(), True),
        StructField("actionOneOnOneStart", StringType(), True),
        StructField("actionOneOnOneEnd", StringType(), True),
        StructField("actionOneOnOneLink", StringType(), True),
        StructField("actionState", StringType(), True),
        StructField("scoreLMS", IntegerType(), True),
        StructField("scoreArticle", IntegerType(), True),
        StructField("scoreRecordedCall", IntegerType(), True),
        StructField("scoreOneOnOne", IntegerType(), True),
        StructField("action_complete", StringType(), True),
        StructField("feedback_given", StringType(), True),
        StructField("action_start", StringType(), True),
        StructField("discovery_complete", StringType(), True),
        StructField("discovery_start", StringType(), True),
        StructField("action_completed", StringType(), True),
        StructField("organization_id", StringType(), True),
        StructField("Survey_question", StringType(), True),
        StructField("Survey_value", StringType(), True),
        StructField("Survey_score", StringType(), True),
        StructField("Survey_type", StringType(), True),
        StructField("Feedback_question", StringType(), True),
        StructField("Feedback_answer", StringType(), True),
        StructField("Feedeback_value", StringType(), True),
        StructField("actionLMSState", StringType(), True),
        StructField("actionOneOnOneState", StringType(), True),
        StructField("actionArticleState", StringType(), True),
        StructField("actionActionState", StringType(), True),
        StructField("actionRecordedCallState", StringType(), True),
        StructField("teamlead_name", StringType(), True),
    ]
)

def get_trek_data(spark):
    extract_start_time = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z"

    for org_timezone in get_active_organization_timezones(spark).rdd.collect():
        org_id = org_timezone["org_id"]
        org_timezone = org_timezone["timezone"]
        print(org_id, " ", org_timezone)
        pipeline = [
            {
                "$match": {
                    "organization_id": org_id,
                    "$expr": {
                        "$gte": [
                            "$updatedAt",
                            {
                                "$dateFromString": {
                                    "dateString": extract_start_time,
                                    "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                                }
                            },
                        ]
                    },
                }
            },
            {
                "$project": {
                    "team_id": 1,
                    "agent_id": 1,
                    "team_leader_id": 1,
                    "kpi_name": 1,
                    "kpi_id": 1,
                    "campaign_id": 1,
                    "trek_global_state": 1,
                    "actionGoalTarget": "$action.goal.goal_target",
                    "trek_date": 1,
                    "discoveryStart": "$discovery.start",
                    "discoveryEnd": "$discovery.end",
                    "actionLMSStart": "$action.lms.start",
                    "actionLMSEnd": "$action.lms.end",
                    "actionLMSLink": "$action.lms.link",
                    "actionArticleStart": "$action.article.start",
                    "actionArticleEnd": "$action.article.end",
                    "actionArticleLink": "$action.article.link",
                    "actionRecordedCallStart": "$action.recorded_call.start",
                    "actionRecordedCallEnd": "$action.recorded_call.end",
                    "actionRecordedCallLink": "$action.recorded_call.link",
                    "actionOneOnOneStart": "$action.one_on_one.start",
                    "actionOneOnOneEnd": "$action.one_on_one.end",
                    "actionOneOnOneLink": "$action.one_on_one.link",
                    "actionLMSState": "$action.lms.state",
                    "actionOneOnOneState": "$action.one_on_one.state",
                    "actionArticleState": "$action.article.state",
                    "actionActionState": "$action.action.state",
                    "actionRecordedCallState": "$action.recorded_call.state",
                    "actionState": "$action.state",
                    "scoreLMS": "$score.lms",
                    "scoreArticle": "$score.article",
                    "scoreRecordedCall": "$score.recorded_call",
                    "scoreOneOnOne": "$score.one_on_one",
                    "action_complete": 1,
                    "feedback_given": 1,
                    "action_start": 1,
                    "discovery_complete": 1,
                    "discovery_start": 1,
                    "action_completed": 1,
                    "organization_id": 1,
                    "Survey": "$discovery.survey",
                    "nfeedback": 1,
                }
            },
            {
                "$lookup": {
                    "from": "User",
                    "localField": "team_id",
                    "foreignField": "works_for.team_id",
                    "as": "TeamLead",
                }
            },
            {"$unwind": {"path": "$TeamLead", "preserveNullAndEmptyArrays": True}},
            {
                "$unwind": {
                    "path": "$TeamLead.works_for",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {"$match": {"TeamLead.works_for.role_id": "Team Lead"}},
            {"$unwind": {"path": "$Survey", "preserveNullAndEmptyArrays": True}},
            {"$unwind": {"path": "$nfeedback", "preserveNullAndEmptyArrays": True}},
            {
                "$project": {
                    "teamlead_name": "$TeamLead.name",
                    "team_id": 1,
                    "agent_id": 1,
                    "team_leader_id": 1,
                    "kpi_name": 1,
                    "kpi_id": 1,
                    "campaign_id": 1,
                    "trek_global_state": 1,
                    "trek_date": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": "$trek_date",
                            "timezone": org_timezone,
                        }
                    },
                    "discoveryStart": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": {
                                "$convert": {"input": "$discoveryStart", "to": "date"}
                            },
                            "timezone": org_timezone,
                        }
                    },
                    "discoveryEnd": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": {
                                "$convert": {"input": "$discoveryEnd", "to": "date"}
                            },
                            "timezone": org_timezone,
                        }
                    },
                    "actionLMSStart": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": {
                                "$convert": {"input": "$actionLMSStart", "to": "date"}
                            },
                            "timezone": org_timezone,
                        }
                    },
                    "actionLMSEnd": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": {
                                "$convert": {"input": "$actionLMSEnd", "to": "date"}
                            },
                            "timezone": org_timezone,
                        }
                    },
                    "actionArticleLink": 1,
                    "actionGoalTarget": 1,
                    "actionRecordedCallStart": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": {
                                "$convert": {
                                    "input": "$actionRecordedCallStart",
                                    "to": "date",
                                }
                            },
                            "timezone": org_timezone,
                        }
                    },
                    "actionRecordedCallEnd": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": {
                                "$convert": {
                                    "input": "$actionRecordedCallEnd",
                                    "to": "date",
                                }
                            },
                            "timezone": org_timezone,
                        }
                    },
                    "actionRecordedCallLink": 1,
                    "actionOneOnOneStart": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": {
                                "$convert": {
                                    "input": "$actionOneOnOneStart",
                                    "to": "date",
                                }
                            },
                            "timezone": org_timezone,
                        }
                    },
                    "actionOneOnOneEnd": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": {
                                "$convert": {
                                    "input": "$actionOneOnOneEnd",
                                    "to": "date",
                                }
                            },
                            "timezone": org_timezone,
                        }
                    },
                    "actionOneOnOneLink": 1,
                    "actionState": 1,
                    "scoreLMS": 1,
                    "scoreArticle": 1,
                    "scoreRecordedCall": 1,
                    "scoreOneOnOne": 1,
                    "action_complete": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": "$action_complete",
                            "timezone": org_timezone,
                        }
                    },
                    "feedback_given": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": "$feedback_given",
                            "timezone": org_timezone,
                        }
                    },
                    "action_start": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": "$action_start",
                            "timezone": org_timezone,
                        }
                    },
                    "discovery_complete": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": "$discovery_complete",
                            "timezone": org_timezone,
                        }
                    },
                    "discovery_start": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": "$discovery_start",
                            "timezone": org_timezone,
                        }
                    },
                    "action_completed": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%SZ",
                            "date": "$action_completed",
                            "timezone": org_timezone,
                        }
                    },
                    "organization_id": 1,
                    "_id": 1,
                    "Survey_question": "$Survey.question",
                    "Survey_value": "$Survey.value",
                    "Survey_score": "$Survey.score",
                    "Survey_type": "$Survey.type",
                    "Feedback_question": "$nfeedback.question",
                    "Feedback_answer": "$nfeedback.answer",
                    "Feedeback_value": "$nfeedback.score",
                    "actionLMSState": 1,
                    "actionOneOnOneState": 1,
                    "actionArticleState": 1,
                    "actionActionState": 1,
                    "actionRecordedCallState": 1,
                }
            },
        ]

        df = exec_mongo_pipeline(spark, pipeline, "TrekData", schema)
        if df.count() > 0:
            df.createOrReplaceTempView("trek_data")
            spark.sql(
                f"""
                        delete from dg_performance_management.trek_data a where exists (
                            select 1 from trek_data b 
                                where b._id.oid = a.trek_id
                                    and b.team_id.oid = a.team_id
                                    and b.agent_id.oid = a.agent_id
                                    and b.team_leader_id.oid = a.team_leader_id
                                    and b.campaign_id.oid = a.campaign_id
                                    and b.kpi_id.oid = a.kpi_id
                                    and b.trek_date = a.trek_date
                                    and lower(b.organization_id) = a.orgId
                        )
                        """
            )

            spark.sql(
                f"""
                    insert into dg_performance_management.trek_data 
                        (
                            trek_id, team_id, agent_id, team_leader_id, kpi_name, kpi_id, campaign_id, 
                            trek_global_state, trek_date, discoveryStart, discoveryEnd, 
                            actionLMSStart, actionLMSEnd, actionLMSLink, 
                            actionArticleStart, actionArticleEnd, actionArticleLink, 
                            actionRecordedCallStart, actionRecordedCallEnd, actionRecordedCallLink, 
                            actionOneOnOneStart, actionOneOnOneEnd, actionOneOnOneLink, 
                            actionState, scoreLMS, scoreArticle, scoreRecordedCall, scoreOneOnOne, 
                            action_complete, feedback_given, action_start, discovery_complete, 
                            discovery_start, action_completed, orgId, actionGoalTarget, 
                            Survey_question, Survey_value, Survey_score, Survey_type, 
                            Feedback_question, Feedback_answer, Feedeback_value, 
                            actionLMSState, actionOneOnOneState, actionArticleState, 
                            actionActionState, actionRecordedCallState, teamlead_name
                        )
                        select  
                            _id.oid trek_id,
                            team_id.oid team_id,
                            agent_id.oid agent_id,
                            team_leader_id.oid team_leader_id,
                            kpi_name kpi_name,
                            kpi_id.oid kpi_id,
                            campaign_id.oid campaign_id,
                            trek_global_state trek_global_state,
                            trek_date trek_date,
                            discoveryStart discoveryStart ,
                            discoveryEnd discoveryEnd ,
                            actionLMSStart actionLMSStart ,
                            actionLMSEnd actionLMSEnd ,
                            actionLMSLink actionLMSLink,
                            actionArticleStart actionArticleStart,
                            actionArticleEnd actionArticleEnd,
                            actionArticleLink actionArticleLink,
                            actionRecordedCallStart actionRecordedCallStart,
                            actionRecordedCallEnd actionRecordedCallEnd,
                            actionRecordedCallLink actionRecordedCallLink,
                            actionOneOnOneStart actionOneOnOneStart,
                            actionOneOnOneEnd actionOneOnOneEnd,
                            actionOneOnOneLink actionOneOnOneLink,
                            actionState actionState,
                            scoreLMS scoreLMS,
                            scoreArticle scoreArticle,
                            scoreRecordedCall scoreRecordedCall,
                            scoreOneOnOne scoreOneOnOne,
                            action_complete action_complete,
                            feedback_given feedback_given,
                            action_start action_start,
                            discovery_complete discovery_complete,
                            discovery_start discovery_start,
                            action_completed action_completed,
                            lower(organization_id) orgId,
                            actionGoalTarget actionGoalTarget,
                            Survey_question Survey_question,
                            Survey_value Survey_value,
                            Survey_score Survey_score,
                            Survey_type Survey_type,
                            Feedback_question Feedback_question,
                            Feedback_answer Feedback_answer,
                            Feedeback_value Feedeback_value,
                            actionLMSState actionLMSState,
                            actionOneOnOneState actionOneOnOneState,
                            actionArticleState actionArticleState,
                            actionActionState actionActionState,
                            actionRecordedCallState actionRecordedCallState,
                            teamlead_name teamlead_name
                        from trek_data
                    """
            )
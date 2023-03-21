from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

pipeline = [
    {
        '$project': {
            'team_id': 1, 
            'agent_id': 1, 
            'team_leader_id': 1, 
            'kpi_name': 1, 
            'kpi_id': 1, 
            'campaign_id': 1, 
            'trek_global_state': 1, 
            'trek_date': 1, 
            'discoveryStart': {
                '$toDate': '$discovery.start'
            }, 
            'discoveryEnd': {
                '$toDate': '$discovery.end'
            }, 
            'actionLMSStart': {
                '$toDate': '$action.lms.start'
            }, 
            'actionLMSEnd': {
                '$toDate': '$action.lms.end'
            }, 
            'actionLMSLink': '$action.lms.link', 
            'actionArticleStart': {
                '$toDate': '$action.article.start'
            }, 
            'actionArticleEnd': {
                '$toDate': '$action.article.end'
            }, 
            'actionArticleLink': '$action.article.link', 
            'actionRecordedCallStart': {
                '$toDate': '$action.recorded_call.start'
            }, 
            'actionRecordedCallEnd': {
                '$toDate': '$action.recorded_call.end'
            }, 
            'actionRecordedCallLink': '$action.recorded_call.link', 
            'actionOneOnOneStart': {
                '$toDate': '$action.one_on_one.start'
            }, 
            'actionOneOnOneEnd': {
                '$toDate': '$action.one_on_one.end'
            }, 
            'actionOneOnOneLink': '$action.one_on_one.link', 
            'actionState': '$action.state', 
            'scoreLMS': '$score.lms', 
            'scoreArticle': '$score.article', 
            'scoreRecordedCall': '$score.recorded_call', 
            'scoreOneOnOne': '$score.one_on_one', 
            'action_complete': 1, 
            'feedback_given': 1, 
            'action_start': 1, 
            'discovery_complete': 1, 
            'discovery_start': 1, 
            'action_completed': 1, 
            'organization_id': 1
        }
    }, {
        '$lookup': {
            'from': 'Organization', 
            'let': {
                'oid': '$organization_id'
            }, 
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {
                                    '$eq': [
                                        '$org_id', '$$oid'
                                    ]
                                }, {
                                    '$eq': [
                                        '$type', 'Organisation'
                                    ]
                                }
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'timezone': {
                            '$ifNull': [
                                '$timezone', 'Australia/Melbourne'
                            ]
                        }
                    }
                }
            ], 
            'as': 'org'
        }
    }, {
        '$unwind': {
            'path': '$org'
        }
    }, {
        '$project': {
            'team_id': 1, 
            'agent_id': 1, 
            'team_leader_id': 1, 
            'kpi_name': 1, 
            'kpi_id': 1, 
            'campaign_id': 1, 
            'trek_global_state': 1, 
            'trek_date': {
                '$dateToString': {
                    'format': '%Y-%m-%dT%H:%M:%SZ', 
                    'date': {
                        '$toDate': {
                            '$dateToString': {
                                'date': '$trek_date', 
                                'timezone': '$org.timezone'
                            }
                        }
                    }
                }
            }, 
            'discoveryStart': {
                '$dateToString': {
                    'format': '%Y-%m-%dT%H:%M:%SZ', 
                    'date': {
                        '$toDate': {
                            '$dateToString': {
                                'date': '$discoveryStart', 
                                'timezone': '$org.timezone'
                            }
                        }
                    }
                }
            }, 
            'discoveryEnd': {
                '$dateToString': {
                    'format': '%Y-%m-%dT%H:%M:%SZ', 
                    'date': {
                        '$toDate': {
                            '$dateToString': {
                                'date': '$discoveryEnd', 
                                'timezone': '$org.timezone'
                            }
                        }
                    }
                }
            }, 
            'actionLMSStart': {
                '$dateToString': {
                    'format': '%Y-%m-%dT%H:%M:%SZ', 
                    'date': {
                        '$toDate': {
                            '$dateToString': {
                                'date': '$actionLMSStart', 
                                'timezone': '$org.timezone'
                            }
                        }
                    }
                }
            }, 
            'actionLMSEnd': {
                '$dateToString': {
                    'format': '%Y-%m-%dT%H:%M:%SZ', 
                    'date': {
                        '$toDate': {
                            '$dateToString': {
                                'date': '$actionLMSEnd', 
                                'timezone': '$org.timezone'
                            }
                        }
                    }
                }
            }, 
            'actionArticleLink': 1, 
            'actionRecordedCallStart': {
                '$dateToString': {
                    'format': '%Y-%m-%dT%H:%M:%SZ', 
                    'date': {
                        '$toDate': {
                            '$dateToString': {
                                'date': '$actionRecordedCallStart', 
                                'timezone': '$org.timezone'
                            }
                        }
                    }
                }
            }, 
            'actionRecordedCallEnd': {
                '$dateToString': {
                    'format': '%Y-%m-%dT%H:%M:%SZ', 
                    'date': {
                        '$toDate': {
                            '$dateToString': {
                                'date': '$actionRecordedCallEnd', 
                                'timezone': '$org.timezone'
                            }
                        }
                    }
                }
            }, 
            'actionRecordedCallLink': 1, 
            'actionOneOnOneStart': {
                '$dateToString': {
                    'format': '%Y-%m-%dT%H:%M:%SZ', 
                    'date': {
                        '$toDate': {
                            '$dateToString': {
                                'date': '$actionOneOnOneStart', 
                                'timezone': '$org.timezone'
                            }
                        }
                    }
                }
            }, 
            'actionOneOnOneEnd': {
                '$dateToString': {
                    'format': '%Y-%m-%dT%H:%M:%SZ', 
                    'date': {
                        '$toDate': {
                            '$dateToString': {
                                'date': '$actionOneOnOneEnd', 
                                'timezone': '$org.timezone'
                            }
                        }
                    }
                }
            }, 
            'actionOneOnOneLink': 1, 
            'actionState': 1, 
            'scoreLMS': 1, 
            'scoreArticle': 1, 
            'scoreRecordedCall': 1, 
            'scoreOneOnOne': 1, 
            'action_complete': 1, 
            'feedback_given': 1, 
            'action_start': 1, 
            'discovery_complete': 1, 
            'discovery_start': 1, 
            'action_completed': 1, 
            'organization_id': 1,
            '_id': 0
        }
    }
]


schema = StructType([
                    StructField('team_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                    StructField('agent_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                    StructField('team_leader_id',StructType(
                         [StructField('oid', StringType(), True)]), True),
                    StructField('kpi_id',StructType(
                         [StructField('oid', StringType(), True)]), True),
                    StructField('kpi_name', StringType(), True),
                     StructField('campaign_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                    StructField('trek_global_state', IntegerType(), True),
                    StructField('trek_date', StringType(), True),
                    StructField('discoveryStart', StringType(), True),
                    StructField('discoveryEnd', StringType(), True),
                    StructField('actionLMSStart', StringType(), True),
                    StructField('actionLMSEnd', StringType(), True),
                    StructField('actionLMSLink', StringType(), True),
                    StructField('actionArticleStart', StringType(), True),
                    StructField('actionArticleEnd', StringType(), True),
                    StructField('actionArticleLink', StringType(), True),
                    StructField('actionRecordedCallStart', StringType(), True),
                    StructField('actionRecordedCallEnd', StringType(), True),
                    StructField('actionRecordedCallLink', StringType(), True),
                    StructField('actionOneOnOneStart', StringType(), True),
                    StructField('actionOneOnOneEnd', StringType(), True),
                    StructField('actionOneOnOneLink', StringType(), True),
                    StructField('actionState', StringType(), True),
                    StructField('scoreLMS', IntegerType(), True),
                    StructField('scoreArticle', IntegerType(), True),
                    StructField('scoreRecordedCall', IntegerType(), True),
                    StructField('scoreOneOnOne', IntegerType(), True),
                    StructField('action_complete', StringType(), True),
                    StructField('feedback_given', StringType(), True),
                    StructField('action_start', StringType(), True),
                    StructField('discovery_complete', StringType(), True),
                    StructField('discovery_start', StringType(), True),
                    StructField('action_completed', StringType(), True),
                    StructField('organization_id', StringType(), True)])


def get_trek_data(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'TrekData', schema)
    df.createOrReplaceTempView("trek_data")
    df = spark.sql("""
                select  team_id.oid team_id,
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
                        lower(organization_id) orgId
                    from trek_data
                """)
    #delta_table_partition_ovrewrite(df, "dg_performance_management.trek_data", ['orgId'])
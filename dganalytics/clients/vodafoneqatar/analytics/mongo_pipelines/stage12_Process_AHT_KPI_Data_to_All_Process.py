aggr_pipeline = [
  {
    "$lookup": {
      "from": "Skill_List",
      "let": {
        "raw_Split": "$split"
      },
      "pipeline": [
        {
          "$group": {
            "_id": {
              "split": "$split"
            },
            "location": {
              "$first": "$location"
            },
            "lobs": {
              "$first": "$lobs"
            },
            "language": {
              "$first": "$language"
            },
            "arpu_segment": {
              "$first": "$arpu_segment"
            },
            "segment_type": {
              "$first": "$segment_type"
            },
            "services": {
              "$first": "$services"
            },
            "site": {
              "$first": "$site"
            }
          }
        },
        {
          "$match": {
            "$expr": {
              "$eq": [
                "$_id.split",
                "$$raw_Split"
              ]
            }
          }
        }
      ],
      "as": "skill"
    }
  },
  {
    "$unwind": {
      "path": "$skill"
    }
  },
  {
    "$addFields": {
      "avg_acd_time": {
        "$multiply": [
          {
            "$ifNull": [
              "$avg_acd_time",
              0
            ]
          },
          {
            "$ifNull": [
              "$acd_calls",
              0
            ]
          }
        ]
      },
      "avg_acw_time": {
        "$multiply": [
          {
            "$ifNull": [
              "$avg_acw_time",
              0
            ]
          },
          {
            "$ifNull": [
              "$acd_calls",
              0
            ]
          }
        ]
      },
      "avg_hold_time": {
        "$multiply": [
          {
            "$ifNull": [
              "$avg_hold_time",
              0
            ]
          },
          {
            "$ifNull": [
              "$acd_calls",
              0
            ]
          }
        ]
      },
      "avg_speed_ans": {
        "$multiply": [
          {
            "$ifNull": [
              "$avg_speed_ans",
              0
            ]
          },
          {
            "$ifNull": [
              "$acd_calls",
              0
            ]
          }
        ]
      }
    }
  },
  {
    "$group": {
      "_id": {
        "location": "$skill.location",
        "lobs": "$skill.lobs",
        "language": "$skill.language",
        "arpu_segment": "$skill.arpu_segment",
        "segment_type": "$skill.segment_type",
        "services": "$skill.services",
        "site": "$skill.site",
        "date": {
          "$dateToString": {
            "format": "%Y-%m-%d",
            "date": "$report_date"
          }
        }
      },
      "avg_acd_time": {
        "$sum": "$avg_acd_time"
      },
      "avg_acw_time": {
        "$sum": "$avg_acw_time"
      },
      "avg_hold_time": {
        "$sum": "$avg_hold_time"
      },
      "avg_speed_ans": {
        "$sum": "$avg_speed_ans"
      },
      "acd_calls": {
        "$sum": {
          "$cond": [
            {
              "$eq": [
                "$acd_calls",
                None
              ]
            },
            0,
            "$acd_calls"
          ]
        }
      }
    }
  },
  {
    "$facet": {
      "AHT": [
        {
          "$project": {
            "location": "$_id.location",
            "lobs": "$_id.lobs",
            "language": "$_id.language",
            "arpu_segment": "$_id.arpu_segment",
            "segment_type": "$_id.segment_type",
            "services": "$_id.services",
            "site": "$_id.site",
            "date": "$_id.date",
            "value1": {
              "$sum": [
                "$avg_acd_time",
                "$avg_acw_time",
                "$avg_hold_time"
              ]
            },
            "value2": "$acd_calls",
            "kpi": "AHT",
            "_id": 0
          }
        }
      ],
      "Hold Time": [
        {
          "$project": {
            "location": "$_id.location",
            "lobs": "$_id.lobs",
            "language": "$_id.language",
            "arpu_segment": "$_id.arpu_segment",
            "segment_type": "$_id.segment_type",
            "services": "$_id.services",
            "site": "$_id.site",
            "date": "$_id.date",
            "value1": {
              "$sum": [
                "$avg_hold_time"
              ]
            },
            "value2": "$acd_calls",
            "kpi": "Hold Time",
            "_id": 0
          }
        }
      ],
      "ASA": [
        {
          "$project": {
            "location": "$_id.location",
            "lobs": "$_id.lobs",
            "language": "$_id.language",
            "arpu_segment": "$_id.arpu_segment",
            "segment_type": "$_id.segment_type",
            "services": "$_id.services",
            "site": "$_id.site",
            "date": "$_id.date",
            "value1": "$avg_speed_ans",
            "value2": "$acd_calls",
            "kpi": "ASA",
            "_id": 0
          }
        }
      ],
      "ACW": [
        {
          "$project": {
            "location": "$_id.location",
            "lobs": "$_id.lobs",
            "language": "$_id.language",
            "arpu_segment": "$_id.arpu_segment",
            "segment_type": "$_id.segment_type",
            "services": "$_id.services",
            "site": "$_id.site",
            "date": "$_id.date",
            "value1": "$avg_acw_time",
            "value2": "$acd_calls",
            "kpi": "ACW",
            "_id": 0
          }
        }
      ],
      "ACD": [
        {
          "$project": {
            "location": "$_id.location",
            "lobs": "$_id.lobs",
            "language": "$_id.language",
            "arpu_segment": "$_id.arpu_segment",
            "segment_type": "$_id.segment_type",
            "services": "$_id.services",
            "site": "$_id.site",
            "date": "$_id.date",
            "value1": "$avg_acd_time",
            "value2": "$acd_calls",
            "kpi": "ACD",
            "_id": 0
          }
        }
      ]
    }
  },
  {
    "$project": {
      "common": {
        "$concatArrays": [
          "$AHT",
          "$ACD",
          "$ACW",
          "$ASA",
          "$Hold Time"
        ]
      }
    }
  },
  {
    "$unwind": {
      "path": "$common"
    }
  },
  {
    "$project": {
      "location": "$common.location",
      "lobs": "$common.lobs",
      "language": "$common.language",
      "arpu_segment": "$common.arpu_segment",
      "segment_type": "$common.segment_type",
      "services": "$common.services",
      "site": "$common.site",
      "date": "$common.date",
      "value1": "$common.value1",
      "value2": "$common.value2",
      "kpi": "$common.kpi"
    }
  }
]
aggr_pipeline = [
  {
    "$group": {
      "_id": {
        "user": "$user_id",
        "report_date": "$report_date"
      },
      "ACD": {
        "$sum": "$total_time"
      },
      "ACW": {
        "$sum": "$total_acw_time"
      },
      "HOLD": {
        "$sum": "$other_time"
      },
      "ACD_CALL": {
        "$sum": "$acd_calls"
      },
      "Agent_Ring_Time": {
        "$sum": "$agent_ring_time"
      },
      "Avail_Time": {
        "$sum": "$avail_time1"
      },
      "Staffed_Time": {
        "$sum": "$staffed_time2"
      }
    }
  },
  {
    "$facet": {
      "AHT": [
        {
          "$project": {
            "user_id": "$_id.user",
            "date": {
              "$dateToString": {
                "format": "%Y-%m-%d",
                "date": "$_id.report_date"
              }
            },
            "value1": {
              "$add": [
                "$ACD",
                "$ACW",
                "$HOLD"
              ]
            },
            "value2": "$ACD_CALL",
            "kpi": "AHT",
            "_id": 0
          }
        }
      ],
      "Occupancy": [
        {
          "$project": {
            "user_id": "$_id.user",
            "date": {
              "$dateToString": {
                "format": "%Y-%m-%d",
                "date": "$_id.report_date"
              }
            },
            "value1": {
              "$add": [
                "$ACD",
                "$ACW",
                "$Agent_Ring_Time"
              ]
            },
            "value2": {
              "$add": [
                "$ACD",
                "$ACW",
                "$Agent_Ring_Time",
                "$Avail_Time"
              ]
            },
            "kpi": "Occupancy",
            "_id": 0
          }
        }
      ],
      "Utilization": [
        {
          "$project": {
            "user_id": "$_id.user",
            "date": {
              "$dateToString": {
                "format": "%Y-%m-%d",
                "date": "$_id.report_date"
              }
            },
            "value1": {
              "$add": [
                "$ACD",
                "$ACW",
                "$Agent_Ring_Time",
                "$Avail_Time"
              ]
            },
            "value2": "$Staffed_Time",
            "kpi": "Utilization",
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
          "$Occupancy",
          "$Utilization"
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
      "user_id": "$common.user_id",
      "date": "$common.date",
      "value1": "$common.value1",
      "value2": "$common.value2",
      "kpi": "$common.kpi"
    }
  }
]
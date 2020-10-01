aggr_pipeline = [
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
    "$lookup": {
      "from": "CALLSERVICE",
      "let": {
        "raw_split": "$_id.split"
      },
      "pipeline": [
        {
          "$match": {
            "$expr": {
              "$eq": [
                "$split",
                "$$raw_split"
              ]
            }
          }
        }
      ],
      "as": "call_data"
    }
  },
  {
    "$unwind": {
      "path": "$call_data"
    }
  },
  {
    "$group": {
      "_id": {
        "location": "$location",
        "lobs": "$lobs",
        "language": "$language",
        "arpu_segment": "$arpu_segment",
        "segment_type": "$segment_type",
        "services": "$services",
        "site": "$site",
        "date": {
          "$dateToString": {
            "format": "%Y-%m-%d",
            "date": "$call_data.report_date"
          }
        }
      },
      "answd_in_threshold": {
        "$sum": "$call_data.answd_in_threshold"
      },
      "offered": {
        "$sum": "$call_data.offered"
      },
      "aban_in_5_secs": {
        "$sum": "$call_data.aban_in_5_secs"
      },
      "aban_calls": {
        "$sum": "$call_data.aban_calls"
      },
      "acd_calls": {
        "$sum": "$call_data.acd_calls"
      }
    }
  },
  {
    "$facet": {
      "Service_Level": [
        {
          "$project": {
            "kpi": "Service Level %",
            "location": "$_id.location",
            "lobs": "$_id.lobs",
            "language": "$_id.language",
            "arpu_segment": "$_id.arpu_segment",
            "segment_type": "$_id.segment_type",
            "services": "$_id.services",
            "site": "$_id.site",
            "date": "$_id.date",
            "value1": "$answd_in_threshold",
            "value2": {
              "$subtract": [
                "$offered",
                "$aban_in_5_secs"
              ]
            },
            "_id": 0
          }
        }
      ],
      "Aban": [
        {
          "$project": {
            "kpi": "Aban %",
            "location": "$_id.location",
            "lobs": "$_id.lobs",
            "language": "$_id.language",
            "arpu_segment": "$_id.arpu_segment",
            "segment_type": "$_id.segment_type",
            "services": "$_id.services",
            "site": "$_id.site",
            "date": "$_id.date",
            "value1": "$aban_calls",
            "value2": "$offered",
            "_id": 0
          }
        }
      ]
    }
  },
  {
    "$project": {
      "items": {
        "$concatArrays": [
          "$Service_Level",
          "$Aban"
        ]
      }
    }
  },
  {
    "$unwind": {
      "path": "$items"
    }
  },
  {
    "$project": {
      "location": "$items.location",
      "lobs": "$items.lobs",
      "language": "$items.language",
      "arpu_segment": "$items.arpu_segment",
      "segment_type": "$items.segment_type",
      "services": "$items.services",
      "site": "$items.site",
      "date": "$items.date",
      "value1": "$items.value1",
      "value2": "$items.value2",
      "kpi": "$items.kpi"
    }
  }
]
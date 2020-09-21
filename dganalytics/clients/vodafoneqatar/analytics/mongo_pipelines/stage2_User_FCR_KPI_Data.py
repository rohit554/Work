aggr_pipeline = [
  {
    "$addFields": {
      "fcr": {
        "$cond": {
          "if": {
            "$eq": [
              "$check",
              "FCR"
            ]
          },
          "then": 1,
          "else": 0
        }
      },
      "total": 1
    }
  },
  {
    "$group": {
      "_id": {
        "user_id": "$user_id",
        "report_date": {
          "$dateToString": {
            "format": "%Y-%m-%d",
            "date": "$report_date",
            "timezone": "+05:30"
          }
        }
      },
      "fcr_count": {
        "$sum": "$fcr"
      },
      "total_calls": {
        "$sum": "$total"
      }
    }
  },
  {
    "$project": {
      "user_id": "$_id.user_id",
      "date": "$_id.report_date",
      "value1": "$fcr_count",
      "value2": "$total_calls",
      "kpi": "FCR",
      "_id": 0
    }
  }
]
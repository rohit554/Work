aggr_pipeline = [
  {
    "$group": {
      "_id": {
        "report_date": "$report_date",
        "split": "$split"
      },
      "offered": {
        "$sum": "$offered"
      }
    }
  },
  {
    "$project": {
      "report_date": "$_id.report_date",
      "split": "$_id.split",
      "offered": 1,
      "_id": 0
    }
  },
  {
    "$lookup": {
      "from": "Skill_List",
      "let": {
        "raw_split": "$split"
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
        },
        {
          "$group": {
            "_id": {
              "split": "$$raw_split",
              "lobs": "$lobs",
              "segment_type": "$segment_type",
              "location": "$location",
              "language": "$language"
            }
          }
        }
      ],
      "as": "skill_data"
    }
  },
  {
    "$unwind": {
      "path": "$skill_data",
      "preserveNullAndEmptyArrays": False
    }
  },
  {
    "$project": {
      "offered": 1,
      "report_date": 1,
      "split": 1,
      "lobs": "$skill_data._id.lobs",
      "segment_type": "$skill_data._id.segment_type",
      "location": "$skill_data._id.location",
      "language": "$skill_data._id.language"
    }
  },
  {
    "$lookup": {
      "from": "FORECAST",
      "let": {
        "raw_lobs": "$lobs",
        "raw_report_date": "$report_date",
        "offered": "$offered",
        "segment_type": "$segment_type",
        "location": "$location",
        "split": "$split",
        "language": "$language"
      },
      "pipeline": [
        {
          "$match": {
            "$and": [
              {
                "$expr": {
                  "$eq": [
                    "$lob",
                    "$$raw_lobs"
                  ]
                }
              },
              {
                "$expr": {
                  "$and": [
                    {
                      "$eq": [
                        {
                          "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$report_date"
                          }
                        },
                        {
                          "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$$raw_report_date"
                          }
                        }
                      ]
                    }
                  ]
                }
              },
              {
                "$expr": {
                  "$eq": [
                    "$language",
                    "$$language"
                  ]
                }
              }
            ]
          }
        }
      ],
      "as": "forecast_data"
    }
  },
  {
    "$unwind": {
      "path": "$forecast_data",
      "preserveNullAndEmptyArrays": True
    }
  },
  {
    "$project": {
      "offered": 1,
      "report_date": 1,
      "lobs": 1,
      "segment_type": 1,
      "location": 1,
      "language": {
        "$cond": [
          {
            "$eq": [
              "$lobs",
              "Business Offshore"
            ]
          },
          {
            "$cond": [
              {
                "$eq": [
                  "$language",
                  "Hindi"
                ]
              },
              "English",
              "$language"
            ]
          },
          "$language"
        ]
      },
      "forecast": {
        "$ifNull": [
          "$forecast_data.forecast",
          0
        ]
      }
    }
  },
  {
    "$group": {
      "_id": {
        "report_date": "$report_date",
        "lob": "$lobs",
        "segment_type": "$segment_type",
        "location": "$location",
        "language": "$language"
      },
      "offered": {
        "$sum": "$offered"
      },
      "forecast": {
        "$first": "$forecast"
      }
    }
  },
  {
    "$project": {
      "report_date": {
        "$dateToString": {
          "format": "%m-%d-%Y",
          "date": "$_id.report_date",
          "timezone": "+05:30"
        }
      },
      "lob": "$_id.lob",
      "segment_type": "$_id.segment_type",
      "location": "$_id.location",
      "language": "$_id.language",
      "actual": "$offered",
      "forecast": 1,
      "_id": 0
    }
  }
]
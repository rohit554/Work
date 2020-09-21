aggr_pipeline = [
  {
    "$match": {
      "user_id": {
        "$ne": None
      }
    }
  },
  {
    "$addFields": {
      "report_date": {
        "$substr": [
          "$report_date",
          0,
          10
        ]
      },
      "TNPS_VAL": {
        "$switch": {
          "branches": [
            {
              "case": {
                "$and": [
                  {
                    "$gte": [
                      "$overall_tnps",
                      0
                    ]
                  },
                  {
                    "$lt": [
                      "$overall_tnps",
                      7
                    ]
                  }
                ]
              },
              "then": "Detractor"
            },
            {
              "case": {
                "$and": [
                  {
                    "$gte": [
                      "$overall_tnps",
                      9
                    ]
                  },
                  {
                    "$lt": [
                      "$overall_tnps",
                      11
                    ]
                  }
                ]
              },
              "then": "Promoter"
            },
            {
              "case": {
                "$and": [
                  {
                    "$gte": [
                      "$overall_tnps",
                      7
                    ]
                  },
                  {
                    "$lt": [
                      "$overall_tnps",
                      9
                    ]
                  }
                ]
              },
              "then": "Passive"
            }
          ],
          "default": ""
        }
      }
    }
  },
  {
    "$group": {
      "_id": {
        "user": "$user_id",
        "report_date": "$report_date"
      },
      "Detractor": {
        "$sum": {
          "$cond": [
            {
              "$eq": [
                "$TNPS_VAL",
                "Detractor"
              ]
            },
            1,
            0
          ]
        }
      },
      "Promoter": {
        "$sum": {
          "$cond": [
            {
              "$eq": [
                "$TNPS_VAL",
                "Promoter"
              ]
            },
            1,
            0
          ]
        }
      },
      "Passive": {
        "$sum": {
          "$cond": [
            {
              "$eq": [
                "$TNPS_VAL",
                "Passive"
              ]
            },
            1,
            0
          ]
        }
      },
      "FCR": {
        "$sum": {
          "$cond": [
            {
              "$eq": [
                "$issue_resolved",
                "Yes, on the first time I contacted Vodafone"
              ]
            },
            1,
            0
          ]
        }
      },
      "NFCR": {
        "$sum": {
          "$cond": [
            {
              "$or": [
                {
                  "$eq": [
                    "$issue_resolved",
                    "No"
                  ]
                },
                {
                  "$eq": [
                    "$issue_resolved",
                    "Yes, but I had to contact Vodafone more than once"
                  ]
                }
              ]
            },
            1,
            0
          ]
        }
      }
    }
  },
  {
    "$facet": {
      "TNPS": [
        {
          "$project": {
            "user": "$_id.user",
            "date": "$_id.report_date",
            "value1": {
              "$subtract": [
                "$Promoter",
                "$Detractor"
              ]
            },
            "value2": {
              "$add": [
                "$Promoter",
                "$Detractor",
                "$Passive"
              ]
            },
            "kpi": "TNPS",
            "_id": 0
          }
        }
      ],
      "FTF": [
        {
          "$project": {
            "user": "$_id.user",
            "date": "$_id.report_date",
            "value1": "$FCR",
            "value2": {
              "$add": [
                "$FCR",
                "$NFCR"
              ]
            },
            "kpi": "FTF",
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
          "$TNPS",
          "$FTF"
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
      "user_id": "$common.user",
      "date": "$common.date",
      "value1": "$common.value1",
      "value2": "$common.value2",
      "kpi": "$common.kpi"
    }
  }
]
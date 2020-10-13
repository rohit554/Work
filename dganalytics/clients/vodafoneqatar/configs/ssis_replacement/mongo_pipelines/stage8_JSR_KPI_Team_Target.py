aggr_pipeline = [
  {
    "$project": {
      "kpi": {
        "$cond": [
          {
            "$eq": [
              "$kpi",
              "FCR7"
            ]
          },
          "FCR",
          "$kpi"
        ]
      },
      "_id": 0,
      "team_id": "$lob",
      "target": {
        "$cond": [
          {
            "$eq": [
              "$target",
              "Not Available"
            ]
          },
          0,
          "$target"
        ]
      },
      "weightage": 1,
      "date": {
        "$concat": [
          "01",
          {
            "$switch": {
              "branches": [
                {
                  "case": {
                    "$eq": [
                      {
                        "$substr": [
                          "$Date",
                          0,
                          3
                        ]
                      },
                      "Jan"
                    ]
                  },
                  "then": "/01/"
                },
                {
                  "case": {
                    "$eq": [
                      {
                        "$substr": [
                          "$Date",
                          0,
                          3
                        ]
                      },
                      "Feb"
                    ]
                  },
                  "then": "/02/"
                },
                {
                  "case": {
                    "$eq": [
                      {
                        "$substr": [
                          "$Date",
                          0,
                          3
                        ]
                      },
                      "Mar"
                    ]
                  },
                  "then": "/03/"
                },
                {
                  "case": {
                    "$eq": [
                      {
                        "$substr": [
                          "$Date",
                          0,
                          3
                        ]
                      },
                      "Apr"
                    ]
                  },
                  "then": "/04/"
                },
                {
                  "case": {
                    "$eq": [
                      {
                        "$substr": [
                          "$Date",
                          0,
                          3
                        ]
                      },
                      "May"
                    ]
                  },
                  "then": "/05/"
                },
                {
                  "case": {
                    "$eq": [
                      {
                        "$substr": [
                          "$Date",
                          0,
                          3
                        ]
                      },
                      "Jun"
                    ]
                  },
                  "then": "/06/"
                },
                {
                  "case": {
                    "$eq": [
                      {
                        "$substr": [
                          "$Date",
                          0,
                          3
                        ]
                      },
                      "Jul"
                    ]
                  },
                  "then": "/07/"
                },
                {
                  "case": {
                    "$eq": [
                      {
                        "$substr": [
                          "$Date",
                          0,
                          3
                        ]
                      },
                      "Aug"
                    ]
                  },
                  "then": "/08/"
                },
                {
                  "case": {
                    "$eq": [
                      {
                        "$substr": [
                          "$Date",
                          0,
                          3
                        ]
                      },
                      "Sep"
                    ]
                  },
                  "then": "/09/"
                },
                {
                  "case": {
                    "$eq": [
                      {
                        "$substr": [
                          "$Date",
                          0,
                          3
                        ]
                      },
                      "Oct"
                    ]
                  },
                  "then": "/10/"
                },
                {
                  "case": {
                    "$eq": [
                      {
                        "$substr": [
                          "$Date",
                          0,
                          3
                        ]
                      },
                      "Nov"
                    ]
                  },
                  "then": "/11/"
                },
                {
                  "case": {
                    "$eq": [
                      {
                        "$substr": [
                          "$Date",
                          0,
                          3
                        ]
                      },
                      "Dec"
                    ]
                  },
                  "then": "/12/"
                }
              ],
              "default": "/01/"
            }
          },
          {
            "$concat": [
              "20",
              {
                "$substr": [
                  "$Date",
                  4,
                  2
                ]
              }
            ]
          }
        ]
      }
    }
  }
]
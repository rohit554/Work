
from dganalytics.utils.utils import get_spark_session, get_path_vars


app_name = "datagamz_performance_management_setup"
tenant = "datagamz"
tenant_path, db_path, log_path = get_path_vars(tenant)
spark = get_spark_session(app_name=app_name, tenant=tenant)


spark.sql(f"""
                create database if not exists dg_performance_management  LOCATION '{db_path}/dg_performance_management'
            """)

spark.sql(f"""
        create table if not exists 
            dg_performance_management.users
            (
                userId string,
                email string,
                firstName string,
                lastName string,
                mongoUserId string,
                name string,
                quartile string,
                roleId string,
                teamLeadName string,
                teamName string,
                state string,
                orgId string
            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_performance_management/users'
        """)

spark.sql(f"""
        create table if not exists 
            dg_performance_management.activity_wise_points
            (
                campaignId string,
                activityId string,
                userId string,
                points int,
                outcomeType string,
                teamId string,
                kpiName string,
                fieldName string,
                fieldValue double,
                frequency string,
                entityName string,
                noOfTimesPerformed int,
                activityName string,
                target double,
                date date,
                mongoUserId string,
                awardedBy string,
                orgId string
            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_performance_management/activity_wise_points'
        """)

spark.sql(f"""
        create table if not exists 
            dg_performance_management.activity_mapping
            (
                
                campaignId string,
                campaignName string,
                activityId string,
                activityName string,
                isChallengeActivty boolean,
                orgId string
            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_performance_management/activity_mapping'
        """)

spark.sql(f"""
        create table if not exists 
            dg_performance_management.campaign
            (
                campaignId string,
                start_date date,
                endDate date,
                isActive boolean,
                isDeleted boolean,
                name string,
                orgId string
            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_performance_management/campaign'
        """)

spark.sql(f"""
        create table if not exists 
            dg_performance_management.badges
            (
                badgeName string,
                campaignId string,
                date date,
                description string,
                leadMongoUserId string,
                userId string,
                orgId string
            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_performance_management/badges'
        """)

spark.sql(f"""
        create table if not exists 
            dg_performance_management.challenges
            (
                action string,
                campaignId string,
                challengeThrownDate date,
                challengeAcceptanceDate date,
                challengeCompletionDate date,
                challengeEndDate date,
                challengeFrequency int,
                challengeName string,
                challengeeMongoId string,
                challengerMongoId string,
                noOfDays int,
                status string,
                orgId string

            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_performance_management/challenges'
        """)

spark.sql(f"""
        create table if not exists 
            dg_performance_management.quizzes
            (
                answeredDate date,
                campaign_id string,
                noOfCorrectQuestions int,
                quizId string,
                quizName string,
                quizPercentageScore float,
                quizStatus string,
                teamLeadMongoId string,
                totalQuestions int,
                userId string,
                userMongoId string,
                orgId string,
                quizStartDate date

            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_performance_management/quizzes'
        """)

spark.sql(f"""
        create table if not exists 
            dg_performance_management.questions
            (
                answerGiven string,
                answeredDate date,
                campaignId string,
                correctAnswer string,
                isCorrect boolean,
                question string,
                quiz_id string,
                subject_area string,
                userId string,
                orgId string

            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_performance_management/questions'
        """)

spark.sql(f"""
        create table if not exists 
            dg_performance_management.levels
            (
                achievedDate timestamp,
                campaignId string,
                campaignName string,
                levelEndPoints int,
                levelId string,
                levelNumber int,
                levelStartPoints int,
                mongoUserId string,
                userId string,
                orgId string

            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_performance_management/levels'
        """)

spark.sql(f"""
        create table if not exists 
            dg_performance_management.user_campaign
            (
                campaignId string,
                teamId string,
                userId string,
                orgId string

            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_performance_management/user_campaign'
        """)

spark.sql(f"""
        create table if not exists 
            dg_performance_management.logins
            (
                date date,
                loginAttempt int,
                userId string,
                orgId string
            )
            using delta
            PARTITIONED BY (orgId)
            LOCATION '{db_path}/dg_performance_management/logins'
        """)
from helpers import *

def run_transform(spark, kafka_messages_df, start_date, start_offset):

    createView(kafka_messages_df, "kafka_messages")

    runSQL(spark, "sql/01_events_dedup.sql", "t_stg_events", {"start_date":start_date, "start_offset":start_offset})
    runSQL(spark, "sql/02_event_continuous_ranges.sql", "t_stg_event_continuous_ranges")

    return runFinalSQL(spark, "sql/03_missing_offsets.sql")

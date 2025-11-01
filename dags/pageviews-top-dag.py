from universal_spark_dag import build_spark_dag

globals()["spark_job_wikipedia_pageviews_top"] = build_spark_dag(
    path_name="wikipedia_pageviews/top",
    table_name="wikipedia_pageviews_top",
    schedule="0 17 * * *",
)
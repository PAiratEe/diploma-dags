from universal_spark_dag import build_spark_dag

spark_job_packet_first = build_spark_dag(
    path_name="packet/first",
    table_name="packet_first",
    schedule="0 17 * * *",
)
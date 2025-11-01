from spark_job_builder import SparkJobBuilder

globals()["spark_job_pageviews_top"] = SparkJobBuilder("wikipedia_pageviews/top", "wikipedia_pageviews_top").build_dag()
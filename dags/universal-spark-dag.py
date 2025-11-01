from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import timedelta, datetime


class SparkJobBuilder:

    def __init__(self, path_name:str, table_name: str):
        self.table_name = table_name
        super().__init__(
            dag_id=f"spark_job_{table_name}",
            default_args={
                "owner": "airflow",
                "depends_on_past": False,
                "start_date": datetime.now() - timedelta(days=1),
                "retries": 1,
                "execution_timeout": timedelta(hours=2),
            },
            schedule="0 15 * * *",
            catchup=False,
            tags=["spark", table_name],
        )

    def build(self, dag):

        KubernetesPodOperator(
            task_id=f"spark_submit_{self.table_name}",
            namespace="default",
            image="pairate/spark-job:latest",
            cmds=["/opt/spark/bin/spark-submit"],
            arguments=[
                "--master", "k8s://https://192.168.49.2:8443",
                "--deploy-mode", "cluster",
                "--class", "org.example.Main",
                "--conf", "spark.kubernetes.file.upload.path=s3a://airbyte-bucket/spark",
                "--conf", "spark.dynamicAllocation.enabled=false",
                "--conf", "spark.executor.instances=2",
                "--conf", "spark.kubernetes.container.image=pairate/spark-job:latest",
                "--conf", "spark.hadoop.fs.s3a.endpoint=http://airbyte-minio-svc:9000",
                "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
                "--conf", "spark.hadoop.fs.s3a.access.key=minio",
                "--conf", "spark.hadoop.fs.s3a.secret.key=minio123",
                "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
                "--conf", "spark.kubernetes.driverEnv.YEAR={{ ds.split('-')[0] }}",
                "--conf", "spark.kubernetes.driverEnv.MONTH={{ ds.split('-')[1] }}",
                "--conf", "spark.kubernetes.driverEnv.DAY={{ ds.split('-')[2] }}",
                "--conf", "spark.kubernetes.driverEnv.PATH={self.path_name}",
                "--conf", "spark.kubernetes.driverEnv.TABLE={self.table_name}",
                "/opt/spark/work-dir/SparkJob-1.0-SNAPSHOT.jar"
            ],
            name=f"spark-job-{self.table_name}",
            get_logs=True,
            in_cluster=True,
            is_delete_operator_pod=True,
            dag=dag,
        )
        return dag
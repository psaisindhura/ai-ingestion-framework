

import os

from string import Template

DAG_TEMPLATE = Template("""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ingestion_pipeline_${df}",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example", "bash"],
) as dag:

    file_check = BashOperator(
        task_id="file_check",
        bash_command='echo "Hello, Airflow!"'
    )

    ingestion = BashOperator(
        task_id="ingestion",
        bash_command="sh /opt/ai-ingestion-framework/scripts/spark_job_execute.sh /opt/ai-ingestion-framework/configs/job_${df}.json abc"
    )

    file_move = BashOperator(
        task_id="file_move",
        bash_command="sleep 5"
    )

    file_check >> ingestion >> file_move
""")



class DagGenerator:
    def __init__(self, dag_template, output_dir):
        self.dag_template = dag_template
        self.output_dir = output_dir

    def generate(self, dag_name: str) -> str:
        dag_code = self.dag_template.substitute(df=dag_name)

        file_name = f"ingestion_pipeline_{dag_name}.py"
        file_path = os.path.join(self.output_dir, file_name)

        os.makedirs(self.output_dir, exist_ok=True)

        with open(file_path, "w") as f:
            f.write(dag_code)

        return file_path

if __name__ == "__main__":
    generator = DagGenerator(
        dag_template=DAG_TEMPLATE,
        output_dir="/opt/airflow/dags/generated"
    )

    dag_file = generator.generate("nagendra")
    print(f"DAG created at: {dag_file}")
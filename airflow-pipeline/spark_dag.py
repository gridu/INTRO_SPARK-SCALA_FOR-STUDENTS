from datetime import datetime
from airflow import DAG

from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

with DAG('marketing_data_lake',
            schedule_interval=None,
            start_date=datetime(2021, 11, 26)) as dag:

        SPARK_STEPS = [ 
            {
                'Name': 'Converting csv to parquet step',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        '--executor-memory', '6G',
                        '--num-executors', '1',
                        '--executor-cores', '2',
                        '--class', 'marketing.Marketing',
                        's3://jar-name.jar', '-task', 'convert-csv-parquet'
                    ]
                }
            }
        ]

        create_emr_cluster = EmrCreateJobFlowOperator(
            task_id="create_emr_cluster",
            aws_conn_id="aws_default",
            emr_conn_id="emr_default"
        )

        step_adder = EmrAddStepsOperator(
            task_id="add_steps",
            job_flow_id=create_emr_cluster.output,
            aws_conn_id="aws_default",
            steps=SPARK_STEPS
        )

        last_step = len(SPARK_STEPS) - 1

        step_checker = EmrStepSensor(
            task_id="watch_step",
            job_flow_id=create_emr_cluster.output,
            step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
                + str(last_step)
                + "] }}",
            aws_conn_id="aws_default"
        )

        terminate_emr_cluster = EmrTerminateJobFlowOperator(
                task_id="terminate_emr_cluster",
                job_flow_id=create_emr_cluster.output,
                aws_conn_id="aws_default"
        )

        step_adder >> step_checker >> terminate_emr_cluster

        


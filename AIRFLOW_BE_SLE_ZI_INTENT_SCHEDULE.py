import boto3
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# =====================================================
# STATIC VARIABLES (specific for this DAG)
# =====================================================

# You can use Airflow Variable if you want dynamic scheduling
SCHEDULE_INTERVAL_VAR = Variable.get('schedule_interval_be_sle_zi_intent', default_var="30 4 * * 1")

# Default arguments for the DAG
default_args = {
    'owner': 'BE-SLE',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 4),
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}

dag = DAG(
    dag_id='AIRFLOW_BE_SLE_ZI_INTENT_SCHEDULE',
    schedule_interval=SCHEDULE_INTERVAL_VAR,  # Runs every Monday 10 AM IST (04:30 UTC)
    default_args=default_args,
    concurrency=5,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30)
)

# Glue Job names - sequential execution
INGEST_JOB = 'be-sle-zi-intent-ingest-glue-job'
LKP_JOB = 'be-sle-zi-intent-lkp-glue-job'

# Dummy operators for DAG flow
stage_Load = DummyOperator(task_id='Start_Trigger', dag=dag)
Load_end = DummyOperator(task_id='Trigger_Success', dag=dag)

# Function to start AWS Glue job with parameters
def run_glue_job(job_name, source_name):
    session = boto3.session.Session()
    glue_client = session.client('glue')

    arguments = {
        '--source_name': source_name
    }

    try:
        job_run_id = glue_client.start_job_run(JobName=job_name, Arguments=arguments)
        print(f"Started Glue job {job_name} with run ID: {job_run_id['JobRunId']}")
        return job_run_id
    except Exception as e:
        print(f"Error starting Glue job {job_name}: {e}")
        raise

# Function to check the status of a Glue job
def wait_for_glue_job_completion(job_name, job_run_id):
    session = boto3.session.Session()
    glue_client = session.client('glue')

    while True:
        try:
            response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
            job_status = response['JobRun']['JobRunState']

            if job_status in ['SUCCEEDED','FAILED','STOPPED']:
                print(f"Glue job {job_name} completed with status: {job_status}")
                if job_status in ['FAILED', 'STOPPED']:
                    raise Exception(f"Glue job {job_name} {job_status}")
                return job_status
            else:
                print(f"Glue job {job_name} is still in progress...")
                time.sleep(30)  # Wait for 30 seconds before checking again
        except Exception as e:
            print(f"Error checking status of Glue job {job_name}: {e}")
            raise

# Define the parameter values to pass
source_name = 'zi_intent'

# Task 1: Start ingest job
start_ingest_job = PythonOperator(
    task_id='start_ingest_job',
    python_callable=run_glue_job,
    op_args=[INGEST_JOB, source_name],
    dag=dag
)

# Task 2: Wait for ingest job completion
wait_ingest_completion = PythonOperator(
    task_id='wait_ingest_completion',
    python_callable=wait_for_glue_job_completion,
    op_args=[INGEST_JOB, "{{ ti.xcom_pull(task_ids='start_ingest_job')['JobRunId'] }}"],
    dag=dag
)

# Task 3: Start lookup job (only if ingest succeeds)
start_lkp_job = PythonOperator(
    task_id='start_lkp_job',
    python_callable=run_glue_job,
    op_args=[LKP_JOB, source_name],
    dag=dag
)

# Task 4: Wait for lookup job completion
wait_lkp_completion = PythonOperator(
    task_id='wait_lkp_completion',
    python_callable=wait_for_glue_job_completion,
    op_args=[LKP_JOB, "{{ ti.xcom_pull(task_ids='start_lkp_job')['JobRunId'] }}"],
    dag=dag
)

# Define task dependencies for sequential execution
stage_Load >> start_ingest_job >> wait_ingest_completion >> start_lkp_job >> wait_lkp_completion >> Load_end
from airflow.decorators import dag, task
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from include.dbt.fraud.cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG
from airflow.models.baseoperator import chain
from datetime import datetime

AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW = 'e4e99186-cad8-4871-bdd6-1db948fe46f2'
AIRBYTE_JOB_ID_LOAD_LABELED_TRANSACTIONS_RAW = '03c06063-271c-4945-a953-deb60c1a3ef5'
AIRBYTE_JOB_ID_RAW_TO_STAGING = '45b2072d-951b-48f2-b0f4-504cd03878e1'

@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['airbyte', 'risk'],
)
def customer_metrics():
    load_customer_transactions_raw = AirbyteTriggerSyncOperator(
        task_id='load_customer_transactions_raw',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW
    )

    load_labeled_transactions_raw = AirbyteTriggerSyncOperator(
        task_id='load_labeled_transactions_raw',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_JOB_ID_LOAD_LABELED_TRANSACTIONS_RAW
    )

    write_to_staging = AirbyteTriggerSyncOperator(
        task_id='write_to_staging',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_JOB_ID_RAW_TO_STAGING
    )

    @task
    def airbyte_jobs_done():
        return True

    # @task.external_python(python='/opt/airflow/soda_venv/bin/python')
    # def audit_customer_transactions(scan_name='customer_transactions',
    #                                 checks_subpath='tables',
    #                                 data_source='staging'):
    #     from include.soda.helpers import check
    #     check(scan_name, checks_subpath, data_source)

    # @task.external_python(python='/opt/airflow/soda_venv/bin/python')
    # def audit_labeled_transactions(scan_name='labeled_transactions',
    #                                checks_subpath='tables',
    #                                data_source='staging'):
    #     from include.soda.helpers import check
    #     check(scan_name, checks_subpath, data_source)

    # @task
    # def quality_checks_done():
    #     return True

    publish = DbtTaskGroup(
        group_id='publish',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models']
        )
    )

    chain(
        [load_customer_transactions_raw, load_labeled_transactions_raw],
        write_to_staging,
        airbyte_jobs_done(),
        # [audit_customer_transactions(), audit_labeled_transactions()],
        # quality_checks_done(),
        publish
    )

customer_metrics()
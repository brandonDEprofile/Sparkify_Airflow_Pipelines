from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    def __init__(
            self,
            redshift_conn_id="",
            tests=[],
            *args, **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

def execute(self, context):
    self.log.info("Starting data quality checks")

    hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

    for test in self.tests:
        self.log.info(f"Running test: {test['check_sql']}")

        records = hook.get_records(test["check_sql"])

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check returned no results: {test['check_sql']}")

        result = records[0][0]

        # ✅ THIS IS THE KEY FIX
        if result <= test["expected_result"]:
            raise ValueError(
                f"Data quality check failed. Query: {test['check_sql']} returned {result}"
            )

        self.log.info(f"Data quality check PASSED: {test['check_sql']} = {result}")

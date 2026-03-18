def execute(self, context):
    self.log.info("Starting data quality checks")

    hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

    for test in self.tests:
        self.log.info(f"Running test: {test['check_sql']}")
        records = hook.get_records(test["check_sql"])

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check returned no results: {test['check_sql']}")

        result = records[0][0]

        # ✅ FIXED LOGIC HERE
        if result <= test["expected_result"]:
            raise ValueError(
                f"Data quality check failed. Query: {test['check_sql']} returned {result}"
            )

        self.log.info(f"Data quality check passed: {test['check_sql']} = {result}")

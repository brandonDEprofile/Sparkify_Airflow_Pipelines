from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    def __init__(
            self,
            redshift_conn_id="",
            table="",
            sql_statement="",
            append_only=False,
            *args, **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.append_only = append_only

    def execute(self, context):

        self.log.info("Starting LoadDimensionOperator")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_only:
            self.log.info(f"Clearing data from dimension table {self.table}")
            redshift.run(f"DELETE FROM {self.table}")

        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_statement}
        """

        self.log.info(f"Loading data into dimension table {self.table}")
        redshift.run(insert_sql)

        self.log.info("LoadDimensionOperator completed successfully")

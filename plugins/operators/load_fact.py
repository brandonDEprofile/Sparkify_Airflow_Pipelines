from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    def __init__(
            self,
            redshift_conn_id="",
            table="",
            sql_statement="",
            *args, **kwargs
    ):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement

    def execute(self, context):

        self.log.info("Starting LoadFactOperator")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_statement}
        """

        self.log.info(f"Loading data into fact table {self.table}")
        redshift.run(insert_sql)

        self.log.info("LoadFactOperator completed successfully")

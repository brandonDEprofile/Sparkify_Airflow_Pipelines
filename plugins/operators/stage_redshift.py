from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook



class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'

    def __init__(
            self,
            redshift_conn_id="",
            aws_credentials_id="",
            table="",
            s3_bucket="",
            s3_key="",
            region="us-east-1",
            json_path="auto",
            *args, **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path = json_path

    def execute(self, context):

        self.log.info("Starting StageToRedshiftOperator")

        aws_hook = AwsBaseHook(aws_conn_id=self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from destination table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            REGION '{self.region}'
            FORMAT AS JSON '{self.json_path}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS
            BLANKSASNULL
            EMPTYASNULL;
        """

        self.log.info(f"Copying data from S3: {s3_path}")
        redshift.run(copy_sql)

        self.log.info("StageToRedshiftOperator completed successfully")

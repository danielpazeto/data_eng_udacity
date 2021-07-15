from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    SQL_COPY = """
        copy {}
        from '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region 'us-west-2'
        json '{}';
    """

    @apply_defaults
    def __init__(self,
                 table,
                 s3_location,
                 json_path = 'auto',
                 redshift_conn_id='redshift',
                 aws_credentials_conn_id ='aws_credentials',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials_conn_id
        self.table = table
        self.s3_location = s3_location
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(StageToRedshiftOperator.SQL_COPY.format(self.table, self.s3_location, credentials.access_key, credentials.secret_key, self.json_path))


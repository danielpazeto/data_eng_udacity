from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 destination_table,
                 insert_query,
                 redshift_conn_id='redshift',
                 aws_credentials_conn_id ='aws_credentials',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials_conn_id
        self.destination_table = destination_table
        self.insert_query = insert_query

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        query = self.insert_query.format(self.destination_table)
        
        redshift_hook.run(query)
        
        self.log.info(f"{self.destination_table} populated")

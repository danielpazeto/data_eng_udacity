from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 destination_table,
                 insert_query,
                 redshift_conn_id='redshift',
                 aws_credentials_conn_id ='aws_credentials',
                 *args, **kwargs):
        """ 
            Load fact tables operator

            :param destination_table: Table where the data will be saved
            :param query_data: Sql statement which will bring the data to be saved
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials_conn_id
        self.destination_table = destination_table
        self.insert_query = insert_query

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        query = self.insert_query.format(self.destination_table)
        
        redshift_hook.run(query)
        self.log.info(f"{self.destination_table} populated")
        
        

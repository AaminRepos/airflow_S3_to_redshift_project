from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    load_sql=""" INSERT INTO {}
                 {} 
             """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 load_sql_stmt,
                 append_only,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql_stmt = load_sql_stmt
        self.append_only = append_only

    def execute(self, context):
           redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
           self.log.info(f"Loading dimension tables")
           load_dim_sql = LoadDimensionOperator.load_sql.format(
               self.table,
               self.load_sql_stmt
           )
           redshift.run(load_dim_sql)
      
           

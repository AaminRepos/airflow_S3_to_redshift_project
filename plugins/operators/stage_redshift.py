from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_stmt="""COPY {}
                 FROM '{}'
                 ACCESS_KEY_ID '{}'
                 SECRET_ACCESS_KEY '{}'
                 REGION '{}'
                 JSON'{}'
                 TIMEFORMAT as 'epochmillisecs'
                 TRUNCATECOLUMNS
                 BLANKSASNULL
                 EMPTYASNULL
                """
    
    @apply_defaults
    def __init__(self,
                 aws_credentials_id,
                 redshift_conn_id,
                 table,
                 s3_bucket,
                 s3_key,
                 json_path,
                 region,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id= aws_credentials_id
        self.redshift_conn_id= redshift_conn_id
        self.table= table
        self.s3_bucket= s3_bucket
        self.s3_key= s3_key
        self.region= region  
        self.json_path= json_path

    def execute(self, context):
           aws_hook = AwsHook(self.aws_credentials_id)
           credentials = aws_hook.get_credentials()
           redshift = PostgresHook(postgres_conn_id= self.redshift_conn_id) 
           self.log.info(f"kobe")
           self.log.info(f"copying new tables from source AWS s3")
           rendered_key = self.s3_key.format(**context)
           s3_path = f"{self.s3_bucket}/{rendered_key}"
            
           
           copied_sql = StageToRedshiftOperator.copy_stmt.format(
               self.table,
               s3_path,
               credentials.access_key,
               credentials.secret_key,
               self.region,
               self.json_path
            )
           redshift.run(copied_sql)
                      
           
                      
          






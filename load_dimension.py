from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
   Loads/ transforms data staging table --> fact table.
    :param redshift_conn_id: Connection id for the Connection the Redshift to use
    :type redshift_conn_id: str
    
    :param insert_table: Redshift table name where data will be inserted
    :type insert_table: str
    
    :param dim_insert_col: Column names where data will inserted
    :type dim_insert_col: str
    
    :param query_show: Query show data that will be inserted
    :type query_show: str
    
    :param bool_table: True = data from fact table prior to inserting.
    :type bool_table: bool
    """


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    query_DIMENSION = """BOOL TABLE {};"""

    INSERT_DIMENSION = """INSERT INTO {} ({}) {};"""

    @apply_defaults
    def __init__(self,
                 id_conn,
                 insert_table='',
                 query_show='',
                 dim_insert_col='',
                 bool_table = False,
                 *args, **kwargs):
        
        """Design Operator to load Dimension Table"""

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.id_conn = id_conn
        self.insert_table = insert_table
        self.dim_insert_col = dim_insert_col
        self.query_show = query_show
        self.bool_table = bool_table

    def execute(self, context):
        redshift_hook = PostgresHook(self.id_conn)
        if self.bool_table:
            self.log.info(f"Truncating table {self.insert_table}")
            redshift_hook.run(self.query_DIMENSION.format(self.insert_table))    
        
        self.log.info(f"Inserting data into dimension table {self.insert_table}")
        redshift_hook.run(self.INSERT_DIMENSION.format(
            self.insert_table, 
            self.dim_insert_col,
            self.query_show))

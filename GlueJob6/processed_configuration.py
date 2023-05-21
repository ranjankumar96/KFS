# Alter champion configuration file
#
# Created By: Ranjan Kumar
#
# Created Date: 12-DEC-2022
#
# Modified Date: 
#
# Reviewed By: 
#
# Reviewed Date: 


# To connect ot Snowflake

# Service Name
aws_service2 = "secretsmanager"

# Secret name for prod
secret_name = "nvsmdvdf01"

# DB secret keys
USERNAME = "USERNAME"
PASSWORD = "PASSWORD"
ACCOUNT = "ACCOUNT"
WAREHOUSE = "WAREHOUSE"
DB = "DB"
SCHEMA = "SCHEMA"

# AWS region 
region_name = "us-east-1"

# S3 Variables

# bucket name for prod
bucket_name = 'kfs.dev.db'
#'nvs3prdf02.aws.demandforecast.analytical.prod'

source_folder = 'Analytical_outputs'

# target data file
target_file_name = "KFS_Orders_Target_Batch.csv"

# Glue job name for prod
tar_glue_job = 'kfs.dev.glue.script6'
#'NVGJPRDF06.AA.alter_champ_rank_shipments'

# Exclude selecting those PF / ITEM whose average 
# monthly quantity is 5 (configure below if required)
filter_volume = 5

# KFS_PROCESSED_ORDERS_TARGET queries
prsd_KFS_orders_table = "KFS_PROCESSED_ORDERS_TARGET"
sel_run_tmp_kfs_order = "select RUN_TIME_STAMP from " + prsd_KFS_orders_table

# Get latest run_time_stamp data query
latest_run_time_stamp_data = "SELECT * FROM KFS_ORDERS_BASELINE_FORECAST2 \
                        where RUN_TIME_STAMP = (\
                SELECT DISTINCT (RUN_TIME_STAMP) \
   FROM KFS_ORDERS_BASELINE_FORECAST2 ORDER BY RUN_TIME_STAMP DESC limit 1 );"

                              
                                                    
# Deletion query
delete_query = "DELETE FROM KFS_ORDERS_BASELINE_FORECAST2 where \
                                        RUN_TIME_STAMP = (\
                SELECT DISTINCT (RUN_TIME_STAMP) \
   FROM     KFS_ORDERS_BASELINE_FORECAST2 ORDER BY RUN_TIME_STAMP DESC limit 1 );"                                                  
# KFS_ERROR_LOG queries
insert_error_log = "INSERT INTO KFS_ERROR_LOG( \
                                          RUN_TIME_STAMP, \
                                          GLUE_JOB, \
                                          EXCEPTION_RAISED_TIME, \
                                          EXCEPTION_KEY, \
                                          EXCEPTION_MESSAGE, \
                                          MESSAGE \
                                         ) VALUES (%s,%s,%s,%s,%s,%s)" 

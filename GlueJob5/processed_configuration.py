# All required constants for champion rank calculation
# have been declared here
#
# Python version used: 3.6.10
#
# Created By: Ranjan Kumar
#
# Created Date: 20-Nov-2022
#
# Reviewed By: 
#
# Reviewed Date: 


# Snowflake DB credentials
USERNAME = "USERNAME"
PASSWORD = "PASSWORD"
ACCOUNT = "ACCOUNT"
WAREHOUSE = "WAREHOUSE"
DB = "DB"
SCHEMA = "SCHEMA"

# Service Name
service_name = "secretsmanager"

# Secret name
secret_name = "nvsmdvdf01"

# Region
region_name = "us-east-1"

# PROCESSED_AA_SHIPMENTS_TARGET queries
prsd_KFS_orders_table = "KFS_PROCESSED_ORDERS_TARGET" 
sel_run_tmp_kfs_order = "select RUN_TIME_STAMP from " + prsd_KFS_orders_table

# Glue job name
tar_glue_job = "kfs.dev.glue.script5"

# ERROR_LOG queries
insert_error_log = "INSERT INTO KFS_ERROR_LOG( \
                                          RUN_TIME_STAMP, \
                                          GLUE_JOB, \
                                          EXCEPTION_RAISED_TIME, \
                                          EXCEPTION_KEY, \
                                          EXCEPTION_MESSAGE, \
                                          MESSAGE \
                                          ) VALUES (%s,%s,%s,%s,%s,%s)"
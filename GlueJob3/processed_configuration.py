
# S3 Folder
aws_service1 = 's3'
# bucket_name = 'nvs3dvdf01.aws.demandforecast.analytical.dev'
bucket_name = 'kfs.dev.db'
folder_name = 'GlueScripts4/Analytical_outputs'

# BU and entity
BU = 'KFS'
entity = 'Orders'

# Analytical output file names
target_file_name = BU + '_' + entity + '_Target_Batch.csv'
rel_file_name = BU + '_' + entity + '_Related_Batch.csv'
itm_mtd_file_name = BU + '_' + entity + '_Itemmeta_Batch.csv'

# To connect ot Snowflake
# Service Name
aws_service2 = "secretsmanager"

# Secret name
# secret_name = "nvsmdvdf01-snowflake"
secret_name = "nvsmdvdf01"

# Glue job name for dev
tar_glue_job = 'kfs.dev.glue.script3'

# Region
region_name = "us-east-1"

# DB secret keys
USERNAME = "USERNAME"
PASSWORD = "PASSWORD"
ACCOUNT = "ACCOUNT"
WAREHOUSE = "WAREHOUSE"
DB = "DB"
SCHEMA = "SCHEMA"

#Forecast horizon
comp_forecast_horizon = 12
buffer = 2

# Reading SKU file
input_sku='Input_SKU'
sku_file_name='SKU_List_new.csv'


# AA_PRODUCT_MASTER queries
#sel_from_prod_master = "select * from AA_PRODUCT_MASTER;"

# KFS_PROCESSED_ORDERS_TARGET queries
prsd_KFS_orders_table = "KFS_PROCESSED_ORDERS_TARGET"
prsd_target_query = "select * from " + prsd_KFS_orders_table
sel_run_tmp_kfs_order = "select RUN_TIME_STAMP from " + prsd_KFS_orders_table

#KFS related data query
prsd_related_query="select * from PROCESSED_FUTURE_ORDER_RELATED;"

# ITEM_METADATA queries
sel_from_itm_mtd = "select * from KFS_ITEM_METADATA;"


# ERROR_LOG queries
insert_error_log = "INSERT INTO KFS_ERROR_LOG ( \
                                          RUN_TIME_STAMP, \
                                          GLUE_JOB, \
                                          EXCEPTION_RAISED_TIME, \
                                          EXCEPTION_KEY, \
                                          EXCEPTION_MESSAGE, \
                                          MESSAGE \
                                         ) VALUES (%s,%s,%s,%s,%s,%s)" 

list_of_cat = [\
               'DEMAND_PROFILE',\
               'PRODUCT_LINE',\
               'BUSINESS_TEAM'\

               ]

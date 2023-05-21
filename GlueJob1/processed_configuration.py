# All required constants for preprocessing target data 
# have been declared here
#
# Python version used: 3.8.12
#
# Created By: Priyanka Srivastava
#
# Created Date: 07-Dec-2022


# S3 Folder
aws_service1 = 's3'
bucket_name = 'kfs.dev.db'
folder_name = 'Delta/'

# BU and entity
BU = 'KFS'
entity = 'Orders'

# Glue job name
tar_glue_job = 'kfs.dev.glue.script'

# Required columns in the orders datasets
req_columns= ["Order Number","Or Ty","Sold To","Sold To Name","Description 1","Request Date","Ship To","2nd Item Number",
              "3rd Item Number","Customer PO","Parent Number","Order Date","Scheduled Pick","Original Promised","Actual Ship",
              "Invoice Date","Cancel Date","Promised Delivery","Branch/Plant","Description Line 2","Quantity Ordered",
              "Quantity Shipped","Quantity Backordered","Quantity Canceled"]

              
# To connect ot Snowflake
# Service Name
aws_service2 = "secretsmanager"

# Secret name
secret_name = "nvsmdvdf01"

# Region
region_name = "us-east-1"

# DB secret keys
USERNAME = "USERNAME"
PASSWORD = "PASSWORD"
ACCOUNT = "ACCOUNT"
WAREHOUSE = "WAREHOUSE"
DB = "DB"
SCHEMA = "SCHEMA"

# Reading SKU file
input_sku='Input_SKU'
sku_file_name='SKU_List_new.csv'


# KFS_PROCESSED_ORDERS_TARGET queries
prsd_KFS_orders_table = "KFS_PROCESSED_ORDERS_TARGET"
del_frm_kfs_order = "delete from KFS_PROCESSED_ORDERS_TARGET"
insert_kfs_order = "INSERT INTO KFS_PROCESSED_ORDERS_TARGET (\
                                                        RUN_TIME_STAMP, \
                                                        ITEM_ID, \
                                                        MONTH_YEAR, \
                                                        UNITS \
                                                        ) \
                                                        VALUES (%s,%s,%s,%s)"
# ERROR_LOG queries
insert_error_log = "INSERT INTO KFS_ERROR_LOG ( \
                                          RUN_TIME_STAMP, \
                                          GLUE_JOB, \
                                          EXCEPTION_RAISED_TIME, \
                                          EXCEPTION_KEY, \
                                          EXCEPTION_MESSAGE, \
                                          MESSAGE \
                                         ) VALUES (%s,%s,%s,%s,%s,%s)" 

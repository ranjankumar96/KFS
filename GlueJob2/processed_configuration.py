# All required constants for preprocessing target data
# have been declared here

# Do not run this code twice in a monthly run

# Python version used: 3.8.12
#
# Created By: Priyanka Srivastava
#
# Created Date: 22-Dec-2022


# S3 Folder
aws_service1 = 's3'
bucket_name = 'kfs.dev.db'
folder_name = 'Delta/'

# BU and entity
BU = 'KFS'
entity = 'Orders'

# Glue job name
tar_glue_job = 'kfs.dev.glue.script2'

# Required columns in the orders datasets
req_columns= ["Order Number","Or Ty","Sold To","Sold To Name","Description 1","Request Date","Ship To","2nd Item Number",
              "3rd Item Number","Customer PO","Parent Number","Order Date","Scheduled Pick","Original Promised","Actual Ship",
              "Invoice Date","Cancel Date","Promised Delivery","Branch/Plant","Description Line 2","Quantity Ordered",
              "Quantity Shipped","Quantity Backordered","Quantity Canceled","Sls Cd3"]

              
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

#Period for which future orders data is required
plus_month_period=12

# Reading SKU file
input_sku='Input_SKU'
sku_file_name='SKU_List_new.csv'

# KFS_PROCESSED_INTER_ORDERS_TARGET queries
prsd_KFS_orders_table = "KFS_PROCESSED_ORDERS_TARGET"
sel_run_tmp_kfs_order = "select RUN_TIME_STAMP from " + prsd_KFS_orders_table

# PROCESSED_FUTURE_ORDER_RELATED queries
#Intermediate Table
prsd_inter_FUTURE_ORDER_table = "PROCESSED_INTER_FUTURE_ORDER_RELATED"
del_frm_inter_order="delete from PROCESSED_INTER_FUTURE_ORDER_RELATED"

#Final Table
# prsd_FUTURE_ORDER_table = "PROCESSED_FUTURE_ORDER_RELATED"
# del_FUTURE_ORDER_query = "delete from " + prsd_FUTURE_ORDER_table
# insert_FUTURE_ORDER_qry = "INSERT INTO PROCESSED_FUTURE_ORDER_RELATED (\
#                                                        RUN_TIME_STAMP, \
#                                                        MONTH_YEAR, \
#                                                        ITEM_ID, \
#                                                        FUTURE_ORDERS \
#                                                        ) \
#                                                        VALUES (%s,%s,%s,%s)"

# ERROR_LOG queries
insert_error_log = "INSERT INTO KFS_ERROR_LOG ( \
                                          RUN_TIME_STAMP, \
                                          GLUE_JOB, \
                                          EXCEPTION_RAISED_TIME, \
                                          EXCEPTION_KEY, \
                                          EXCEPTION_MESSAGE, \
                                          MESSAGE \
                                         ) VALUES (%s,%s,%s,%s,%s,%s)" 

# Forecast pipeline configuration file - ASHLAND

# To connect ot Snowflake

# Service Name
aws_service2 = "secretsmanager"

# Secret name for dev
secret_name = "nvsmdvdf01"   #"nvsmdvdf01-snowflake-kfs"

# Secret name for preprod
# secret_name = "NVSMPPDF01-de-preprod-snow"

# DB secret keys
USERNAME = "USERNAME"
PASSWORD = "PASSWORD"
ACCOUNT = "ACCOUNT"
WAREHOUSE = "WAREHOUSE"
DB = "DB"
SCHEMA = "SCHEMA"


# KFS_PROCESSED_ORDERS_TARGET queries - changed names of files

prsd_KFS_orders_table = "KFS_PROCESSED_ORDERS_TARGET"
sel_run_tmp_kfs_order = "select RUN_TIME_STAMP from " + prsd_KFS_orders_table


# Glue job name
# glue_job = 'NVGJPPDF04.AA.model_to_forecast_final_shipments'

# Glue job name for dev
tar_glue_job = 'kfs.dev.glue.script4'

# pick the algorithmns from below list
alg_lst=['ets','arima','npts','cnn_qr','prophet','deep_ar_plus']


# Generic variables
aws_common_arn_prefix = 'arn:aws:forecast:us-east-1:890585303181:'
role_arn = 'arn:aws:iam::890585303181:role/DFA_Amazon_Forecast'
region_name = "us-east-1"
service_name = "forecast"


# Dataset Variables
dataset_frequency = "M" # Data frequency H: Hourly; M: Monthly
timestamp_format = "yyyy-MM-dd" # The timestamp format in the input data
#FOR DEFINING PROJECT VARIBALE, SEE ABOVE.
# Below variables will be prefixed with project name where '-' replacd with '_'
target_dataset_name = 'target_data' 
related_dataset_name = 'related_data' # Put '' if not required
dataset_group_name = 'datasets' 
item_metadataset_name = 'item_meta_data' #item_meta_data Put '' if not required

# Predictor variables - To change
forecast_horizon = 3 # Give the period for forecasting future 
total_forecast_period = 12 # Long forecasting period--was 18 months earlier
forecast_frequency = "M" # Forecasting type(ex, M: months, H: hours etc,.)
back_test_window_offset = 3 # Data period for evaluting or testing
number_of_back_test_windows = 1 # Number of testing windows to move backwards
Auto_ml = False # Put True/False 


# bucket name for dev
bucket_name = 'kfs.dev.db' #'nvs3devf01.aws.kfs.analytical.dev'
processed_folder_name = 'Intermediate_output'
forecast_output_folder_name = 'Forecast_output'
source_folder = 'Analytical_outputs'

target_file_name = "KFS_Orders_Target_Batch.csv"
related_file_name = "KFS_Orders_Related_Batch.csv"
item_file_name = "KFS_Orders_Itemmeta_Batch.csv" # put '' if item not reqd.




# Schema definition. Make sure, the order of attributes is same as the column 
# order in the input data

# Target schema
target_schema ={
    "Attributes":[
      {
         "AttributeName":"timestamp",
         "AttributeType":"timestamp"
      },
      {
         "AttributeName":"target_value",
         "AttributeType":"float"
      },
      {
         "AttributeName":"item_id",
         "AttributeType":"string"
      }
   ]
}

# Item schema
item_schema ={
    "Attributes":[
      {
         "AttributeName":"item_id",
         "AttributeType":"string"
      },
      {
         "AttributeName":"Demand_Profile",
         "AttributeType":"string"
      },
      {
         "AttributeName":"Product_Line",
         "AttributeType":"string"
      },
      {
         "AttributeName":"Business_Team",
         "AttributeType":"string"
      }
   ]
}

# Related schema. Uncomment and make necessary changes if related data is 
# required

related_schema ={
   "Attributes":[
      {
         "AttributeName":"timestamp",
         "AttributeType":"timestamp"
      },
      {
         "AttributeName":"item_id",
         "AttributeType":"string"
      },
      {
         "AttributeName":"Future_Orders",
         "AttributeType":"float"
      }       
   ]
}

# Final forecast output database loading query.. - names changed

insert_final_output = "INSERT INTO KFS_ORDERS_BASELINE_FORECAST (\
                                                       RUN_TIME_STAMP, \
                                                       ITEM_ID, \
                                                       MONTH_YEAR, \
                                                       FORECAST_METHOD, \
                                                       FORECAST_VALUE, \
                                                       TEST_MAPE, \
                                                       TEST_MAD \
                                                                ) \
                                            VALUES (%s,%s,%s,%s,%s,%s,%s)"
                                            
                                            
                                           
# ERROR_LOG queries
insert_error_log = "INSERT INTO KFS_ERROR_LOG( \
                                          RUN_TIME_STAMP, \
                                          GLUE_JOB, \
                                          EXCEPTION_RAISED_TIME, \
                                          EXCEPTION_KEY, \
                                          EXCEPTION_MESSAGE, \
                                          MESSAGE \
                                         ) VALUES (%s,%s,%s,%s,%s,%s)"
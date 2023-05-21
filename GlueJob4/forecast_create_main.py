#For Ashland

# Forecast pipeline Start script
#
# Python Version: 3.6.10
#
# Input : forecast_configuration.py
#
# Output : NA 
#           
# Description :  Triggers forecasting pipeline
#
# Created By: Ranjan Kumar
#
# Created Date: 22-Nov-2022
#
# Modified Date: 
#
# Reviewed By: 
#
# Reviewed Date: 
#
# List of called programs: parallel_processing
#
# Approximate time to execute each function : 5 mins

# forecast_create_main..

# Loading Libraries..

from snowflake_db_connection import connect_to_db
from kfs_get_latest_run_id import get_latest_run_id
import processed_configuration as con
from error_logging import create_and_insert_error
from parallel_processing import multi_threading
from datetime import datetime
from warnings import simplefilter
simplefilter("ignore")
import sys
from awsglue.utils import getResolvedOptions
import boto3
from datetime import datetime,date,timedelta

#Added on 05Feb23 - Ranjan - Successful
args = getResolvedOptions(sys.argv, ['REGION_NAME', 'SRC_FILE_PATH', 'SNS_TOPIC_ARN'])
region_name = args['REGION_NAME']
SRC_FILE_PATH = args['SRC_FILE_PATH']
sns_Topic_Arn = args['SNS_TOPIC_ARN']

DATE = datetime. now(). strftime("%Y%m%d_%H%M%S") 
#Added on 05Feb23 - Ranjan - Successful

# forecast_create_main..

def forecast_create_main():
    
    # Database connection validation..
    
    try:   
      
        # Get cursor and connection to DB
        cur, conn = connect_to_db()
        
        # Get the latest run_id from processed shipments table
        run_time_stamp = get_latest_run_id(cur, conn)
    
    except Exception as db_connection_exception:
        print ("Database connection error.\n", str(db_connection_exception))
        import sys
        sys.exit(1)
        

    # configuration file schema validation...

    # target schema
    target_keys=['timestamp','target_value','item_id']
    target_values=['timestamp','float','string']
    try:
        for item_record in con.target_schema['Attributes']:
            assert(any(item == item_record['AttributeName'] \
                       for item in target_keys) and \
                   item_record['AttributeType'] ==target_values\
                       [target_keys.index(item_record['AttributeName'])]),\
                        "Attribute name or data type error. Please check \
                            the target schema"

    except Exception as target_schema_exception: 
        print ("please adhere to the standard syntax in the target schema\n",\
               str(target_schema_exception))         
        create_and_insert_error(run_time_stamp)
        raise

    # item schema
    item_keys=['item_id','Demand_Profile','Product_Line','Business_Team']
    
    try:
        for item_record in con.item_schema['Attributes']:
            assert(any(item == item_record['AttributeName']  \
                       for item in item_keys) and \
                   item_record['AttributeType'] =='string'),\
                        "Attribute name or data type error. Please check the \
                            item schema"

    except Exception as item_schema_exception: 
        print ("please adhere to the standard syntax in the item schema\n",\
               str(item_schema_exception))        
        create_and_insert_error(run_time_stamp)
        raise

    # related schema
    related_keys=['timestamp','item_id','Future_Orders']
    related_values=['timestamp','string','float']

    try:
        for item_record in con.related_schema['Attributes']:
            assert(any(item == item_record['AttributeName']  \
                       for item in related_keys) and \
                   item_record['AttributeType'] ==related_values\
                       [related_keys.index(item_record['AttributeName'])]),\
                        "Attribute name or data type error. Please check the \
                            related schema"
                         

    except Exception as related_schema_exception: 
        print ("please adhere to the standard syntax in the related schema\n",\
               str(related_schema_exception))        
        create_and_insert_error(run_time_stamp)
        raise
    

    # Fetching run_time_Stamp and preparing run variables.
    ts = datetime.now()
    
    ###     DEFINING PROJECT VARIBALE ( AUTOMATIC )  ########
    project_name = 'AUTO_'+str(run_time_stamp)+'_'+str(ts.strftime('%H'))+\
                                                str(ts.strftime('%M'))
        
    
    alg_lst = [(alg,project_name,run_time_stamp) for alg in con.alg_lst]

    # Calling multi_threading function from parallel processing script
    try:
        multi_threading(alg_lst,run_time_stamp)
    
        #Added on 06Feb23 - Ranjan - Successful
        sns = boto3.client("sns", region_name=region_name)
        sns.publish(TopicArn=sns_Topic_Arn, 
                Message="Hi All,\n\n DFA Fenwal Glue4 run is successful. Forecast is available in Snowflake Table - KFS_ORDERS_BASELINE_FORECAST \n\nThanks",
                Subject=" DFA Fenwal Glue4 run successful")
        #Added on 06Feb23 - Ranjan - Successful
        
    except Exception as forecast_create_main_exception: 
        print ("Exception occured in the forecast_create_main function.\n",str\
               (forecast_create_main_exception))  
        create_and_insert_error(run_time_stamp)
        
        sns = boto3.client("sns", region_name=region_name)
        sns.publish(TopicArn=sns_Topic_Arn, 
                Message="Hi All,\n\nException occured inside DFA Fenwal Glue4 main file, Kindly check KFS_ERROR_LOG \n\nThanks", 
                Subject="Alert: Exception occured while running")
                
        raise
    
# To execute this module when called
#if __name__ == "__main__":
forecast_create_main()
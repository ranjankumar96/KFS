# Initiates preprocessing of orders data
#
# Python version used: 3.8.12
#
# Description : This main file starts the preprocessing 
#               of the incoming orders data
#
# Coding Steps :
#               1. Create a run id. for this job
#               2. Establish connection to snowflake DB
#               3. Preprocess the orders data for this job
#               4. Write the result to DB
#
# Created By: Priyanka Srivastava
#
# Created Date: 22-Dec-2022

# 1. Import built-in packages and user defined functions

from kfs_get_latest_run_id import get_latest_run_id
from snowflake_db_connection import connect_to_db
from kfs_add_monthly_futureorder_data import add_monthly_data
from error_logging import create_and_insert_error
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


# 2. Definition of the main function

def main():
    """Pre-processes the target orders dataset and 
    updates the orders table.
    
    How it works
    ------------
        1. Creates an alphanumeric job run id. based on 
        the timestamp, BU and entity.
        2. Creates DB connection.
        3. Preprocess the latest incremental monthly orders data.
        4. Write the processed data to the orders table.
    """
    try:
        # Initializing cursor, connection and 
        # run_time_stamp (run id) as empty string
        cur = ""
        conn = ""
        run_time_stamp = ""
        
        # Get cursor and connection to DB
        cur, conn = connect_to_db()
        
        # Creating run id - an alphanumeric name based 
        # on timestamp to identify a job
        run_time_stamp = get_latest_run_id(cur, conn) #Changed here       
        

        # Preprocessing the orders data and 
        # inserting it into db
        add_monthly_data(cur, conn, run_time_stamp)
        
        #Added on 05Feb23 - Ranjan - Successful
        sns = boto3.client("sns", region_name=region_name)
        sns.publish(TopicArn=sns_Topic_Arn, 
                Message="Hi All,\n\n DFA Fenwal Glue2 run is successful. Glue2 output data is available in Snowflake Table  PROCESSED_INTER_FUTURE_ORDER_RELATED \n\nThanks",
                Subject= "DFA Fenwal Glue2 run is successful")
        #Added on 05Feb23 - Ranjan - Successful
        
    except:
        print("Exception occurred inside processed_related_data_create_main.")

        # Creating and logging an error message, in case of an exception
        create_and_insert_error(cur, run_time_stamp)
        sns = boto3.client("sns", region_name=region_name)
        #Added on 05Feb23 - Ranjan - Successful
        sns.publish(TopicArn=sns_Topic_Arn, 
                Message="Hi All,\n\nException occured inside main file, Kindly check KFS_ERROR_LOG \n\nThanks", 
                Subject="Alert: Exception occured while running")
        #Added on 05Feb23 - Ranjan - Successful
        
        raise
    finally:
        # Close the cursor and connection, if exist
        if((cur != "") & (conn != "")):
            print("Closing the cursor and the connection.")
            cur.close()
            conn.close()

            
# To execute this module when called
if __name__ == "__main__":
    print("Calling main() inside processed_related_data_create_main.")
    main()
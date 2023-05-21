# Initiating champion rank calculation 
#
# Python Version: 3.6.10
#
# Description : This main file calls the champion rank
#               stored procedure in Snowflake. 
#
# Coding Steps :
#               1. Establish connection with Snowflake database
#               2. Call the champion rank stored procedure
#
# Created By: Ranjan Kumar
#
# Created Date: 20-Nov-2022
#
# List of called programs: None
#
# Approximate time to execute the code: 1 min


# 1. Import packages and functions

import boto3
import snowflake.connector as snow
import processed_configuration as cfg
from error_logging import create_and_insert_error
from kfs_get_latest_run_id import get_latest_run_id
import sys
from awsglue.utils import getResolvedOptions
import boto3
from datetime import datetime,date,timedelta

args = getResolvedOptions(sys.argv, ['REGION_NAME', 'SRC_FILE_PATH', 'SNS_TOPIC_ARN'])
region_name = args['REGION_NAME']
SRC_FILE_PATH = args['SRC_FILE_PATH']
sns_Topic_Arn = args['SNS_TOPIC_ARN']

DATE = datetime. now(). strftime("%Y%m%d_%H%M%S") 


# 2. Get DB secret keys from AWS Secrets Manager

def get_secret():
    """Get snowflake secret keys and values from AWS secrets manager.

    Returns
    -------
    dict
        Secret keys and values.

    How it works
    ------------
        1. Create a Secrets Manager client using boto3 for the 
        given service and region name.
        2. Then get the secret values using the client 
        for the given secret and return it.

    """
    try:
        print("Inside... get_secret()")

        # Get service, region and secret name from config file
        service_name = cfg.service_name
        region_name = cfg.region_name
        secret_name = cfg.secret_name

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
                                service_name = service_name,
                                region_name = region_name
                                )

        # Get secret keys and values and return the dict
        get_secret_value_response = client.get_secret_value(
            SecretId = secret_name
        )
        
        print("Exiting... get_secret()")
        return get_secret_value_response
    except:
        print("Error occured inside get_secret()")
        raise


# 3. Execute champion rank stored procedure

def main():
    """Execute champion rank stored procedure to calculate
    ranks of all the forecast methods.
    
    How it works
    ------------
        1. Establish connection with Snowflake database.
        2. Execute the cursor to call champion rank stored procedure.
                
    """
    try:
        print("Inside champ_rank_main.")
        
        # Initializing connection
        conn = None
        cur = None
        
        # Get the secret keys from Secrets Manager
        response = get_secret()
        secret_string = eval(response['SecretString']) 
        pwd = secret_string[cfg.PASSWORD]
        user = secret_string[cfg.USERNAME]
        account = secret_string[cfg.ACCOUNT]
        warehouse = secret_string[cfg.WAREHOUSE]
        database = secret_string[cfg.DB]
        schema = secret_string[cfg.SCHEMA]
        #role = secret_string[cfg.key7]
        
        # Establish connection with the Snowflake database
        conn = snow.connect(user=user,
                            account=account,
                            password=pwd,
                            database=database,
                            schema=schema,
                            warehouse=warehouse
                            # role=role
                            )
                        
        # Call the champ_rank() stored procedure 
        # using the Snowflake connection's cursor 
        cur = conn.cursor()
        
        # Initializing run_time_stamp (run id) as empty string
        run_time_stamp = ""
        
        # Get the latest run_id from processed shipments table
        run_time_stamp = get_latest_run_id(cur, conn)
        cur = conn.cursor().execute("call champ_rank()")
        
        print("Calling champion rank procedure in Snowflake")

        query_id1 = cur.sfqid
        
        sns = boto3.client("sns", region_name=region_name)
        sns.publish(TopicArn=sns_Topic_Arn, 
                Message="Hi All,\n\n DFA Fenwal Glue5 run is successful. Champion Forecast is available in Snowflake table KFS_ORDERS_BASELINE_FORECAST2 \n\nThanks",
                Subject=" DFA Fenwal Glue5 run is successful")
        

        #query_status = conn.get_query_status_throw_if_error(query_id1)
        #print("Status of champion rank procedure:",query_status)
    except Exception as e:    
        # Raise exception
        print(e)
        
        # Creating and logging an error message, in case of an exception
        create_and_insert_error(cur, run_time_stamp)
        
        sns = boto3.client("sns", region_name=region_name)
        sns.publish(TopicArn=sns_Topic_Arn, 
                Message="Hi All,\n\nException occured inside DFA Fenwal Glue5, Kindly check KFS_ERROR_LOG \n\nThanks", 
                Subject="Alert: Exception occured while running DFA Fenwal Glue 5")
        raise
    finally:
        # Close the database connection
        if conn is not None:
            print("Closing the connection")
            conn.close() 


# To execute this module when called
if __name__ == "__main__":
    print("Calling main() inside champ_rank_shipments_main.")
    main()
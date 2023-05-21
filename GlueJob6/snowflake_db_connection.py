# Establish connection to DB
#
# Python Version: 3.6.10
#
# Description : Connect to snowflake DB
#
# Coding Steps :
#               1. Get the DB credentials from the config file
#               2. Connect to DB using the credentials
#               3. Return the created cursor and connection
#
# Created By: Ranjan Kumar
#
# Created Date: 15-DEC-2022
#
# Reviewed By: 
#
# Reviewed Date: 
#
# Approximate time to execute the code: 2 sec


# 1. Import packages and functions

import snowflake.connector as snow
import processed_configuration as con
from error_logging import create_and_insert_error
import boto3

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
        service_name = con.aws_service2
        region_name = con.region_name
        secret_name = con.secret_name

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
    except Exception as get_secret_function_exception:
        print("Error occured inside get_secret()",str(get_secret_function_exception))
        
        # Creating and logging an error message, in case of an exception
        create_and_insert_error()
        raise



# 3. Creation of cursor and connection to connect to database

def connect_to_db():
    """Creating connection using the snowflake connector.

    Returns
    -------
    objects
        Cursor and connection objects.

    How it works
    ------------
        1. Creates connection object using the credentials from 
        the config file and the snowflake connector.
        2. Creates cursor using the above connection object.
        3. Returns both objects.

    """
    try:        
        print("Inside... connect_to_db()")

        # Initializing connection and cursor
        connection = ""
        cursor = ""
        
        # Get the secret keys from Secrets Manager
        get_secret_value_response = get_secret()
        secret_string = eval(get_secret_value_response['SecretString']) 
        
        user = secret_string[con.USERNAME]
        pwd = secret_string[con.PASSWORD]
        account = secret_string[con.ACCOUNT]
        warehouse = secret_string[con.WAREHOUSE]
        database = secret_string[con.DB]
        schema = secret_string[con.SCHEMA]
        #role = secret_string[con.key7]
        
        # Connection creation using the snoflake secret values
        connection = snow.connect(
                                  account = account,
                                  user = user,
                                  password = pwd,
                                  database = database,
                                  schema = schema,
                                  warehouse = warehouse
                                  #role = role
                                  )
        
        # Cursor creation using the above created connection
        cursor = connection.cursor()
        
        print("Exiting... connect_to_db()")
        return cursor, connection   
    
    except Exception as connect_to_db_function_exception:
        print("Error occured inside connect_to_db()",str(connect_to_db_function_exception))
        
        # Creating and logging an error message, in case of an exception
        create_and_insert_error()
        raise

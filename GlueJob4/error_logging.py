# Create error message and insert in the error log
#
# Python version used: 3.6.10
#
# Coding Steps:
#               1. Get the current timestamp
#               2. Get the exception type, object and traceback info
#               3. Create the error message
#               4. Update the error log table
#
# Created By: gowdhaman.jayavel
#
# Created Date: 20-Dec-2020
#
# Reviewed By: Asim Pattnaik
#
# Reviewed Date: 02-Jan-2021
#
# List of called programs: None
#
# Approximate time to execute the code: 30 sec


# 1. Import packages and data

from sys import exc_info
from pandas import DataFrame
from pytz import timezone
from datetime import datetime
from traceback import extract_tb
from re import split

import processed_configuration as con


# 2. Creation of exception message and logging it

def create_and_insert_error(run_time_stamp = ""):
    """Create an exception message and insert it into 
    the error log table.

    Parameters
    ----------
    cur : object
        Snowflake DB cursor object.
    run_time_stamp : string
        Timestamp based run id. for the job.

    How it works
    ------------
        1. Get the current timestamp.
        2. Get the exception type, object and traceback info.
        3. Create the error message.
        4. Update the error log table.

    """    
    try:
        print("Inside... create_and_insert_error()")
        
        # Initalising db variables if not passed as parameters.
        from snowflake_db_connection import connect_to_db
        cur, conn = connect_to_db()
                    
        # Get the execution info from sys
        exp_type, exp_obj, exp_tb = exc_info()
        exp_obj = str(exp_obj)
        
        # Get the current timestamp
        tz = timezone('EST')
        now_est = datetime.now(tz)
        now_est = str(now_est)
        
        # Get the exception key or name
        ex_key = exp_type.__name__
        
        # Get the file name where exception happened
        filename = exp_tb.tb_frame.f_code.co_filename
        filename = split(r'/|\\', filename)[-1]
        
        # Get the exception line no.
        line_number = exp_tb.tb_lineno
        
        # Get the function inside which the exception happened
        func_name = extract_tb(exp_tb, 1)[0][2]
        
        # Create the exception message
        message = "Error occurred in line no. " + str(line_number) + \
                  " of file "+ filename + " inside " + func_name +" function."

        print("Exception message: ", message)
        print("Exception: ", exp_obj)
        if len(exp_obj) >=900: exp_obj = exp_obj[:900]
        # Create a temp dataframe with the exception record
        temp_dict = {
                     'RUN_TIME_STAMP':[run_time_stamp],
                     'GLUE_JOB':[con.tar_glue_job], 
                     'EXCEPTION_RAISED_TIME':[now_est[:26]],
                     'EXCEPTION_KEY':[ex_key],
                     'EXCEPTION_MESSAGE':[exp_obj],
                     'MESSAGE':[message]
                    }
        
        df_error = DataFrame(temp_dict)
        data = df_error.values.tolist()
        
        # Get the insert query
        insert_qry = con.insert_error_log
        
        # Insert the exception inside the error log table
        if(cur != ""):
            cur.executemany(insert_qry, data)
            cur.close()
            conn.close()
        else:
            print("Cursor object is empty.")
        
        print("Exiting... create_and_insert_error()")
    
    except:
        print("Exception occured inside create_and_insert_error().")
        raise

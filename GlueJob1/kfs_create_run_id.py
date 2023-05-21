# Run id. creation for the job
#
# Python Version: 3.8.12
#
# Description : Create an alphanumeric run id. to identify the run based on 
#               timestamp, BU and entity

# Coding Steps :
#               1. Get the current timestamp, and BU and 
#                  entity from the config file
#               2. Create the run id. in the format - yyyymmddhhmmssBUEntity
#               3. Return the created run id
#
# Created By: Priyanka Srivastava
#
# Created Date: 07-Dec-2022


# 1. Import built-in packages and user defined functions

from pytz import timezone
from datetime import datetime

import processed_configuration as con


# 2. Creation of run id.

def create_run_id():
    """Create an alphanumeric run id. to identify the run based on 
       timestamp, BU and entity.

    Returns
    -------
    string
        run id. (run_time_stamp)

    How it works
    ------------
        1. Get the current timestamp, and BU and entity from the config file.
        2. Create the run id. in the format - yyyymmddhhmmssBUEntity.
        3. Return the created run id.

    """
    try:
        print("Inside... create_run_id()")
        
        # Initializing run id. - run_time_stamp
        run_time_stamp = ""
        
        # Get the current timestamp in the EST zone
        tz = timezone('EST')
        now_est = datetime.now(tz)
        now_est = str(now_est)
        
        # Create and return the run id.
        run_time_stamp = now_est[:4]+ \
                         now_est[5:7]+ \
                         now_est[8:10]+ \
                         now_est[11:13]+ \
                         now_est[14:16]+ \
                         now_est[17:19]+ \
                         con.BU+ \
                         con.entity
        
        print("Exiting... create_run_id()")
        return run_time_stamp
    except:
        print("Exception occurred while creating run id.")
        raise
 
# Parallel processing - ASHLAND
#
# Python Version: 3.6.10
#
# Input : 
#        1) Algorithm name
#        2) Run time stamp 
#
# Output : NA 
#           
# Description :  Implements multi-processing of forecast algorithms
#
# Steps :
#               1. Creates a new process for each algorithm
#               2. Starts the process
#
# Created By: Buddha Swaroop
#
# Created Date: 29-SEP-2020
#
# Modified Date: 19-FEB-2021
#
# Reviewed By: Sushanth Nalinaksh
#
# Reviewed Date: 22-FEB-2021
#
# List of called programs: start_forecast_process
#
# Approximate time to execute each function : 5 mins

# Loading libraries

from multiprocessing import Process
from start_forecast_process import forecast_process   #---changed name of code

from error_logging import create_and_insert_error
from os import getppid
from time import sleep

# Create a process

def multi_threading(alg_lst,run_time_stamp):

    """Creating a process
    Parameters: 
    argument1 (alg_lst): contains alg short name and project names as strings
    argument2 (run_time_stamp): current job's run id
    Returns: Nothing
    """

    try:
        # Initialising variables and data structures 
        print (alg_lst)
        print('inside multi_threading function')
        print('process ID:', getppid())
        procs = []
        wait_cmd = 1
        
        # creating process for each algorithm
        for alg in alg_lst:
            print (alg)
            p = Process(target=forecast_process, args=(alg,))
            print (p)
            procs.append(p)
            p.start()    
            #sleep(1000)
        
        # while loop to let the program wait till all the child processes 
        # finishes
        while wait_cmd:
            is_alive = []
            for proc in procs:
                proc.join(timeout=0)
                is_alive.append(proc.is_alive())
            wait_cmd = sum(is_alive)
           
    except Exception as multi_threading_exception:
        print ("Exception occured in the multi_threading function.\n",str\
               (multi_threading_exception))  
        create_and_insert_error(run_time_stamp)
        
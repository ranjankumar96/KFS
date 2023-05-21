# AWS_predictor
#
# Python Version: 3.6.10
#
# Input : Target time series data uploaded to the S3 bucket.
#                1) region_name 
#                2) service_name
#                3) hpo_flag
#                4) predictor_name
#                5) forecast_horizon
#                6) dataset_group_arn
#                7) forecast_frequency
#                8) algorithm_arn [optional]
#                9) number_of_backtest_windows [optional]
#                10) back_test_window_offset [optional]
#        
#
# Output : Forecasting Model for  time series data
#
# Description : Creating the forecast model for  time series data
#
# Coding Steps :
#               1. Setup
#               2. Create Predictor
#
# Created By: Buddha Swaroop
#
# Created Date: 16-SEP-2020
#
# Modified Date: 19-FEB-2021
#
# Reviewed By: Sushanth Nalinaksh
#
# Reviewed Date: 22-FEB-2021
#
# List of called programs: None
#
# Approximate time to execute the code: 40 - 90 mins
#
# Loading libraries

from warnings import simplefilter
from time import sleep
# Library for creating session to AWS forecast platfrom
from boto3 import Session 
import notebook_utils as util
from error_logging import create_and_insert_error
simplefilter("ignore")


# Create a Predictor


def create_predictor_function(
    region_name, 
    service_name,
    run_time_stamp,
    hpo_flag,
    predictor_name,
    forecast_horizon,
    dataset_group_arn,
    forecast_frequency,
    algorithm_arn=None,
    number_of_backtest_windows=None,
    back_test_window_offset=None
):

    """Creating predictor
    Parameters:
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id
    argument4 (hpo_flag): Falg enable / disable for hyper parameter tuning
    argument5 (predictor_name): Name of the predictor
    argument6 (forecast_horizon): Future period to forecast as number of months
    argument7 (dataset_group_arn): AWS resource name (unique identifier)
    argument8 (forecast_frequency): Forecasting type
                                    (ex, M: months, H: hours etc,.)
    argument9 (algorithm_arn[optional]): AWS resource name (unique identifier)
    argument10 (number_of_backtest_windows[optional]): Number of testing windows
        (ex, NumberOfBacktestWindows * BackTestWindowOffset) to move backwards
    argument11 (back_test_window_offset[optional]): # Data period for evaluting 
                                                        or testing
    Returns: Nothing
    """
    try:
        # Session setup

        session = Session(region_name=region_name)
        forecast = session.client(service_name=service_name)
    
        # Model building using specific algorithm
        if (
            (algorithm_arn is not None)
            & (number_of_backtest_windows is not None)
            & (back_test_window_offset is not None)
        ):
            create_predictor_response = forecast.create_predictor(
                PredictorName=predictor_name,
                AlgorithmArn=algorithm_arn,
                ForecastHorizon=forecast_horizon,
                PerformAutoML=False,
                PerformHPO=hpo_flag,
                EvaluationParameters={
                    "NumberOfBacktestWindows": number_of_backtest_windows,
                    "BackTestWindowOffset": back_test_window_offset,
                },
                InputDataConfig={"DatasetGroupArn": dataset_group_arn},
                FeaturizationConfig={
                    "ForecastFrequency": forecast_frequency,
                    "Featurizations": [
                        {
                            "AttributeName": "target_value",
                            "FeaturizationPipeline": [
                                {
                                    "FeaturizationMethodName": "filling",
                                    "FeaturizationMethodParameters": {
                                        "frontfill": "none",
                                        "middlefill": "zero",
                                        "backfill": "zero",
                                    },
                                }
                            ],
                        }
                    ],
                },
            )

        else:
            # Auto ML
            create_predictor_response = forecast.create_predictor(
                PredictorName=predictor_name,
                ForecastHorizon=forecast_horizon,
                PerformAutoML=True,
                InputDataConfig={"DatasetGroupArn": dataset_group_arn},
                FeaturizationConfig={
                    "ForecastFrequency": forecast_frequency,
                    "Featurizations": [
                        {
                            "AttributeName": "target_value",
                            "FeaturizationPipeline": [
                                {
                                    "FeaturizationMethodName": "filling",
                                    "FeaturizationMethodParameters": {
                                        "frontfill": "none",
                                        "middlefill": "zero",
                                        "backfill": "zero",
                                    },
                                }
                            ],
                        }
                    ],
                },
            )

        # Checking status of predictor building
        predictor_arn = create_predictor_response["PredictorArn"]
        status_indicator = util.StatusIndicator()
        while True:
            status = forecast.describe_predictor(PredictorArn=predictor_arn)\
                                                                    ["Status"]
            status_indicator.update(status)
            if status in ("ACTIVE", "CREATE_FAILED"):
                break
            sleep(10)
        status_indicator.end()

        # Getting Mapes
        forecast.get_accuracy_metrics(PredictorArn=predictor_arn)

        return 0

    except Exception as predictor_exception:
        print ("Exception caught in the create_predictor_function.\n",
                                       str(predictor_exception))
        create_and_insert_error(run_time_stamp)
        return str(predictor_exception)

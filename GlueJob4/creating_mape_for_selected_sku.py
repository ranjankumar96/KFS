#!/usr/bin/env python
# coding: utf-8

# # Calculating MAPE for top 10 product ids for Oct-Dec 2019 prediction
# Python version used: 3.6.10
#
# Input File: "lcml_srt_it01_data_9b_top_10_summer_export.csv"
#
# Output : "lcml_srt_it01_data_9b_top_10_mape_summer.csv"
#
# Description:
#
#         1. Import the amazon forecast file product ids.
#         2. Calculate the MAPE.
#         3. Export the file with MAPE values.
#
# Created By: Tanumay Misra/Sushanth
#
# Created Date: 17-Sep-2020
#
# Reviewed By: Asim Pattnaik
#
# Reviewed Date: 18-Sep-2020
#
# Approximate time to execute the code: 1 minutes

# Import standard python libraries

#import s3fs
from pandas import DataFrame, concat, melt, read_csv, to_datetime
import numpy as np
import awswrangler as wr
import boto3

def merge_exp_file(all_files):
    frame = DataFrame()
    for filename in all_files:
        try:
            df = wr.s3.read_csv(filename, index_col=None, header=0)
            #df = read_csv('s3://'+filename, index_col=None, header=0)
            frame = concat([frame, df], axis=0, ignore_index=True)
        except Exception as ex:
            print(f"Error while reading the File: {filename}")
            print(f"Error : {str(ex)}")

    cols = frame.columns.tolist()
    frame = frame[cols[1:2] + cols[:1] + cols[2:]]
    frame["item_id"] = frame["item_id"].str.upper()
    frame["item_id"] = frame["item_id"].astype(str)
    return frame

def act_pred_merge(pred_data,act_data):
    pred_data['date'] = pred_data['date'].str.replace("T00:00:00Z", "")
    pred_data['date'] = pred_data['date'].str.upper()
    pred_data['(L0) Product'] = pred_data['item_id'].str.strip()
    pred_data = pred_data.drop(['item_id','p30','p40'], axis = 1) #Change here for different percentiles
    pred_data['p50'] = round(pred_data['p50'], 7)

    act_data['(L0) Product'] = act_data['item_id'].str.strip()
    act_data['date'] = act_data['timestamp']
    act_data = act_data.drop(['item_id','timestamp'],axis =1)
    act_data['(L0) Product'] = act_data['(L0) Product'].str.upper()

    act_pred = act_data.merge(pred_data, on = ['(L0) Product','date'])
    return act_pred

#def calculating_mape(exp_file_loc, actuals_df, forecast_horizon_len, model_name='ETS'):
def calculating_mape(bucket_name, file_key, exp_file_loc, actuals_df, forecast_horizon_len, model_name='ETS'):
    
    # Location should contain forecasted exported files.
    #fs = s3fs.S3FileSystem()
    #list_f = fs.ls(exp_file_loc)
    #list_f = [i for i in list_f if i[-4:] == '.csv']
    session = boto3.session.Session()
    s3 = session.resource("s3")
    bucket = s3.Bucket(bucket_name)
    list_f = ["s3://" + bucket_name + "/" + objects.key for objects in bucket.objects.filter(Prefix=file_key).all() if
              file_key in objects.key and objects.key[-4:] == '.csv']
    merged_frame = merge_exp_file(list_f)
    df_act_prd = act_pred_merge(merged_frame, actuals_df)
    # MAPE calculation
    list_mape = []
    list_product = []
    df_act_prd['p50'] = df_act_prd['p50'].round().astype(int)
    df_act_prd['p50'] = np.where(df_act_prd['p50'] < 0, 0, df_act_prd['p50'])

    # Calculating mape for each SKU
    monthly_list_diff = []
    monthly_list_diff2 = []
    
    for p in df_act_prd["(L0) Product"].unique():
        df_pred = df_act_prd[df_act_prd["(L0) Product"] == p]["p50"].to_list()
        df_act = df_act_prd[df_act_prd["(L0) Product"] == p]["target_value"].tolist()
        list_diff = []
        list_diff2 = []
        
        #For MAPE
        for i in range(forecast_horizon_len):
            if df_act[i] == 0 and df_pred[i] != 0:
                a = 100
            elif df_act[i] == 0 and df_pred[i] == 0:
                a = 0
            else:
#                 Here, if the mape exceeds 100%, we are replacing it with 100%
                a = round(min(abs(df_act[i]-df_pred[i]) / df_act[i], 1), 4)*100
            list_diff.append(a)
        
        # FOR MAD
        # print(df_pred)
        for j in range(forecast_horizon_len):
            a2 = round(abs(df_act[j]-df_pred[j]), 4)
            list_diff2.append(a2)

        # Append all mape into one single list
        mape = sum(list_diff) / len(list_diff)
        mape

        # Append all mape into one single list
        mad = sum(list_diff2) / len(list_diff2)
        mad

        list_product.append(p)
        list_product

        # Create a list for APE per SKU level -FOR MAPE
        monthly_list_diff.extend([[p]+list_diff+[min(mape, 100)]])
        monthly_list_diff

        # Create a list for APE per SKU level -FOR MAD
        monthly_list_diff2.extend([[p]+list_diff2+[mad]])
        monthly_list_diff2
        
    list_diff
    list_diff2

    # After all the skus mape are compiled - run this
    monthly_ape = DataFrame(monthly_list_diff, columns=['item_id']+list(df_act_prd.date.unique())+['OVERALL'])
    monthly_ape

    # After all the skus mape are compiled - run this
    monthly_mad = DataFrame(monthly_list_diff2, columns=['item_id']+list(df_act_prd.date.unique())+['OVERALL'])
    monthly_mad

    # Transposing the dataset
    monthly_ape = melt(monthly_ape, id_vars=['item_id'], value_vars=list(df_act_prd.date.unique())+['OVERALL'],
            var_name='month', value_name='perc_error_MAPE')
    monthly_ape

    # Transposing the dataset -FOR MAD
    monthly_mad = melt(monthly_mad, id_vars=['item_id'], value_vars=list(df_act_prd.date.unique())+['OVERALL'],
            var_name='month', value_name='perc_error_MAD')
    monthly_mad

    # joining mape & mad datasets here
    import pandas as pd
    monthly_ape2=pd.merge(monthly_ape,monthly_mad,how='left',on=["item_id","month"])
    print(monthly_ape2.shape)
    monthly_ape2

    monthly_ape2['model'] = model_name +"_P50"   #Changed name here
    monthly_ape2

   
    return monthly_ape2

# Preprocessing of monthly orders and updating table
#
# Python Version: 3.8.12
#
# Description : Preprocessing the orders data and updating the orders 
#               table
#
# Coding Steps :
#               1. Read the orders data from S3
#               3. Treat negative orders and calculate the orders 
#                  for all item_id
#               4. Write the processed orders data to the orders table
#
# Created By: Priyanka Srivastava
#
# Created Date: 07-Dec-2022

# 1. Import built-in packages and user defined functions

import pandas as pd
import numpy as np
import os
from boto3 import resource
from io import BytesIO
from pytz import timezone
from datetime import datetime, date
from calendar import monthrange, month_abbr
from pandas import read_csv, read_excel, concat, read_sql
from dateutil.relativedelta import relativedelta
import awswrangler as wr

import processed_configuration as con
from kfs_data_cleaning import transform_data_and_write_db
from error_logging import create_and_insert_error
from snowflake_db_connection import connect_to_db


# 2. Preprocessing the orders data

def add_monthly_data(cur, conn, run_time_stamp):
    """Preprocessing the orders data and updating the orders table.

    Parameters
    ----------
    cur : object
        Snowflake DB cursor object.
    conn : object
        Snowflake DB connection object.        
    run_time_stamp : string
        Timestamp based run id. for the job.

    How it works
    ------------
        1. Read the incremental current month
        orders data from S3.
        2. Filter data for 590 sku. Exclude Order Types FH, FL, FP, FQ, FS, FV, ZA, ZB, ZO,
        which in turn removes 1 item (Hence, left with 589 skus).
        3. Aggregate Quantity_Ordered at Item_id and date level.
        4. Replace negative orders with 0.
        5. Write the processed orders data to the orders table.

    """
    try:
        print("Inside... add_monthly_data()")
        
        # Get the current time, month and year in EST zone
        tz = timezone('EST')
        now_est = datetime.now(tz)
        curr_mon = now_est.strftime("%b")
        curr_yr = now_est.strftime("%Y")
        
        # Connect to AWS S3 bucket where the orders file reside
        s3 = resource(con.aws_service1)
        bucket = s3.Bucket(con.bucket_name)
        prefix = con.folder_name
        prefix_objs = bucket.objects.filter(Prefix = prefix)

        # Initializing the list of dataframes that will
        # hold incremental monthly data
        prefix_dfs = []
        
        # Setting the file available indicator to false
        incre_file_ind = False
        
        # Iterating through the objects in S3 bucket folder
        # to find incremental monthly file
        for obj in prefix_objs:
            name, extension = os.path.splitext(obj.key)
            body = obj.get()['Body'].read()
            if 'incremental' in str(obj).lower():
                if(extension == '.csv'):
                    temp = read_csv(BytesIO(body))
                elif(extension == '.xlsx'):
                    temp = read_excel(BytesIO(body),engine='openpyxl')
                else:
                    raise Exception( \
                            "Incremental or current month data file is not " + 
                            "in the expected format (csv or xlsx) for the " +
                            curr_mon + "-" + curr_yr + " run."
                           )
                    
                if(temp.shape[0] == 0):
                    raise Exception( \
                            "Incremental data file is empty for the " +
                            curr_mon + "-" + curr_yr + " run."
                           )
                
                prefix_dfs.append(temp)
                incre_file_ind = True
        
        # Raising the resp. exception message if
        # the file is not found
        if (not incre_file_ind):
            raise Exception( \
                            "Incremental data is missing for the " + 
                            curr_mon + "-" + curr_yr + " run."
                           )
            
        # Concatenating the list of dfs
        if(len(prefix_dfs)>0):
            for i in range(len(prefix_dfs)):
                prefix_dfs[i] = prefix_dfs[i][con.req_columns]
            df_monthly = concat(prefix_dfs, axis = 0, ignore_index = True)
        else:
            raise Exception( \
                            "Orders data is not available for the "
                            + curr_mon + "-" + curr_yr + " run."
                           )
                           
        print("Shape of Incremental data read from S3 : ",df_monthly.shape)
        
        # Added - 2Feb23 For removing blank rows
        df_monthly=df_monthly[~((df_monthly["3rd Item Number"].isna()) | (df_monthly["3rd Item Number"]==""))]
        print("Shape of Incremental data post removing blank rows : ",df_monthly.shape)

#         Dropping last row    
#         df_monthly.drop(df_monthly.tail(1).index,inplace=True)

        # Filling spaces in column names with "_"
        df_monthly.columns=df_monthly.columns.str.strip().str.replace(' ', '_')

        # Keeping track of original names of SKUs
        df_monthly["3rd_Item_Number_orig"]=df_monthly["3rd_Item_Number"]
        df_monthly["2nd_Item_Number_orig"]=df_monthly["2nd_Item_Number"]

        # Replacing columns with " " values with ""
        cols_to_strip=['3rd_Item_Number','2nd_Item_Number','Or_Ty']

        for cl in cols_to_strip:
#             df_monthly[cl]=df_monthly[cl].str.strip() #Commented on 9Feb23
            df_monthly[cl]=df_monthly[cl].astype(str).str.strip()
            df_monthly[cl]=df_monthly[cl].str.upper()
        
        print("Number of SKUs before filtering order types : ", df_monthly["3rd_Item_Number"].nunique())
        
        # 2. Excluded Order Types FH, FL, FP, FQ, FS, FV, ZA, ZB, ZO
        df_monthly["Flag_step2"]=np.where(df_monthly["Or_Ty"].isin(["FH","FL","FP","FQ","FS","FV","ZA","ZB","ZO"]),1,0)
        
        # Dropping excluded order type rows
        df_monthly2=df_monthly.query("Flag_step2 == 0")
        print("Data left after excluding 9 order types for ALL SKUs : ",df_monthly2.shape)
        
        print("Number of SKUs left after filtering order types : ", df_monthly2["3rd_Item_Number"].nunique())
        
        # Extracting date from Request_Date column in
        # the format Mon-yyyy
        df_monthly2['mm'] = df_monthly2['Request_Date'].dt.month
        
        df_monthly2['mon'] = df_monthly2['mm'].apply(lambda x: month_abbr[x])
        
        print("Count of Months : ", df_monthly2['mon'].value_counts(dropna=False))
        
        df_monthly2['mon_yyyy'] = df_monthly2['mon'] + "-" + df_monthly2['Request_Date'].dt.year.astype(str)
        print(df_monthly2.head(1))
        
        # Creating a list of periods for which the data is available
        periods2 = list((df_monthly2['mon_yyyy']).unique())
        print("periods2 : ",periods2)
        
        periods = [x for x in periods2 if str(x) != 'nan']
        print("periods after removing nan if it exists : ",periods)
        
        print("Unique Item_id : " ,df_monthly2["3rd_Item_Number"].nunique())
        
        # Obtaining the periods for which the data is already available in the processed orders table
        print("Reading the Historical orders table ...",con.prsd_KFS_orders_table)
        kfs_proc_order2= "select * from " + con.prsd_KFS_orders_table
        kfs_proc_order=read_sql(kfs_proc_order2, conn)
        
        print("Reading MONTH_YEAR column from historical orders table ...")
        sel_mon_yrs_from_kfs_order = "select MONTH_YEAR from " + con.prsd_KFS_orders_table
        mon_yrs_prsd_order2=read_sql(sel_mon_yrs_from_kfs_order, conn)
        mon_yrs_prsd_order = list(mon_yrs_prsd_order2["MONTH_YEAR"].unique())
        print("Unique list of MONTH_YEAR in historical orders table is : ", mon_yrs_prsd_order)
        
        # Initializing a list of dfs to store the resulting dfs
        df_results = []

        # Converting period (Mmm-yyyy) to dates (yyyy-mm-dd)
        period_dates = []

        for period in periods:
            year = int(period[-4:])
            month = list(month_abbr).index(period[:3])
            day = 1
            date_time_str = str(year) + '-' + str(month) +'-' + str(day)
            period_date = datetime.strptime(date_time_str, '%Y-%m-%d')
            period_dates.append(period_date)
        print("period_dates : " , period_dates)
        
        # Iterating through each available period (Mon-yyyy)
        for period in periods:
            if((period != 'nan') & (period != 'None')):

                print('period-->', period)
                # Pulling only records of the corresponding period
                temp1 = df_monthly2[df_monthly2['mon_yyyy'] == period].copy()

                # Setting the update indicator to true if the data for the 
                # period is already available in the processed orders table
                update_ind = period in (mon_yrs_prsd_order)
                print("update_ind : ", update_ind)
                
                #Sorting the data by Item_id & Request_Date
                temp2=temp1[["3rd_Item_Number","Request_Date","Quantity_Ordered"]].sort_values(by=["3rd_Item_Number","Request_Date"])
                print("Shape of dataset sorted at Item_id & Request_Date : ", temp2.shape)
                
                # New addition - Removing blank item_ids
                temp2=temp2[temp2["3rd_Item_Number"]!=""]
                print("Shape of dataset post removal of blank item_ids : ", temp2.shape)
                
                # Creating a Date column which will be 1st of the month
                temp2["Date"]=temp2["Request_Date"].to_numpy().astype('datetime64[M]')
                print("Shape of dataset after creating Date column : ", temp2.shape)
                
                # Aggregating the Quantity_Ordered at Monthly level
                temp2=temp2.groupby(["3rd_Item_Number","Date"])["Quantity_Ordered"].sum().reset_index()
                print("Shape of dataset on aggregating the data at Monthly level : ", temp2.shape)
                
                # 2. Checking if there's any negative orders in Quantity_Ordered per month per sku!
                temp2["Flag"]=np.where(temp2["Quantity_Ordered"]<0,1,0)
                temp2["Quantity_Ordered_n"]=np.where(temp2["Quantity_Ordered"]<0,0,temp2["Quantity_Ordered"])
                temp2.drop(columns={"Quantity_Ordered"},inplace=True)
                temp2.rename(columns={"Quantity_Ordered_n":"Quantity_Ordered"},inplace=True)
                print("Shape of dataset after treating for negative orders : ", temp2.shape)
                
                print("Flag counts : ", temp2["Flag"].value_counts(dropna=False))
                
                # Dropping column not required
                temp2.drop(columns={"Flag"},inplace=True)
                print("Data after removing Flag column : ", temp2.head(1))
              
                # History data shouldnt have current months' data
                data_hist=kfs_proc_order[kfs_proc_order["MONTH_YEAR"]!=period]
                
                print("Shape of historical dataset : ", data_hist.shape)
                
                data_hist["ITEM_ID"]=data_hist["ITEM_ID"].astype(str) #Added 9thFeb23
                
                data_hist["year"]=data_hist["MONTH_YEAR"].str[-4:]
                data_hist["month"]=data_hist["MONTH_YEAR"].str[:3]
                data_hist["month"]=pd.to_datetime(data_hist.month, format='%b').dt.month
                data_hist["day"]=1

                data_hist["date_time_str"] = data_hist["year"].astype('str') + '-' + data_hist["month"].astype('str') +\
                '-' + data_hist["day"].astype('str')

                data_hist["Date"]=pd.to_datetime(data_hist['date_time_str'], format='%Y-%m-%d')
                print("data_hist : ", data_hist.head(1))
                
                data_hist.drop(columns={"year","MONTH_YEAR","month","day","date_time_str"},inplace=True)
                print("Data types of data_hist : ", data_hist.dtypes)
                
                # Minimum value as per group
                dates=data_hist.groupby('ITEM_ID')['Date'].min().to_frame(name='min')
                print("Shape of dates dataset: ", dates.shape)
                
                print("Min date in Snowflake history dataset: ", data_hist["Date"].min(),\
                      "Max date in Snowflake history dataset: ", data_hist["Date"].max())
                
                dates=dates.reset_index()
                print("Data types of dates dataset: ", dates.dtypes)
                
                # New addition - incremenatl month data
                temp3=temp2.drop_duplicates(subset=["3rd_Item_Number"])
                temp3=temp3[["3rd_Item_Number"]]
                temp3.rename(columns={"3rd_Item_Number":"ITEM_ID"},inplace=True)
                print("No of ITEM_IDs in Incremental dataset:", temp3.shape)
                
                # New addition
                dates2=dates[["ITEM_ID"]]
                dates_f=pd.concat([dates2,temp3])
                print("No of ITEM_IDs whose data for current month is to be created - may include duplicates: ", dates_f.shape)
                
                # New Addition
                dates_f2=dates_f.drop_duplicates(subset=["ITEM_ID"]) #Dropping duplicates, if any from final list of ITEM_IDs
                dates_f2["max"]=temp2["Date"].max()  #Setting max date to current month date
                dates_f2["max"] = pd.to_datetime(dates_f2["max"])
                print("No of ITEM_IDs whose data for current month is to be created : ", dates_f2.shape)
                
                # Joining the DataFrame with the actual data to get the Quantity_Ordered populated for incremental month
                temp4=pd.merge(dates_f2,temp2,how='left',left_on=["ITEM_ID","max"],right_on=["3rd_Item_Number","Date"])
                print("Shape of dataset after merging: ", temp4.shape)
                
                # Filling data with blanks with 0
                temp4["Quantity_Ordered"].fillna(0,inplace=True)
                
                print("Count of Date in temp4 dataset: " , temp4["Date"].value_counts(dropna=False))
                
                print("Count of max in temp4 dataset: " , temp4["max"].value_counts(dropna=False))
                
                # Dropping & Renaming columns
                temp4.rename(columns={"max":"timestamp","Quantity_Ordered":"target_value","ITEM_ID":"item_id"},inplace=True)
                temp4.drop(columns={"3rd_Item_Number","Date"},inplace=True)
                print("Shape of dataset after renaming and dropping columns: ", temp4.shape)
                print("temp4 dataset: ", temp4.tail(2))
                
#                 Working on historical dataset to check item_id's with 0 demand in last 2 years
                data_hist2=data_hist.rename(columns={"ITEM_ID":"item_id","UNITS":"target_value","Date":"timestamp"})
                data_hist2.drop(columns={"RUN_TIME_STAMP"},inplace=True)
                print("Shape of historical dataset :" , data_hist2.shape)
            
                # Appending history and incremental datasets
                data_all=pd.concat([data_hist2,temp4])
                print("Shape of combined dataset : ", data_all.shape)
                
                print("Maximum timestamp : ", data_all["timestamp"].max())
                
                # Checking which all SKUs doesnt have any orders in past 2 years
                two_years = data_all["timestamp"].max() + relativedelta(months=-24)
                print("Date for 2 years : ", two_years)
                
                data_all["Date_Tag"]=np.where((data_all["timestamp"]>two_years),"Last 2 year","Other")
                
                grp=data_all.groupby(["item_id","Date_Tag"])["target_value"].sum().reset_index()
                print("Shape of grp table : ", grp.shape)
                
                item_emp=grp[(grp["target_value"]==0) & (grp["Date_Tag"]=="Last 2 year")]
                print("No of items in combined dataset which has 0 demand in past 2 years : ", item_emp.shape)
                
                #Converting the above ontained items to a list
                sku_list_2yrs=item_emp["item_id"].tolist()
                print("List of skus which has 0 demand in past 2 years : ", sku_list_2yrs)
                
                #We would want to exclude items from above list which were provided by Business
                # File path of SKUs to be forecasted
                sku_file = 's3://' + con.bucket_name + '/' + con.input_sku + '/' + con.sku_file_name
                print("sku_file :", sku_file)
                
                target_sku = wr.s3.read_csv(sku_file)
                print("No of SKUs provided by Business : ", target_sku.shape)
                print("target_sku :", target_sku)
                
                # Convert to string
                target_sku["combined PN"]=target_sku["combined PN"].astype(str)
                target_sku["combined PN"]=target_sku["combined PN"].str.upper()
                
                # Removing leading and trailing spaces
                target_sku["combined_PN_new"]=target_sku["combined PN"].astype(str).str.strip()
                
                # Checking if theres any NaN values in item list
                target_sku[target_sku["combined_PN_new"].isna()]
                
                target_sku["Flag"]=np.where((target_sku["combined_PN_new"]==target_sku["combined PN"]),1,0)
                
                # To check how many SKUs has changed post removing leading and trailing spaces
                print("Flag counts : ", target_sku["Flag"].value_counts(dropna=False))
                
                # Dropping duplicates in #SKUs
                target_sku2=target_sku.copy()
                target_sku2=target_sku2[["combined_PN_new"]].drop_duplicates()
                print("target_sku2 :", target_sku2)
                
                # Converting the new SKUs names to a list - Has been added in main code
                sku_list=target_sku2["combined_PN_new"].tolist()
                print("sku_list : ", sku_list)
                
                # Removing sku_list provided by Filip from sku_list_2yrs
                set1 = set(sku_list) #SKUs list provided by Filip
                print(len(set1))
                set2 = set(sku_list_2yrs) #SKUs with 0 demand since 2 years
                print(len(set2))
                res = list(set2 - set1) #We need to exclude SKUs provided by business for forecasting purpose
                print(len(res))
                
                print(res)
                
                # Changing elements in list to a str in order to write sql code
                if len(res)==1:
                    fin_lst=str(tuple(res)).replace(",)",")")
                    print("Length of res is 1 & element in it is : ",fin_lst)
                    print(type(fin_lst))
                else:
                    fin_lst=tuple(res)
                    fin_lst = str(fin_lst)
                    print("Length of res is NE 1 & elements in it are : ",fin_lst)
                    print(type(fin_lst))
                    
                                
                # Get the delete and insert queries from config file
                delete_qry_item = con.del_frm_kfs_order + " where ITEM_ID in " + \
                             fin_lst + ""
                print("delete query : ", delete_qry_item)
                
                print("length of resultant skus whose data needs to be deleted from historical order table :",len(res))
                
                if len(res)>0:
                    cur.execute(delete_qry_item)
                    
                # Removing these skus from incremental data as well if exists
                temp5=temp4[~temp4["item_id"].isin(res)]
                print("Shape of temp5 dataset post removing 0 demand items : ", temp5.shape)
                
#                 Sorting the data
                temp5=temp5.sort_values(by=["item_id","timestamp"])
                
  
                # Transforming the data for the corresponding period
                # and appending the resulting df to the list
                df_results.append(transform_data_and_write_db( \
                                                              period,
                                                              temp5,
                                                              update_ind,
                                                              run_time_stamp,
                                                              cur
                                                             )
                                 )
                
        print("Exiting... add_monthly_data()")
        
    except:
        print("Exception occurred inside add_monthly_data()")
        
        # Creating and logging an error message, in case of an exception
        create_and_insert_error(cur, run_time_stamp)
        raise
        
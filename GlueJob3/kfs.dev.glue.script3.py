# Code modified on 3Feb23


from snowflake_db_connection import connect_to_db
from kfs_get_latest_run_id import get_latest_run_id
from kfs_aws_time_series_format import outlier_treatment
from kfs_concat_for_related import concat_for_rel
from kfs_creating_item_metadata import creating_item_metadata
from error_logging import create_and_insert_error
import processed_configuration as con #Added
import awswrangler as wr #Added

def main():
    try:
        cur = ""
        conn = ""        
        run_time_stamp = ""
        
        cur, conn = connect_to_db()
        run_time_stamp = get_latest_run_id(cur, conn)
        
        #Added on 3Feb23 - Priyanka - starts
        # File path of SKUs to be forecasted
        sku_file = 's3://' + con.bucket_name + '/' + con.input_sku + '/' + con.sku_file_name
        print("sku_file :", sku_file)
        
        target_sku = wr.s3.read_csv(sku_file)
        print(target_sku.shape)
        
        print("Shape of SKUs in the AWS bucket :", target_sku.shape)
        print("target_sku :", target_sku)
        
        # Using Series.astype() to convert to string
        target_sku["combined PN"]=target_sku["combined PN"].astype(str)
        target_sku["combined PN"]=target_sku["combined PN"].str.upper()
        
        # Removing leading and trailing spaces
        target_sku["combined_PN_new"]=target_sku["combined PN"].astype(str).str.strip()
        print(target_sku.shape)
        target_sku
        
        # Dropping duplicates in #SKUs
        target_sku2=target_sku.copy()
        target_sku2=target_sku2[["combined_PN_new"]].drop_duplicates()
        # print("Unique SKUs to be forecasted : ", target_sku2.shape)
        print("target_sku2 :", target_sku2)
        
        # Converting the new SKUs names to a list - Has been added in main code - This list will be forecasted!!!!!!!!
        sku_list=target_sku2["combined_PN_new"].tolist()
        print(len(sku_list))
        sku_list 
        #Added on 3Feb23 - Priyanka - ends        
        
        data,min_date,max_date,pf_tos_list=outlier_treatment(run_time_stamp, cur, conn, sku_list) #Added sku_list
        all_related = concat_for_rel(\
                                     min_date, 
                                     max_date, 
                                     run_time_stamp,
                                     cur,
                                     conn,
                                     sku_list
                                    )
        creating_item_metadata(\
                               pf_tos_list, 
                               run_time_stamp,
                               cur,
                               conn
                              )
    except:        
        print("Exception occured inside processed_related_data_create_main.")
        
        # Creating and logging an error message, in case of an exception
        create_and_insert_error(cur, run_time_stamp)
        raise
    finally:
        # Close the cursor and connection, if exist
        if((cur != "") & (conn != "")):
            print("Closing the cursor and the connection.")
            cur.close()
            conn.close()
            

# To execute this module when called
if __name__ == "__main__":
    print("Calling main() inside processed_analytical_data_create_main.")
    main()
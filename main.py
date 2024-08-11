import calendar
import pandas as pd
import numpy as np
import seaborn as sns
import glob
import os
from datetime import time,datetime,timedelta
from holidays import holidays
from modules import files_path, date_formater, find_previous_straddle_bid, find_previous_straddle_ask



folder_path = "E:\\Backtesting\\new_data_format"

df_location = files_path(folder_path)

df_straddle_cleaned = pd.DataFrame()
df_options_cleaned = pd.DataFrame()
for folder in df_location['location'].unique():
    #print(folder)
    # Get the date from the folder name
    # date = folder.split("\\")[-1]  # -2 because the path ends with a slash

    # Get all CSV files in the folder
    csv_files = glob.glob(os.path.join(folder, '*.csv'))
    df_straddle = pd.DataFrame()
    
    # Initialize an empty DataFrame to store combined data
    #df_straddle = pd.DataFrame()

    for file in csv_files:
        try:
            if os.path.getsize(file) < 10720:
                print(f"Skipping file due to size < 10KB: {file}")
                continue
           
            # Read each CSV file
            df = pd.read_csv(file, low_memory=False)
            
            # Skip empty DataFrames
            if df.empty:
                print(f"Empty DataFrame: {file}")
                continue
            
            if len(df) <= 5:
                print(f"Skipping file {file} because it has only {len(df)} rows.")
                continue
#             # Filter out rows where most of the data is missing
#             df = df.dropna(thresh = int(df.shape[1] * 0.5))  # Adjust the threshold as needed
            
#             # Drop rows where all elements are missing
#             df = df.dropna(how='all')
            
            #Skip the file if it has too many missing values or certain key columns are missing
            if df.isnull().sum().sum() > df.size * 0.5:  # More than 50% missing values
                print(f"Skipping file due to too many missing values: {file}")
                continue

            # List of key columns that must be present
#             key_columns = ['Unnamed: 0', 'atm_leg1', 'contract_leg1']
#             if not all(column in df.columns for column in key_columns):
#                 print(f"Skipping file due to missing key columns: {file}")
#                 continue
            
            df_straddle = pd.concat([df_straddle, df], ignore_index=True)
        except pd.errors.EmptyDataError:
            # Skip the file if it's empty
            print(f"EmptyDataError: {file} is empty and has been skipped.")
        except Exception as e:
            # Catch other potential errors and continue processing
            print(f"Error processing file {file}: {e}")
    # break
    if not df_straddle.empty:
        df_straddle.rename(columns = {df_straddle.columns[0] : 'datetime'} , inplace = True)
        df_straddle['time'] = df_straddle['datetime'].apply(lambda x: x.split(' ')[-1])
        df_straddle['date'] = df_straddle['datetime'].apply(lambda x: x.split(' ')[0])
        df_straddle['option_type'] = df_straddle['contract_leg1'].apply(lambda x: x[-2:])
        df_straddle['exp_date'] = df_straddle['contract_leg1'].apply(lambda x: x[5:10])

        # Convert custom date format to standard datetime format
        df_straddle['exp_date'] = df_straddle['exp_date'].apply(date_formater)

        columns = ['datetime','date','time', 'atm_leg1','option_type','exp_date','contract_leg1', 'ltq_leg1', 'iv1_leg1', 'vtt_leg1',
                    'ask_qty_leg1', 'ltp_high_leg1', 'ltp_low_leg1', 'theta_leg1',
                    'oi_leg1', 'delta_leg1', 'iv2_leg1', 'ltp_close_leg1', 'ltp_open_leg1',
                    'ask_leg1', 'bid_leg1', 'low_price_leg1', 'atp_leg1', 'high_price_leg1',
                    'vega_leg1', 'close_price_leg1', 'bid_qty_leg1', 'open_price_leg1',
                    'iv_leg1', 'gamma_leg1', 'ltt_leg1', 'underlying_price']
        df_straddle = df_straddle[columns]
        df_straddle = df_straddle.drop_duplicates(subset=columns).reset_index(drop=True)
        # convert date column to datetime format
        df_straddle['date'] = pd.to_datetime(df_straddle['date'] ,format='%Y-%m-%d', errors='coerce').dt.strftime('%d-%b-%Y')

        # Fill NaN values in 'date' column with forward fill (ffill) and then backward fill (bfill)
        df_straddle['date'] = df_straddle['date'].ffill()
        df_straddle['time'] = pd.to_datetime(df_straddle['time'], format='mixed').dt.time
        df_straddle['atm_leg1'] = df_straddle['atm_leg1'].astype(int)
        df_straddle['atm'] = (round(df_straddle['underlying_price']/50)*50).astype(int) 

                        ########################### calculating atm straddle ########################3

        # Separate DataFrames for CE and PE options
        df_ce = df_straddle[df_straddle['option_type'] == 'CE'][['atm_leg1', 'date', 'time','exp_date', 'bid_leg1','ask_leg1']]
        df_pe = df_straddle[df_straddle['option_type'] == 'PE'][['atm_leg1', 'date', 'time','exp_date', 'bid_leg1','ask_leg1']]

        # Rename columns to distinguish between CE and PE
        df_ce.rename(columns={'bid_leg1': 'bid_leg1_ce', 'ask_leg1': 'ask_leg1_ce'}, inplace=True)
        df_pe.rename(columns={'bid_leg1': 'bid_leg1_pe', 'ask_leg1': 'ask_leg1_pe'}, inplace=True)

        # Merge the CE and PE DataFrames on 'atm_leg1', 'date', and 'time'
        df_merged = pd.merge(df_ce, df_pe, on=['atm_leg1', 'date', 'time','exp_date'])

        # Calculate the straddle as the sum of 'bid_leg1_ce' and 'bid_leg1_pe'
        df_merged['straddle_bid'] = df_merged['bid_leg1_ce'] + df_merged['bid_leg1_pe']
        df_merged['straddle_ask'] = df_merged['ask_leg1_ce'] + df_merged['ask_leg1_pe']

        df_merged.rename(columns={'atm_leg1': 'atm'}, inplace=True)
        
        # Merge the straddle values back to the original DataFrame
        df_straddle = pd.merge(df_straddle, df_merged[['atm', 'date', 'time','exp_date', 'straddle_bid','straddle_ask']], 
                                left_on=['atm', 'date', 'time','exp_date'], 
                                right_on=['atm', 'date', 'time','exp_date'], 
                                how='left')
        
        df_options_cleaned = pd.concat([df_options_cleaned,df_straddle], ignore_index=True)
        
        df_straddle = df_straddle.drop_duplicates(subset=['time']).reset_index(drop=True)
        
                            #######################################
        
        # Assuming you want to drop certain columns from drop and assign the result to df_straddle
        df_straddle = df_straddle.drop(columns=['atm_leg1', 'option_type', 'contract_leg1',
                                        'vtt_leg1', 'bid_qty_leg1', 'close_price_leg1', 'atp_leg1',
                                        'delta_leg1', 'ltp_high_leg1', 'gamma_leg1', 'ltp_close_leg1',
                                        'theta_leg1', 'iv_leg1', 'ltq_leg1', 'iv2_leg1', 'iv1_leg1',
                                        'ltp_open_leg1', 'ask_leg1', 'low_price_leg1', 'ask_qty_leg1',
                                        'open_price_leg1', 'vega_leg1', 'oi_leg1', 'high_price_leg1',
                                        'ltp_low_leg1', 'bid_leg1', 'ltt_leg1'])

        #df_straddle = df_straddle.ffill()
        
        # Define the time range
        start_time = time(9, 15 )
        end_time = time(15, 30 )

        # Filter the DataFrame
        df_straddle = df_straddle[(df_straddle['time'] >= start_time) & (df_straddle['time'] <= end_time)]

        # Calculate the maximum values from the second row onwards
        df_straddle.loc[1:, 'min_bid'] = df_straddle.loc[1:, 'straddle_bid'].expanding().min()
        df_straddle.loc[1:, 'min_ask'] = df_straddle.loc[1:, 'straddle_ask'].expanding().min()
        df_straddle.loc[1:, 'max_bid'] = df_straddle.loc[1:, 'straddle_bid'].expanding().max()
        df_straddle.loc[1:, 'max_ask'] = df_straddle.loc[1:, 'straddle_ask'].expanding().max()
        ## what??
        df_straddle.loc[1:, 'en1'] = np.where((df_straddle.loc[1:, 'straddle_bid'] + df_straddle.loc[1:, 'straddle_ask'])/2 - (df_straddle.loc[1:, 'min_bid'] + df_straddle.loc[1:, 'min_ask'])/2 > 10 , 10 , 0)

        #Replace 0 values with NaN
        df_straddle.replace(0, np.nan, inplace=True)
        #Forward-fill the NaN values (which were originally 0)
        df_straddle['en1'] = df_straddle['en1'].ffill()
        df_straddle['straddle_bid'] = df_straddle['straddle_bid'].ffill()
        df_straddle['straddle_ask'] = df_straddle['straddle_ask'].ffill()
        
        
        df_straddle_cleaned = pd.concat([df_straddle_cleaned,df_straddle], ignore_index=True)

        ######################################### pre straddle value 

# Function to find previous straddle bid
def find_previous_straddle_bid(row, df):
    for days in range(1, 10):
        previous_date = (pd.to_datetime(row['date'], format='%d-%b-%Y') - timedelta(days=days)).strftime('%d-%b-%Y')
        exp_date = pd.to_datetime(row['exp_date'], format='%d-%b-%Y').strftime('%d-%b-%Y')
        match = df[(df['date'] == previous_date) & (df['time'] == time(15, 28)) & (df['exp_date'] == exp_date)]
        if not match.empty:
            return match['straddle_bid'].values[0]
            break
    return np.nan
def find_previous_straddle_ask(row, df):
    for days in range(1, 10):
        previous_date = (pd.to_datetime(row['date'], format='%d-%b-%Y') - timedelta(days=days)).strftime('%d-%b-%Y')
        exp_date = pd.to_datetime(row['exp_date'], format='%d-%b-%Y').strftime('%d-%b-%Y')
        match = df[(df['date'] == previous_date) & (df['time'] == time(15, 28, 0)) & (df['exp_date'] == exp_date)]
        #print(match)
        if not match.empty:
            return match['straddle_ask'].values[0]  
            break
    return np.nan

# Apply the functions to each row in df_index
df_straddle_cleaned['pre_straddle_bid'] = df_straddle_cleaned.apply(lambda row: find_previous_straddle_bid(row , df_straddle_cleaned), axis=1)
df_straddle_cleaned['pre_straddle_ask'] = df_straddle_cleaned.apply(lambda row: find_previous_straddle_ask(row , df_straddle_cleaned), axis=1)
df_straddle_cleaned['cxt-mn/mx-mn'] = ((df_straddle_cleaned['straddle_bid'] + df_straddle_cleaned['straddle_ask'])/2 - (df_straddle_cleaned['min_bid'] + df_straddle_cleaned['min_ask'])/2) / ((df_straddle_cleaned['max_bid'] + df_straddle_cleaned['max_ask'])/2 - (df_straddle_cleaned['min_bid'] + df_straddle_cleaned['min_ask'])/2)

# Define the path to save the combined CSV files
save_path = "E:\\Backtesting\\cleaned_data"
#save_path = 'D:\\ratio_strategy_rawfile\\backtested_file\\straddle_option'

# Create the save directory if it doesn't exist
if not os.path.exists(save_path):
    os.makedirs(save_path)
#file_name = os.path.join(save_path, f"straddle.csv")
# Define the file names and save paths for each DataFrame
file_name_straddle = os.path.join(save_path, "straddle.csv")
file_name_options = os.path.join(save_path, "options.csv")

df_straddle_cleaned.to_csv(file_name_straddle, index=False)
df_options_cleaned.to_csv(file_name_options, index=False)

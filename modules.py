import calendar
import pandas as pd
import numpy as np
import seaborn as sns
import glob
import os
from datetime import time,datetime,timedelta
from holidays import holidays

def files_path(folder_path):

    location = [glob.glob(os.path.join(folder_path, f.name ))for f in os.scandir(folder_path) if f.is_dir()]

    df_location = pd.DataFrame(location, columns=['location'])

    #location format: "E:\Backtesting\new_data_format\01 JAN 2024"
    df_location['date'] = df_location['location'].apply(lambda x: x.split("\\")[-1]) 

    # Replace underscores with spaces in the 'date' column
    df_location['date'] = df_location['date'].str.replace('_', ' ')

    df_location['date'] = pd.to_datetime(df_location['date'], format='%d %b %Y',dayfirst=True)

    # Sort the DataFrame by the 'date' column
    df_location = df_location.sort_values(by='date',ascending = True, ignore_index=True)

    df_location['date'] = pd.to_datetime(df_location['date'], format='%Y-%m-%d').dt.strftime('%d-%b-%Y')

    return df_location


def date_formater(date_str: str):
    # date_str = "24FEB"
    if date_str.isnumeric():               # Format is 24201 or 241005
        print("The string is numeric.")
        if len(date_str)==5:                # Format is 24201
            year = 2000 + int(date_str[:2])  # Assuming the year starts from 2000
            month = int(date_str[2])
            day = int(date_str[-2:])
            exp_date = datetime(year, month, day) #.strftime('%d-%b-%Y')
        
        if len(date_str)==6:                # Format is 24201
            year = 2000 + int(date_str[:2])  # Assuming the year starts from 2000
            month = int(date_str[2:4])
            day = int(date_str[-2:])
            exp_date = datetime(year, month, day) #.strftime('%d-%b-%Y')

    elif date_str.isalnum():
        print("The string is alphanumeric.")

        # Format is 24FEB
        year = 2000 + int(date_str[:2])
        month_str = date_str[2:].capitalize()
        # print(month_str)
        month = list(calendar.month_abbr).index(month_str.capitalize())
        # print(month)
        # Find the last Thursday of the month
        last_day = calendar.monthrange(year, month)[1]
        # print(calendar.monthrange(year, month))
        exp_date = datetime(year, month, last_day).date()
        # print(exp_date.weekday())
        while exp_date.weekday() != calendar.THURSDAY:
            exp_date -= timedelta(days=1)
        while exp_date in holidays(int(year)):
            exp_date -= timedelta(days=1)  # Move to the previous day if Thursday is a holiday
            exp_date.strftime('%d-%b-%Y')
    else:
        raise ValueError(f"Unsupported date format: {date_str}")
    
    return exp_date.strftime('%d-%b-%Y')


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

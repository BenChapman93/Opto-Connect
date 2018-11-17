from ftplib import FTP
import csv
import os
from datetime import datetime
import sys
import pandas as pd
import pickle
import sys

### ~ ~ ~ Connecting to the Opto HDD and transfering today's RawLogs onto the P Drive

pw = 'foo'
user_name = 'baa'
host = '00.0.00.00'
port = 1025


ftp = FTP()
ftp.connect(host= host, port= port)
ftp.login(user= user_name, passwd= pw)
ftp.cwd('/opto22')

files = ftp.nlst()[3:]

today_date = str(datetime.date(datetime.today())).replace('-','')

P_Drive_Raw = 'P:/8 Staff General/Ben Chapman/Opto/#113/RawLogs/'

def grab_file(file):
    localFile = open(P_Drive_Raw + file, 'wb') # change this to where you want to write the files to
    ftp.retrbinary('RETR ' + file, localFile.write, 1024)
    localFile.close()

for f in files:
    if today_date in f:
        grab_file(f)

ftp.close()

### ~ ~ ~ Moving raw CSVs to a separate dir and removing the necessary rows

modded_csv_dst = 'P:/8 Staff General/Ben Chapman/Opto/#113/1 Modded CSV/'

def row_remover(file): 

    lines = []

    with open(P_Drive_Raw + file, 'r') as ex:
        reader = csv.reader(ex)
        data_lines = list(reader)
        for i in data_lines:
            lines.append(i)

    with open(modded_csv_dst + file, 'w', newline='') as exs: #To prevent the writing of blank rows, newline needs to be equal to ''
        writer = csv.writer(exs)
        for rt in lines[6:]:
            writer.writerow(rt)

for f in os.listdir(P_Drive_Raw):
    if today_date in f:
        row_remover(f)


### ~ ~ ~ Combining each of the 6 CSVs (that make up 1 day's logs) into one DataFrame and applying the filters needed to find individual processes

df_list = []

def DF_creater(csv_file):

    df = pd.read_csv(modded_csv_dst + csv_file, index_col= False, parse_dates= [0], infer_datetime_format= True, dayfirst= True)

    df_list.append(df)

for f in os.listdir(modded_csv_dst):
    if today_date in f:
        DF_creater(f)

merged_df = pd.concat(df_list)

if len(merged_df[merged_df['Fwd pwr'] > 0].index) == 0: #accouns for days where no processes were performed
    sys.exit()
else:
    main_df = merged_df[merged_df['Fwd pwr'] > 0]

main_df = main_df.reset_index(drop= True)

### ~ ~ ~ Locating the being of each new process

for i in range(len(main_df.index)):
    main_df.loc[i, 'Time'] = pd.to_timedelta(main_df.loc[i, 'Time'])

for i in range(len(main_df.index))[1:]:
    main_df.loc[i, 'Diff Time'] = main_df.loc[i, 'Time'] - main_df.loc[i - 1, 'Time']

mask = main_df['Diff Time'] > pd.to_timedelta('00:05:00')

### ~ ~ ~ Finding the index position that marks the end of each process

split_points = []

for i in main_df[mask].index:
    split_points.append(i - 1)

act_split_points = dict(enumerate(split_points))

### ~ ~ ~ code for allocating run numbers based on how many processes were conducted in a day
### ~ ~ ~ This needs to be altered to allow for days where the No. of processes is greater than 3

if len(split_points) == 0:
    for i in range(len(main_df.index)):
        main_df.loc[i, 'Run#'] = 1
        main_df.loc[i, 'Process Time'] = i

elif len(split_points) == 1:
    for i in range(len(main_df.index)):
        for y, x in enumerate(main_df[mask].index):
            if main_df.loc[i,'Time'] <= main_df.loc[x-1, 'Time']:
                main_df.loc[i, 'Run#'] = y+1
                main_df.loc[i, 'Process Time'] = i
                break # this break is required to break the inner for loop
            elif main_df.loc[i, 'Time'] >= main_df.loc[x-1, 'Time']:
                main_df.loc[i , 'Run#'] = y+2
                break
                #for z in range(main_df.last_valid_index()-main_df[mask].index[0]):
                    #main_df.loc[i, 'Process Time'] = z
    for i, z in enumerate(range(main_df[mask].index[0], main_df.last_valid_index() + 1)):
        main_df.loc[z, 'Process Time'] = i

elif len(split_points) == 2:
    for row in range(len(main_df.index)):
        for i in act_split_points:
            try:
                if main_df.loc[row,'Time'] <= main_df.loc[act_split_points[i], 'Time']:
                    main_df.loc[row, 'Run#'] = 1
                    main_df.loc[row, 'Process Time'] = row # should do the first process
                    break
                elif main_df.loc[row,'Time'] >= main_df.loc[act_split_points[i], 'Time'] and main_df.loc[row,'Time'] <= main_df.loc[act_split_points[i+1], 'Time']:
                    main_df.loc[row, 'Run#'] = 2
                    break
                else:
                    main_df.loc[row, 'Run#'] = 3
            except KeyError:
                continue
    for i, z in enumerate(range(act_split_points[0] + 1, act_split_points[1] + 1)):
        main_df.loc[z, 'Process Time'] = i # show do the middle process
    for i, z in enumerate(range(act_split_points[1] + 1, main_df.last_valid_index() + 1)):
        main_df.loc[z, 'Process Time'] = i # should do the 3rd and final process



### The above could be cleaned up, consider using functions

### ~ ~ ~ Reading in the pickle DB and appending current days process logs to it, then saving the updated pickle

db_location = 'P:/8 Staff General/Ben Chapman/Opto/#113/2 DB/DB.pickle'
db_pickle = pd.read_pickle(db_location)

up_db_pck = db_pickle.append(main_df)

pd.to_pickle(up_db_pck, db_location)


### All of the above works as expected
### Now write the code for what to do with the transfered files:
###     Removed the top 5/6 rows DONE
###     Read in all 6 CSVs and create DataFrames from them DONE
###     Merge all 6 DataFrames into one daily DataFrames DONE
###     Filter daily DataFrame for when RF is on (if more than one process for the day, the split DF into number of processes) - creating process DataFrames DONE
###     Add a column for the zero'd/normalised run time DONE
###     Use Pickle to load and upload process DataFrames DONE

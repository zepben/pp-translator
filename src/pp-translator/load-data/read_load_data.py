# This script helps to read the load data from the CSV file
import pandas as pd
import numpy as np


# Red raw data from CSV file
def read_data(filename):
    return pd.read_csv(filename)


# Obtain the meter list from the raw data
def get_list_meter(raw):
    return pd.unique(raw.meternumber)


# Get load data for an specific meter and ext (terminal)
def get_load_data(meternumber, ext, raw):
    date = sorted(pd.unique(raw.timestamp))
    meternumber_all = get_list_meter(raw)

    # Verify the meter number and collect the corresponding data
    if meternumber in meternumber_all:
        index = np.where(raw.meternumber == meternumber)
        n_index = len(index[0])
        result = np.zeros((len(date), 48))
        flag = 0

        # Obtaining the data for the selected ext
        for i in range(n_index):
            ind = index[0][i]

            if ext == raw.ext[ind]:
                result[date.index(raw.timestamp[ind]), :] = raw.iloc[ind, 9:57]
                flag = 1

        # Indicate if the ext was not found in the meter data
        if flag == 0:
            print('Error:', ext, 'not found for', meternumber)

    # Indicates if the meter requested was not found
    else:
        print('Error:', meternumber, 'not found')

    print(meternumber, 'and', ext, 'found')
    return date, result


# Example
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
meter = 'LG031600862'  # Number of the Meter
ext = 'B1'  # Number of ext
file = 'CPM3B3.csv'  # Name of the input file

Data = read_data(file)  # Read data

List_Meter = get_list_meter(Data)

Resp = get_load_data(meter, ext, Data)

print(Resp[0])
print(Resp[1])

# End

import pandas as pd
import numpy as np
from datetime import date


def data_cleaning(data_path):
    #read data
    df = pd.read_csv(data_path)

    #change date type
    df['date'] = pd.to_datetime(df.date)

    #select the date
    date_from = pd.Timestamp(date(2020,10,1))
    df = df[df['date']>= date_from]

    temp = df.isna().sum().sort_values()

    drop_columns = temp.index.values[-24:]
    df = df.drop(columns = drop_columns)

    #clean the columns
    cols = ['Eligible', 'Total Registered', 'Democratic', 'Republican',
           'American Independent', 'Green', 'Libertarian', 'Peace and Freedom',
           'Unknown', 'Other', 'No Party Preference']

    for i in cols:
        df[i] = df[i].str.replace(',', '').fillna(0).astype('int')

    #apply one hot encoding on county
    df = pd.get_dummies(df, columns=['county'])

    y = df[['totalcountconfirmed']]
    x = df[df.columns[5:]]
    d = df[['date']]

    return x,y,d

import pandas as pd
import requests
import bs4
from bs4 import BeautifulSoup
import numpy as np
from selenium import webdriver
import time
from zipfile import ZipFile
import json
import urllib
import io
from datetime import date, timedelta
from io import BytesIO
import os


def get_data():
    '''
    Retrieve all data
    '''

    print('begin to load data')
    print('----------------------------------------------')

    cases = get_cases()
    ppe = get_ppe()
    hospital = get_hospital()
    mobility = get_google_mobility()
    trips = get_trips_data()

    temp = cases.merge(hospital,on=['date','county'],how='left')
    temp = temp.merge(ppe,on=['date','county'],how='left')
    temp = temp.merge(mobility,on=['date','county'],how='left')

    print('combine the data into one dataframe')
    print('----------------------------------------------')

    temp.to_csv('./data/combined.csv', index = False)
    print('save the result in data/combined.csv')
    print('----------------------------------------------')


    return temp

#Helper function for getting trips data
def clean_trips(df_trips):
    df_trips.drop(df_trips[df_trips['date'] < '2020/01/01'].index, inplace = True)
    df_trips.drop(['level', 'state_fips', 'state_code', 'county_fips', 'county'], axis=1, inplace=True)
    df_trips['date'] = pd.to_datetime(df_trips['date'])
    df_trips.reset_index(inplace=True, drop=True)

    return df_trips

def get_trips_data():
    print('Load trips data')
    print('----------------------------------------------')
    # retrieve data
    pd.options.mode.chained_assignment = None
    trips_url = 'https://data.bts.gov/resource/w96p-f2qv.json?State%20Postal%20Code=CA'
    json_trips = pd.read_json(trips_url)
    df_trips = clean_trips(pd.DataFrame(json_trips))
    df_trips['date'] =pd.to_datetime(df_trips['date'])
    return df_trips

def get_apple_mobility():
    print('Load apple mobility data')
    print('----------------------------------------------')
    url = 'https://covid19.apple.com/mobility'
    driver = webdriver.Chrome()
    driver.get(url)
    time.sleep(10)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    div = soup.find( class_ ='download-button-container')
    url = div.a['href']
    driver.close()
    s = requests.get(url).text
    apple =  pd.read_csv(io.StringIO(s))

    return apple

#helper function for get google mobility
def clean_google(df, date):
    df_google_mobility = df[(df['date'] == date) & (df['sub_region_1'] == 'California')]
    df_google_mobility.drop(df_google_mobility.columns[[0,1,2,4,5,6]], axis=1, inplace=True)
    df_google_mobility.dropna(subset=['sub_region_2'], inplace=True)
    df_google_mobility.rename(columns={'sub_region_2':'county'}, inplace=True)
    df_google_mobility.reset_index(drop=True, inplace=True)

    return df_google_mobility

def get_google_mobility():
    google_url = 'https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip'
    d = date.today() - timedelta(days=5)

    print('load google mobility data')
    print('----------------------------------------------')
    content = requests.get(google_url)
    zf = ZipFile(BytesIO(content.content))
    us_mobility = [s for s in zf.namelist() if s == '2020_US_Region_Mobility_Report.csv'][0]
    df = pd.read_csv(zf.open(us_mobility), low_memory=False)
    df_google = clean_google(df, str(d))
    df_google['date'] =pd.to_datetime(df_google['date'])

    return df_google


def get_hospital():
    print('load hospital data')
    print('----------------------------------------------')
    url = 'https://data.ca.gov/dataset/529ac907-6ba1-4cb7-9aae-8966fc96aeef/resource/42d33765-20fd-44b8-a978-b083b7542225/download/hospitals_by_county.csv'
    s = requests.get(url).text
    hospital =  pd.read_csv(io.StringIO(s))
    hospital = hospital.rename(columns={'todays_date':'date'})
    hospital['date'] = pd.to_datetime(hospital.date)

    return hospital

def get_ppe():
    print('load ppe data')
    print('----------------------------------------------')
    url = 'https://data.ca.gov/dataset/da1978f2-068c-472f-be2d-04cdec48c3d9/resource/7d2f11a4-cc0f-4189-8ba4-8bee05493af1/download/logistics_ppe.csv'
    s = requests.get(url).text
    ppe = pd.read_csv(io.StringIO(s))
    ppe = ppe.drop(columns = ['quantity_filled','shipping_zip_postal_code'])
    ppe = pd.get_dummies(ppe, columns=['product_family'])
    ppe = ppe.rename(columns ={'as_of_date':'date'})
    ppe = ppe.groupby(by=['date','county']).sum().reset_index()
    ppe['date'] = pd.to_datetime(ppe.date)

    return ppe

def get_cases():
    print('load covid cases data')
    print('----------------------------------------------')

    url = 'https://data.ca.gov/dataset/590188d5-8545-4c93-a9a0-e230f0db7290/resource/926fd08f-cc91-4828-af38-bd45de97f8c3/download/statewide_cases.csv'
    s = requests.get(url).text
    cases = pd.read_csv(io.StringIO(s))
    cases['date'] = pd.to_datetime(cases.date)

    return cases

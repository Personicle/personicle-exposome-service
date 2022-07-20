import datetime
import logging
import json
from utils.db_connection import *
from utils.base_schema import *
import pandas.io.sql as sqlio
from sqlalchemy import select

import pandas as pd
import numpy as np
import swifter

from producer.send_datastreams_to_azure import datastream_producer
import requests
from itertools import groupby
from utils.config import *
from geopy.geocoders import Nominatim

import traceback
import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow()
    # .replace(
    #     tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    api_key_weather = "8b4545eed9dc9ba39fcf91ff79d26930"

    try:
        query_events = 'select * from  users '
        df_users = sqlio.read_sql_query(query_events, engine)
        temp_info = pd.DataFrame(
            df_users['info'].values.tolist(), index=df_users.index)
        ud = df_users.copy()

        geolocator = Nominatim(user_agent="geoapiExercises")
        temp_info['latitude'] = (temp_info['zipcode']+' '+temp_info['country']
                                 ).swifter.apply(lambda x: geolocator.geocode(x).latitude)
        temp_info['longitude'] = (temp_info['zipcode']+' '+temp_info['country']
                                  ).swifter.apply(lambda x: geolocator.geocode(x).longitude)

        ud_final = pd.concat([ud[['user_id']], temp_info[[
            'country', 'zipcode', 'latitude', 'longitude']]], axis=1)  # .to_csv('test.csv')

        for i in range(ud_final.shape[0]):
            if ud_final['zipcode'].iloc[i] is None:
                continue

            lat = np.float(ud_final['latitude'].iloc[i])
            lon = np.float(ud_final['longitude'].iloc[i])

            api_key_weather = "8b4545eed9dc9ba39fcf91ff79d26930"
            weather_url = "https://api.openweathermap.org/data/2.5/onecall?lat=%s&lon=%s&appid=%s&units=metric" % (
                lat, lon, api_key_weather)

            response = requests.get(weather_url)
            data = json.loads(response.text)

            logging.info("Weather api response")
            logging.info(data)

            df = pd.json_normalize(data)
            df.columns = [col.replace('current.', '') for col in df.columns]

            df_weather = df[['dt', 'temp', 'feels_like',
                            'pressure', 'humidity', 'uvi']].copy()
            df_weather = df_weather.melt(id_vars='dt').copy()

            dct_weather = {'temp': 'C', 'feels_like': 'C',
                           'pressure': 'hPa', 'humidity': '%', 'uvi': 'mwpscc'}

            df_weather['unit'] = df_weather['variable'].map(dct_weather)

            api_key_pol = "8b4545eed9dc9ba39fcf91ff79d26930"

            pollution_url = "http://api.openweathermap.org/data/2.5/air_pollution?lat=%s&lon=%s&appid=%s" % (
                lat, lon, api_key_pol)
            response1 = requests.get(pollution_url)
            data1 = json.loads(response1.text)

            logging.info("Pollution api response")
            logging.info(data1)

            df_pol = pd.json_normalize(data1)

            df_pol = pd.DataFrame.from_dict(df_pol.list.values[0][0:])

            json_struct = json.loads(df_pol.to_json(orient="records"))
            df_pol = pd.io.json.json_normalize(
                json_struct, errors='ignore').copy()

            df_pol.columns = df_pol.columns.str.replace('main.', '')
            df_pol.columns = df_pol.columns.str.replace('components.', '')

            df_pol = df_pol.melt(id_vars='dt').copy()

            df_pol['unit'] = 'ug/cc'
            df_comb = pd.concat([df_pol, df_weather], ignore_index=True)
            df_comb['individual_id'] = ud_final['user_id'].iloc[i]
            df_comb['source'] = 'org.personicle.exposome'
            df_comb.rename(columns={'variable': 'streamName',
                                    'dt': 'timestamp'}, inplace=True)

            df_comb['streamName'] = 'com.personicle.individual.datastreams.exposome.' + \
                df_comb['streamName']

            df_comb = df_comb[['streamName', 'individual_id',
                               'source', 'unit', 'timestamp', 'value']].copy()

            df_comb['timestamp'] = pd.to_datetime(
                df_comb['timestamp'], unit='s')

            df_comb['timestamp'] = df_comb['timestamp'].astype(str)

            logging.info("logging weather data packet")
            logging.info(df_comb)

            l1 = df_comb.to_dict(orient='records')

            data = l1.copy()

            res = []

            def key_func(k): return (k['streamName'],
                                     k['individual_id'], k['unit'], k['source'])

            for k, g in groupby(sorted(data, key=key_func), key=key_func):
                obj = {'streamName': k[0], 'individual_id': k[1],
                       'source': k[3], 'unit': k[2], 'dataPoints': []}
                for group in g:
                    d = {}

                    if ('timestamp' in group):
                        d['timestamp'] = group['timestamp']

                    if ('value' in group):
                        d['value'] = group['value']

                    obj['dataPoints'].append(d)
                res.append(obj)

            try:
                for i in range(len(res)):
                    datastream_producer(res[i])

            except Exception as e:

                logging.info("Total data points added for source {}: {}".format(
                    datastream_producer(res[i]), len(datastream_producer(res[i]))))
                logging.error(traceback.format_exc())
        logging.info({"msg": "successful request at {}".format(utc_timestamp)})
    except Exception as e:
        logging.error({"msg": "unsuccessful request at {}".format(utc_timestamp),
                       "error": e})
        logging.error(traceback.format_exc())

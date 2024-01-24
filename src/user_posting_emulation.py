import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from decouple import Config, RepositoryIni
import os


random.seed(100)

class AWSDBConnector:

    def __init__(self):
    
        self.path =  os.path.realpath(__file__)
        self.file_dir = os.path.dirname(self.path)
        self.dir_creds = self.file_dir.replace('src','creds')
        self.settings_file = os.path.join(self.dir_creds, 'settings.ini')

        config =  Config(RepositoryIni(self.settings_file))

        self.host = config('host')
        self.user = config('user')
        self.password = config('password')
        self.database = config('database')
        self.port = config('port')
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print('pinterest_data contains data about posts being updated to Pinterest \n', pin_result, '\n')
            print('geolocation_data contains data about the geolocation of each Pinterest post found in pinterest_data \n',geo_result, '\n')
            print('user_data contains data about the user that has uploaded each post found in pinterest_data \n',user_result, '\n')


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')


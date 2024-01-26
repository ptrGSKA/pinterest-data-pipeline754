import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.pool import NullPool
from decouple import Config, RepositoryIni
import os
import datetime



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
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?charset=utf8mb4", poolclass=NullPool)
        return engine


class DataProducer:

    def __init__(self):
        
        self.new_connector = AWSDBConnector()

        self.path =  os.path.realpath(__file__)
        self.file_dir = os.path.dirname(self.path)
        self.dir_creds = self.file_dir.replace('src','creds')
        self.settings_file = os.path.join(self.dir_creds, 'settings.ini')

        config =  Config(RepositoryIni(self.settings_file))
        
        self.aws_user_id = config('aws_user_id')
        self.invoke_url  = config('aws-api-url')

    def serialize_datetime(self,obj): 
        if isinstance(obj, datetime.datetime): 
            return obj.isoformat() 
        raise TypeError("Object is not serializable.")
    
    def data_emulator(self, data, topic):

        payload = json.dumps({ "records": [{ "value": data }] }, default = self.serialize_datetime)

        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        invoke_url = f'{self.invoke_url}/topics/{self.aws_user_id}{topic}'
        response = requests.request("POST", invoke_url, headers=headers, data=payload)

        try:
            response.raise_for_status()
            print(f'Successful data transmission to topic {topic}')
        except requests.exceptions.HTTPError as err:
            print(f'The following error has occured: {err}')

    def run_infinite_post_data_loop(self):
        while True:
            sleep(random.randrange(0, 2))
            random_row = random.randint(0, 11000)
            engine = self.new_connector.create_db_connector()

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


                # Dictionary with topics and DB date that is being looped through and the dataemulator method called
                # that prepares the payload and send a POST request to the API.
                data_topics = {'.pin': pin_result, '.geo': geo_result, '.user': user_result}
                for topic, data in data_topics.items():
                    self.data_emulator(data, topic)
            

if __name__ == "__main__":
    producer = DataProducer()
    producer.run_infinite_post_data_loop()

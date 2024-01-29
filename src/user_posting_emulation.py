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
    '''
    Database connector class responsible for all database related comminucation.

    Methods:
        __init__: class constructor
        create_db_connector: responsible for sqlalchemy database engine creation
    '''

    def __init__(self):
        '''
        Class constructor responsible for the initialization of the default objects.

        Args:
            path: real path to the source file where it was called
            dir: the directory of the file
            dir_creds: full path pointing to the directory of the credentials
            settings_file: credentials and settings for the project
            config: coordinates all the configuration retrieval
            host: database host
            user: database user
            password: user password for the database connection
            database: the database to connect to
            port: database port
        '''

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
        '''
        This function is reponsible to create a database engine that is responsible for all the
        database related communication.

        Returns:
            engine: sqlalchemy database engine.
        '''
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?charset=utf8mb4", poolclass=NullPool)
        return engine


class DataProducer:
    '''
    Data emulation class that contains the retrieval and posting of pinterest data.

    Methods:
        __init__: class contructor
        serialize_datatime: convert datatime object into standard iso format if there is any in the data
        data_emulator: reponsible for the communication with the API.
        run_infinite_post_data_loop: runs continuously, retrieves data from database and calls data_emulator to post the data
                                    to the API.
    '''
    def __init__(self):
        '''
        Class constructor responsible for the initialization of the default objects.

        Args:
            new_connector: AWSConnector class instance
            config: coordinates all the configuration retrieval
            aws_user_id: aws user id to build the unique Kafka topic namnes
            aws_invoke_url: the API address where the request is being sent
        '''
        
        self.new_connector = AWSDBConnector()

        config =  Config(RepositoryIni(self.new_connector.settings_file))
        
        self.aws_user_id = config('aws_user_id')
        self.invoke_url  = config('aws-api-url')

    def serialize_datetime(self,obj):
        '''
        This function receives and object and if that object is datatime type it converts into standard iso format
        for the data_emulator method for errorless data serialization.

        Args:
            obj(dict): data to parse through

        Returns:
            JSON encodable version of the object.
        '''

        if isinstance(obj, datetime.datetime): 
            return obj.isoformat() 
        raise TypeError("Object is not serializable.")
    
    def data_emulator(self, data, topic):
        '''
        This function is responsible for the preparation of the payload by serializing the json file,
        also converts any datetime object into iso format by calling the serialoze_datetime method.
        It also builds the API's invoke url and sends the POST request to the API.

        Args:
            data (dict): dictionary object contains the pin, geo or user data.
            topic (string): topic name that belongs to the data and the invoke url is build from.

        Returns:
            None, informs the user about a succesfull post request if the status code is 200,
            otherwise exits with a HTTPError and status code.
        '''

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
        '''
        This function is responsible for the random data collection from the database for each topic and
        the mapping of the result into a dictionary object. It calls the data_emulator method for each 
        topic subsequently.

        Returns:
            None.
        '''

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

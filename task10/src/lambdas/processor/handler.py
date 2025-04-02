from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda
import json
import uuid
import boto3
import requests
import os
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

patch_all()

_LOG = get_logger(__name__)
TABLE_NAME = os.environ.get('TARGET_TABLE', 'Weather')

class Processor(AbstractLambda):

    def __init__(self):
        super().__init__()
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(TABLE_NAME)

    def validate_request(self, event) -> dict:
        # For this task, we'll assume all requests are valid
        return {'is_valid': True}

    @xray_recorder.capture('fetch_weather_data')
    def fetch_weather_data(self):
        url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @xray_recorder.capture('store_weather_data')
    def store_weather_data(self, data):
        item = {
            'id': str(uuid.uuid4()),
            'forecast': {
                'elevation': data['elevation'],
                'generationtime_ms': data['generationtime_ms'],
                'hourly': {
                    'temperature_2m': data['hourly']['temperature_2m'],
                    'time': data['hourly']['time']
                },
                'hourly_units': {
                    'temperature_2m': data['hourly_units']['temperature_2m'],
                    'time': data['hourly_units']['time']
                },
                'latitude': data['latitude'],
                'longitude': data['longitude'],
                'timezone': data['timezone'],
                'timezone_abbreviation': data['timezone_abbreviation'],
                'utc_offset_seconds': data['utc_offset_seconds']
            }
        }
        self.table.put_item(Item=item)

    @xray_recorder.capture('handle_request')
    def handle_request(self, event, context):
        """
        Fetch weather data from Open-Meteo API and store it in DynamoDB
        """
        try:
            weather_data = self.fetch_weather_data()
            self.store_weather_data(weather_data)
            _LOG.info("Weather data stored successfully")
            return 200, {'message': 'Weather data stored successfully'}
        except Exception as e:
            _LOG.error(f"Error occurred: {str(e)}")
            return 500, {'error': str(e)}

HANDLER = Processor()

def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
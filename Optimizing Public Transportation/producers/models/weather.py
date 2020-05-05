"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from models.producer import Producer
from models.ctaconstants import CTAConstants
from json import encoder



logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    encoder.FLOAT_REPR = lambda o: format(o, '.2f')
    
    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))
    
    # instance variables
    # self.status
    # self.temp
    
    def __init__(self, month):
        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)        
        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)
                        
        super().__init__(
            topic_name=CTAConstants.WEATHER_TOPIC_NAME, 
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=1,
            num_replicas=1,            
        )       
        
        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0


    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        logger.debug("weather kafka proxy integration")

        headers = {"Content-Type": "application/vnd.kafka.avro.v2+json"}

        data = {
            "key_schema": json.dumps(Weather.key_schema),            
            "value_schema": json.dumps(Weather.value_schema),
            "records": [
                {"value": {"temperature": float(self.temp),
                                  "status": self.status.name},
                 "key": {"timestamp": self.time_millis()} }]
        }

        print (f"{CTAConstants.REST_PROXY_HOST}topics/{CTAConstants.WEATHER_TOPIC_NAME}")
        resp = requests.post(
            f"{CTAConstants.REST_PROXY_HOST}topics/{CTAConstants.WEATHER_TOPIC_NAME}",              
            data=json.dumps(data),
            headers=headers,
        )

        try:
            resp.raise_for_status()
        except:
            logger.error(f"Failed to send data to REST Proxy {json.dumps(resp.json(), indent=2)}")

        logger.debug(f"Sent data to REST Proxy {json.dumps(resp.json(), indent=2)}")
    
        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
        
if __name__ == "__main__":
    Weather(4).run(4)

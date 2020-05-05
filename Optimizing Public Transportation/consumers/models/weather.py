"""Contains functionality related to Weather"""
import logging

from models.ctaconstants import CTAConstants

logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("weather process_message.")
        #
        # Process incoming weather messages. Set the temperature and status.
        # Null and error messages are checked in consumer/KafkaConsumer
        if ( WEATHER_TOPIC_NAME in message.topic()):
            self.temperature = message.value["temperature"]
            self.status = message.value["status"]
        else:
            logger.warning(f"Weather model got message unexpected topic {message.topic()}, skipping.")
        

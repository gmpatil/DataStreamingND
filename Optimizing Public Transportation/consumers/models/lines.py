"""Contains functionality related to Lines"""
import json
import logging

from models import Line


logger = logging.getLogger(__name__)


class Lines:
    """Contains all train lines"""

    def __init__(self):
        """Creates the Lines object"""
        self.red_line = Line("red")
        self.green_line = Line("green")
        self.blue_line = Line("blue")

    def process_message(self, message):
        """Processes a station message"""
        logger.debug(f"Processing station msg from topic: {message.topic()}")
        
        if ("org.chicago.cta.station" in message.topic()):
            logger.debug(f"In lines process_message/station from topic: {message.topic()}")            
            
            value = message.value()
            if "org.chicago.cta.stations.table.v1" in message.topic():
                logger.debug("In lines process_message/stations table")            
                value = json.loads(value)
                
            if value["line"] == "green":
                self.green_line.process_message(message)
            elif value["line"] == "red":
                self.red_line.process_message(message)
            elif value["line"] == "blue":
                self.blue_line.process_message(message)
            else:
                logger.info("discarding unknown line msg %s", value["line"])
        elif "TURNSTILE_SUMMARY" in message.topic():
            self.green_line.process_message(message)
            self.red_line.process_message(message)
            self.blue_line.process_message(message)
        else:
            logger.debug("ignoring non-lines message %s", message.topic())

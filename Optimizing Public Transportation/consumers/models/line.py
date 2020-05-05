"""Contains functionality related to Lines"""
import json
import logging

from models import Station

from models.ctaconstants import CTAConstants


logger = logging.getLogger(__name__)

# Represents model for metro line, like "red", "blue", and "green"
class Line:
    """Defines the Line Model"""

    def __init__(self, color):
        """Creates a line"""
        self.color = color
        self.color_code = "0xFFFFFF"
        if self.color == "blue":
            self.color_code = "#1E90FF"
        elif self.color == "red":
            self.color_code = "#DC143C"
        elif self.color == "green":
            self.color_code = "#32CD32"
        self.stations = {}

    def _handle_station(self, value):
        """Adds the station to this Line's data model"""
        logger.info(f"In Line({self.color}), creating station:{json.dumps(value, indent=2)}")
                     
        if (value["line"] != self.color):
            logger.warning(f"Oops...station msg to wrong line. color: {self.color}")                     
            return
                         
        self.stations[value["station_id"]] = Station.from_message(value)

    def _handle_arrival(self, message):
        """Updates train locations"""
        logger.info(f"In Line ({self.color}), processing train arrival.")        
        
        value = message.value()
        prev_station_id = value.get("prev_station_id")
        prev_dir = value.get("prev_direction")
        if prev_dir is not None and prev_station_id is not None:
            prev_station = self.stations.get(prev_station_id)
            if prev_station is not None:
                prev_station.handle_departure(prev_dir)
            else:
                logger.info("unable to handle previous station due to missing station")
        else:
            logger.info(
                "unable to handle previous station due to missing previous info"
            )

        station_id = value.get("station_id")
        station = self.stations.get(station_id)
        if station is None:
            logger.info(f"arrival:unable to handle message due to missing station {station_id}")
            return
        station.handle_arrival(
            value.get("direction"), value.get("train_id"), value.get("train_status")
        )

    def process_message(self, message):
        """Given a kafka message, extract data"""      

        msgTopic = message.topic()
        logger.debug(f"In line ({self.color}) process_message. Topic {msgTopic}")

        if (msgTopic is None):
            logger.warn(f"msgTopic None in process_message ({self.color}) process_message.")
        else:
            # stations Faust Table, non-Avro msg so, deserialize.       
            if (CTAConstants.STATION_TRANSFORMED_TOPIC in msgTopic): 
                try:
                    value = json.loads(message.value())
                    self._handle_station(value)
                except Exception as e:
                    logger.fatal("bad station? %s, %s", value, e)
            # Arrival topic                
            elif ((CTAConstants.TRAIN_ARRIVAL_TOPIC_PREFIX in msgTopic) or ("com.cta.station.arrivals" in msgTopic)): 
                logger.info(f"In line ({self.color}) process_message. Topic {msgTopic}")
                self._handle_arrival(message)
            # KSQL Turnstile Summary Topic, non-Avro msg so, deserialize.
            elif (CTAConstants.STATION_TURNSTILE_SUMMARY in msgTopic):
                json_data = json.loads(message.value())
                station_id = json_data.get("STATION_ID")
                station = self.stations.get(station_id)
                if station is None:
                    logger.debug(f"turnstile:unable to handle message due to missing station{station_id}:{self.color}")
                    return
                station.process_message(json_data)
            else:
                logger.info(
                    "unable to find handler for message from topic %s", message.topic
                )

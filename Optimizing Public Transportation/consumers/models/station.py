"""Contains functionality related to Stations"""
import json
import logging


logger = logging.getLogger(__name__)


class Station:
    """Defines the Station Model"""

    def __init__(self, station_id, station_name, order):
        """Creates a Station Model"""
        self.station_id = station_id
        self.station_name = station_name
        self.order = order
        self.dir_a = None
        self.dir_b = None
        self.num_turnstile_entries = 0

    @classmethod
    def from_message(cls, value):
        """Given a Kafka Station message, creates and returns a station"""
        return Station(value["station_id"], value["station_name"], value["order"])

    def handle_departure(self, direction):
        """Removes a train from the station"""
        logger.info(f"In station handle de[arture. stn id:{self.station_id}, d:{direction}")        
        
        if direction == "a":
            self.dir_a = None
        else:
            self.dir_b = None

    def handle_arrival(self, direction, train_id, train_status):
        """Unpacks arrival data"""
        logger.info(f"In station handle arrival. stn id:{self.station_id}, d:{direction}, tin:{train_id} t st:{train_status}")
        
        status_dict = {"train_id": train_id, "status": train_status.replace("_", " ")}
        if direction == "a":
            self.dir_a = status_dict
        else:
            self.dir_b = status_dict
        
        logger.info("Done station handle arrival.")

    def process_message(self, json_data):
        """Handles arrival and turnstile messages"""
        logger.debug(f"In Station({self.station_name}), updating turnstile entries:{json_data['COUNT']}")        
        
        self.num_turnstile_entries = self.num_turnstile_entries + json_data["COUNT"]

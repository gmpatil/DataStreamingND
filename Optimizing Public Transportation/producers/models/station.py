"""Methods pertaining to loading and configuring CTA "L" station data."""
import logging
from pathlib import Path

from confluent_kafka import avro

from models import Turnstile
from models.producer import Producer
from models.ctaconstants import CTAConstants

logger = logging.getLogger(__name__)

#
# Actually represents station on a "line", as same physical station may locate more than one station-line.
# Ex: Station Addison, has 3 lines Red, Blue and Yellow?(Kimball-Loop).
# So, there will be 3 instances of Station, one for each line.
#
class Station(Producer):
    """Defines a single station"""
    
    # class variables
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_value.json")

    # instamce variables
#     self.topic_name    
#     self.name
#     self.station_id
#     self.color
#     self.dir_a
#     self.dir_b
#     self.a_train
#     self.b_train
#     self.turnstile
    
    def __init__(self, station_id, name, color, direction_a=None, direction_b=None):
        self.name = name

        # No longer used.
        # station_name = (
        #     self.name.lower()
        #     .replace("/", "_and_")
        #     .replace(" ", "_")
        #     .replace("-", "_")
        #     .replace("'", "")
        # )

#         Unique stations  91.
#         yellow (not r, b, and g) = 42
#         red                      = 66
#         blue                     = 66
#         green                    = 56
#         ---------------------------------
#         # of Station-lines        230
#
#         Since our reporting is train-line based, and key is timestamp, we will have to aggregate on metro line. 
#         Topic per station vs station-line: Topic per station as per station-line is will have a mesaages per few minutes instead of seconds.
#         Assuming busy station with three lines crossing, with topic per station, we can expect appx 4-6 msgs/min. 
#         
        # Ex: com.cta.stations.addison.red, com.cta.stations.addison.blue, com.cta.stations.ohare.blue.
        #topic_name = f"{CTAConstants.TRAIN_ARRIVAL_TOPIC_PREFIX}.{color}"             
        #topic_name = f"{CTAConstants.TRAIN_ARRIVAL_TOPIC_PREFIX}{station_name}.{color}" 
        topic_name = f"{CTAConstants.TRAIN_ARRIVAL_TOPIC_PREFIX}"         
      
        super().__init__(
            topic_name,
            key_schema=Station.key_schema,
            value_schema=Station.value_schema, 
            num_partitions=2, 
            num_replicas=1,            
#             num_replicas=2,
        )

        self.topic_name = topic_name
        self.station_id = int(station_id)
        self.dir_a = direction_a
        self.dir_b = direction_b
        self.a_train = None
        self.b_train = None        
        colors = CTAConstants.colors
        if (colors.red == color):
            self.color = "red"
        elif (colors.blue == color):
            self.color = "blue"
        elif (colors.green == color):
            self.color = "green"
        else:
            self.color = "yellow" #Unknown.
            
        self.turnstile = Turnstile(self)
        

    def run(self, train, direction, prev_station_id, prev_direction):
        """Simulates train arrivals at this station"""
        logger.debug(f"arrival kafka integration. sin:  {self.station_id}, tin: {train.train_id},  dir: {direction}, col: {self.color}, p.sin: {prev_station_id}, pdir:{prev_direction}")

        # if (prev_station_id is None):
        #     val = {
        #         "station_id": self.station_id,
        #         "train_id": train.train_id,
        #         "direction": direction,
        #         "line": self.color,
        #         "train_status":train.status.name},
        # else:
        val = {
            "station_id": self.station_id,
            "train_id": train.train_id,
            "direction": direction,
            "line": self.color,
            "train_status":train.status.name,
            "prev_station_id": prev_station_id,
            "prev_direction": prev_direction
            }
        
        self.produce(topic=self.topic_name, 
           key={"timestamp": self.time_millis()},
           value=val
        )

    def __str__(self):
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing to {:<30} | Direction B: | {:^5} | departing to {:<30} | ".format(
            self.station_id,
            self.name,
            self.a_train.train_id if self.a_train is not None else "---",
            self.dir_a.name if self.dir_a is not None else "---",
            self.b_train.train_id if self.b_train is not None else "---",
            self.dir_b.name if self.dir_b is not None else "---",
        )

    def __repr__(self):
        return str(self)

    def arrive_a(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'a' direction"""
        self.a_train = train
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'b' direction"""
        self.b_train = train
        self.run(train, "b", prev_station_id, prev_direction)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.turnstile.close()
        super(Station, self).close()

"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware
from models.ctaconstants import CTAConstants


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    # Class variables
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load( f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    # Instance variables
#     self.topic_name    
#     self.station = station
#     self.turnstile_hardware = TurnstileHardware(station)

        
    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        # Ex: com.cta.turnstile.addison.red, com.cta.turnstile.addison.blue, com.cta.turnstile.ohare.blue.
        #topic_name = f"{CTAConstants.STATION_TURNSTILE_TOPIC_PREFIX}"                 
        #topic_name = f"{CTAConstants.STATION_TURNSTILE_TOPIC_PREFIX}.{station.color}"                 
        #topic_name = f"{CTAConstants.STATION_TURNSTILE_TOPIC_PREFIX}.{station_name}"         
        topic_name = f"{CTAConstants.STATION_TURNSTILE_TOPIC_PREFIX}"                 
        
        super().__init__(
            topic_name,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema, 
            num_partitions=5,
            #num_replicas=2,
            num_replicas=1,            
        )
        
        self.topic_name = topic_name
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.debug("turnstile kafka integration")

        for i in range(num_entries):
            Producer.produce(self,
               topic=self.topic_name,
               key={"timestamp": self.time_millis()},
               value={
                    "station_id": self.station.station_id,
                    "station_name": self.station.name,
                    "line": self.station.color,
               },
            )
        

if __name__ == "__main__":
    Turnstile().run()

"""Defines trends calculations for stations"""
from dataclasses import dataclass, asdict
import json
import logging

import faust

from models.ctaconstants import CTAConstants

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker=f"kafka://{CTAConstants.BOOTSTRAP_SERVERS}", store="memory://")
# Input Kafka Topic. 
in_topic = app.topic(f"{CTAConstants.STATION_DATA_TOPIC_PREFIX}stations", value_type=Station)

# # Output Kafka Topic
out_topic = app.topic(CTAConstants.STATION_TRANSFORMED_TOPIC, partitions=1, value_type=TransformedStation)

# # Define a Faust Table
table = app.Table(
   CTAConstants.FAUST_STATION_TRANSFORMED_TABLE_NAME,
   default=TransformedStation, #TODO
   partitions=1,
   changelog_topic=out_topic,
   value_type=TransformedStation
)

#
#
# Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(in_topic)
async def transform_stations(in_stations):
    async for sn in in_stations:
        
        t = TransformedStation(sn.station_id, sn.station_name, sn.order, "na")
        if (sn.red):
            t.line = "red"
        elif (sn.blue):
            t.line = "blue"
        elif (sn.green):
            t.line = "green"
        else:
            #t.line = "yellow"
            continue
            
        table[sn.station_id] = t
        
        #logger.debug(f" Faust processor: {json.dumps(asdict(t))}")

if __name__ == "__main__":
    app.main()

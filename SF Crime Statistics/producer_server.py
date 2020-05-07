from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime

import asyncio

import json
import time



# Sample record from the file.
#     {
#         "crime_id": "183653763",
#         "original_crime_type_name": "Traffic Stop",
#         "report_date": "2018-12-31T00:00:00.000",
#         "call_date": "2018-12-31T00:00:00.000",
#         "offense_date": "2018-12-31T00:00:00.000",
#         "call_time": "23:57",
#         "call_date_time": "2018-12-31T23:57:00.000",
#         "disposition": "ADM",
#         "address": "Geary Bl/divisadero St",
#         "city": "San Francisco",
#         "state": "CA",
#         "agency_id": "1",
#         "address_type": "Intersection",
#         "common_location": ""
#     },


class ProducerServer():
    
    def __init__(self, input_file, topic, bootstrap_servers, client_id):
        self.input_file = input_file
        self.topic_name = topic
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id

    """ Checks if the given topic exists """      
    """  If auto.create.topics.enable is set to true on the broker and an unknown topic is specified it will be created. """
    def _topic_exists(self, client, topic_name):
        topicsMeta = client.list_topics(topic=topic_name);
        
        if topic_name in topicsMeta.topics:
            return True
        else: 
            return False
    
    """ Creates the topic with the given topic name """    
    def _create_topic(self, client, topic_name):
    # Create the topic. Make sure to set the topic name, the number of partitions, the
    # replication factor. Additionally, set the config to have a cleanup policy of delete, a
    # compression type of lz4, delete retention milliseconds of 2 seconds, and a file delete delay
    # milliseconds of 2 second.
        num_part = 3;
        repl_fact = 1;
        futures = client.create_topics(
            [
                NewTopic(topic_name, num_part, repl_fact)
            ],
            {
                "cleanup.policy": "delete",
                "compression.type": "lz4",
                "delete.retention.ms": "2000",
                "file.delete.delay.ms": "2000"
            }
        )

        for topic, future in futures.items():
            try:
                print(f"Topic {topic_name} {' created.' if future.result() else 'creating...'}. Result: {future.result()}")
            except Exception as e:
                print(f"Failed to create topic {topic_name}: {e}")
                raise
        
    """ Generate data """   
    def generate_data(self):
        client = AdminClient({"bootstrap.servers": self.bootstrap_servers})

        exists = self._topic_exists(client, self.topic_name)
        if (exists):
            print(f"Topic {self.topic_name} exists.")
        else:
            print(f"Topic {self.topic_name} does not exists, creating.")
            self._create_topic(client, self.topic_name)

        _POLL_PER_RECORDS = 1000            
        
        try:
            asyncio.run(self._produce_task(self.topic_name, self.bootstrap_servers, self.input_file, _POLL_PER_RECORDS))
            
        except KeyboardInterrupt as e:
            print("shutting down.")        

    """Runs the Producer and Consumer tasks"""
    async def _produce_task(self, topic_name, bootstrap_servers, input_file, poll_per_recs):

        task1 = asyncio.create_task(self._produce(topic_name, bootstrap_servers, input_file, poll_per_recs))
        await task1
    
    """Runs the Producer """
    async def _produce(self, topic_name, bootstrap_servers, input_file, poll_per_recs):
        p = Producer({"bootstrap.servers": bootstrap_servers})

        with open(input_file) as json_file:
            data = json.load(json_file)
        
        start = datetime.now()
        print(f"Producing {len(data)} records to the topic {topic_name} at {start}")
        
        for i in range(len(data)):
            p.produce(topic_name, json.dumps(data[i]).encode('utf-8'))
            if (i % poll_per_recs == 0):
                p.poll(1)
#             print(json.dumps(data[i]))

        p.flush(5)
    
        end = datetime.now()
        print(f"Produced {len(data)} records to the topic {topic_name} at {end}. Took {end - start}")    
        
        await asyncio.sleep(0.5)
        
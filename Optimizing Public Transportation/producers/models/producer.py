"""Producer base-class providing common utilites and functionality"""
import logging
import time
import json

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

from models.ctaconstants import CTAConstants

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # ***Class variablaes***
    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    # ***Instance variablaes***    
#     self.topic_name 
#     self.key_schema 
#     self.value_schema 
#     self.num_partitions
#     self.num_replicas
#     self.avroProducer
    
    
    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            CTAConstants.MAP_KEY_BOOTSTRAP_SERVERS:CTAConstants.BOOTSTRAP_SERVERS
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        avro_properties = {
            CTAConstants.MAP_KEY_BOOTSTRAP_SERVERS:CTAConstants.BOOTSTRAP_SERVERS,
            CTAConstants.MAP_KEY_SCHEMA_REGISTRY_URL:CTAConstants.SCHEMA_REGISTRY_HOST
        }         
        
        # Configure the AvroProducer
        self.avroProducer = AvroProducer(avro_properties, default_key_schema=self.key_schema, default_value_schema=self.key_schema)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        logger.info("In topic creation kafka integration.")

        client = AdminClient(self.broker_properties)
        
        """Checks if the given topic exists"""
        topic_metadata = client.list_topics(timeout=5)
        topic_present = self.topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))
        
        if (not topic_present):
            futures = client.create_topics(
                [NewTopic(topic=self.topic_name, num_partitions=self.num_partitions,
                          replication_factor=self.num_replicas)]
            )
            for _, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Created topic {self.topic_name}.")
                except Exception as e:
                    print( f"Failed to create topic: {self.topic_name}. Exception: {e}" )                
                    pass
        else:
            logger.info(f"Topic {self.topic_name} present doing nothing.")
        

#     def time_millis(self):
#         return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        # Write cleanup code for the Producer here
        #
        logger.info("producer cleanup.")
        
        Producer.existing_topics.discard(self.topic_name)        
        
        if self.avroProducer.len() > 0:
            self.avroProducer.flush(5)

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

    
    def produce(self, topic, key, value):
        # Avro producer will serialize the JSON key and value objects for us, no need to serialize to string.
        self.avroProducer.produce(topic=topic, key=key, value=value, key_schema=self.key_schema, value_schema=self.value_schema)        
        #self.avroProducer.produce(topic=topic, key=key, value=json.dumps(value), key_schema=self.key_schema, value_schema=self.value_schema)

        
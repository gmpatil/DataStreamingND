"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen

from models.ctaconstants import CTAConstants

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    consumer_group_counter = 0
    
    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        
        KafkaConsumer.consumer_group_counter = KafkaConsumer.consumer_group_counter + 1
        
        if (self.offset_earliest):
            self.broker_properties = {
                CTAConstants.MAP_KEY_BOOTSTRAP_SERVERS:CTAConstants.BOOTSTRAP_SERVERS,
                "group.id":f"{CTAConstants.CONSUMER_GRP_ID_PRFX}-{KafkaConsumer.consumer_group_counter}",
                "default.topic.config": {"auto.offset.reset":"earliest"}
            }
        else:
            self.broker_properties = {
                CTAConstants.MAP_KEY_BOOTSTRAP_SERVERS:CTAConstants.BOOTSTRAP_SERVERS,
                "group.id":f"{CTAConstants.CONSUMER_GRP_ID_PRFX}-{KafkaConsumer.consumer_group_counter}",
                "default.topic.config": {"auto.offset.reset":"earliest"}
            }
        
        # Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = CTAConstants.SCHEMA_REGISTRY_HOST
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        #
        # Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        self.consumer.subscribe([topic_name_pattern], on_assign=self.on_assign)
        
        logger.info(f"Instantiated consumer and subscribed: ({self.topic_name_pattern})")

    
    # Called back on assign of partition(s) to this Consumer.        
    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        logger.info("on_assign")
        if (self.offset_earliest):
            for partition in partitions:
                #partition.offset(Offset.OFFSET_BEGINNING)
                partition.offset = confluent_kafka.OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        # Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        logger.debug(f"In _consume({self.topic_name_pattern})")
        
        try:
            msg = self.consumer.poll(timeout=self.consume_timeout)
            if (msg is None):
                logger.debug("No msg in topic yet.")
                return 0;
            else:
                if (msg.error() is None):
                    logger.debug("Got msg.")
                    self.message_handler(msg)
                else:
                    # handle error.
                    error = msg.error()
                    logger.error(f"Error in consumer:{self.topic_name_pattern} while consuming msgs. Err code: {error.code()}, error-name: {error.name()}, error.str:{error.str()}" )
                    
                return 1
        except RuntimeError as re:
            logger.error(f"Runtime error in consumer:{self.topic_name_pattern}. Err msg: {re.message}" )

    def close(self):
        """Cleans up any open kafka consumers"""
        # Cleanup the kafka consumer
        self.consumer.unassign()
        self.consumer.unsubscribe()
        
                         
                         
                         
        
                         
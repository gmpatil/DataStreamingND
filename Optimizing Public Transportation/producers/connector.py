"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests

from models.ctaconstants import CTAConstants


logger = logging.getLogger(__name__)

KAFKA_CONNECT_URL =  f"{CTAConstants.CONNECT_HOST}connectors"
CONNECTOR_NAME = CTAConstants.STATION_DATA_CONNECTOR_NAME

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)

    logger.info("In connector code")
    rest_method = requests.post
    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,  
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "batch.max.rows": "500",
                    "topic.prefix": CTAConstants.STATION_DATA_TOPIC_PREFIX,
                    "mode": "incrementing", 
                    "incrementing.column.name": "stop_id",  
                    "table.whitelist": "stations",  
                    "connection.url": f"jdbc:postgresql://{CTAConstants.POSTGRES_DB_HOST}/{CTAConstants.POSTGRES_DB_NAME}",
                    "connection.user": CTAConstants.POSTGRES_DB_USER,
                    "connection.password":CTAConstants.POSTGRES_DB_P,
                    "poll.interval.ms": "3600000", # Poll every hour
                },
            }
        ),
    )

    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
        logger.error(f"failed creating connector: code: {resp.stat_code}, content: {json.dumps(resp.json())}")
        exit(1)
        
    logger.info(f"connector {CONNECTOR_NAME} created successfully.")    

if __name__ == "__main__":
    configure_connector()

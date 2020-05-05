"""A class providing common constants shared across project"""
"""For use in both Workspace environment and Docker environment by commenting and uncommenting below."""
from enum import IntEnum


class CTAConstants:

    # -------------------------------------------------------------
    # Docker environment constants....
#     ZOOKEEPER_HOST = "zookeeper:2181"
#     BOOTSTRAP_SERVERS = "kafka0:9092"  #string of csv
#     SCHEMA_REGISTRY_HOST = "http://schema-registry:8081/"
#     REST_PROXY_HOST = "http://rest-proxy:8082/"
#     CONNECT_HOST = "http://connect:8083/"
#     KSQL_HOST = "http://ksql:8088/"
#     POSTGRES_DB_HOST = "postgres:5432"
    # -------------------------------------------------------------    
    # Workspace environment constants....
    ZOOKEEPER_HOST = "localhost:2181"
    BOOTSTRAP_SERVERS = "localhost:9092"  #string of csv
    SCHEMA_REGISTRY_HOST = "http://localhost:8081"
    REST_PROXY_HOST = "http://localhost:8082/"
    CONNECT_HOST = "http://localhost:8083/"
    KSQL_HOST = "http://localhost:8088/"
    POSTGRES_DB_HOST = "localhost:5432"
    # -------------------------------------------------------------        
    # Common for both environments 

    # Dependent on consumer classes (server.py, faust_stream, etc)
    # If constraint is not to update consumers/server.py and consumer/models/lines.py
    # then Arrival topic name must have "org.chicago.cta.station.arrivals."
    # and transformed Stations table topic must have "org.chicago.cta.stations.table.v1"
    # in their names.
    TRAIN_ARRIVAL_TOPIC_PREFIX = "org.chicago.cta.station.arrivals.t001"
    STATION_TURNSTILE_TOPIC_PREFIX = "com.cta.stations.turnstile.entry"
    STATION_TURNSTILE_SUMMARY = "TURNSTILE_SUMMARY"
    # Hard coded in consumer.Lines "org.chicago.cta.station"
    #STATION_TRANSFORMED_TOPIC = "com.cta.stations.tablet001" 
    STATION_TRANSFORMED_TOPIC = "org.chicago.cta.stations.table.v1t001" 
    WEATHER_TOPIC_NAME = "com.cta.weather"
    CONSUMER_GRP_ID_PRFX = "com.cta.consumer.grps."
    
    POSTGRES_DB_NAME = "cta"
    POSTGRES_DB_USER = "cta_admin"
    POSTGRES_DB_P = "chicago"
    STATION_DATA_CONNECTOR_NAME = "cta_stationst001"
    STATION_DATA_TOPIC_PREFIX = "com.cta.stations.data.rawt001." 
    FAUST_STATION_TRANSFORMED_TABLE_NAME= "com.cta.faust.stntbl"
    
    # Broker properties keys
    MAP_KEY_SCHEMA_REGISTRY_URL = "schema.registry.url"    
    MAP_KEY_BOOTSTRAP_SERVERS = "bootstrap.servers"
    MAP_KEY_TOPIC_NAME = "topic"
    MAP_KEY_NUM_PARTITIONS = "num_partitions"
    MAP_KEY_REPL_FACTOR = "replication_factor"
    MAP_KEY_CLIENT_ID = "client.id"
    MAP_KEY_LINGER_MS = "linger.ms"
    MAP_KEY_COMPRESSION_TYPE = "compression.type"
    MAP_KEY_BATCH_NUM_MSGS = "batch.num.messages"
    
    #Misc
    colors = IntEnum("colors", "blue green red", start=0)  
    MsgTypes = IntEnum("MsgTypes", "Stations Arrivals Transtile Unknown", start=0)
    # -------------------------------------------------------------            
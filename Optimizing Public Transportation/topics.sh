# !/bin/bash
LOGS=/home/workspace/logs
mkdir -p $LOGS

# Station data Connector created topic
kafka-topics --delete --zookeeper localhost:2181 --topic "com.cta.station.data.stations" > $LOGS/topics.log 2>&1
kafka-topics --delete --zookeeper localhost:2181 --topic "com.cta.station.datax.stations" > $LOGS/topics.log 2>&1

# Topics preferred to be created ahead.
declare -a topics=( 
#"com.cta.stations.addison.blue"
"com.cta.weather"
)

for t in "${topics[@]}"; do
    kafka-topics --delete --zookeeper localhost:2181 --topic $t > $LOGS/topics.log 2>&1
    kafka-topics --create --zookeeper localhost:2181 --topic $t --replication-factor 2 --partitions 2 > $LOGS/topics.log 2>&1
done
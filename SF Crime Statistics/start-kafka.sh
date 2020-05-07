#!/bin/bash
mkdir -p /home/workspace/startup
pushd .
cd /home/workspace

echo 'schema.registry.url=http://localhost:8081' >> /etc/kafka/connect-distributed.properties
systemctl start confluent-zookeeper >> startup/startup.log 2>&1
systemctl start confluent-kafka >> startup/startup.log 2>&1
systemctl start confluent-schema-registry >> startup/startup.log 2>&1
#systemctl start confluent-kafka-rest >> startup/startup.log 2>&1
#systemctl start confluent-kafka-connect >> startup/startup.log 2>&1
#systemctl start confluent-ksql >> startup/startup.log 2>&1



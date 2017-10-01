#!/bin/bash
#Insert 5000 messges for stream topic
for i in {1..5000}
do
   echo "Hello, World" | /opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic /mapr/FiveTwo/srcDir/srcStream:test

done

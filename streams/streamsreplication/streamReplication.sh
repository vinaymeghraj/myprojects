#!/bin/bash

srcCluster=/mapr/FiveTwo
dstCluster=/mapr/test
srcDir=srcDir
dstDir=dstDir
srcStream=srcStream
dstStream=dstStream

src=${srcCluster}/${srcDir}/${srcStream}
echo ${src}
dst=${dstCluster}/${dstDir}/${dstStream}
echo ${dst}

hadoop fs -mkdir ${srcCluster}/${srcDir}
echo hadoop fs -mkdir ${srcCluster}/${srcDir}
hadoop fs -mkdir ${dstCluster}/${dstDir}
echo hadoop fs -mkdir ${dstCluster}/${dstDir}



#step 1) Create source stream
maprcli stream create -path ${src} -produceperm p -consumeperm p -topicperm p -defaultpartitions 3
echo maprcli stream create -path ${src} -produceperm p -consumeperm p -topicperm p -defaultpartitions 3

#Step 2) Create replica Stream
maprcli stream create -path ${dst} -produceperm p -consumeperm p -topicperm p -defaultpartitions 3
echo maprcli stream create -path ${dst} -produceperm p -consumeperm p -topicperm p -defaultpartitions 3


#Step 3) Copy meta from src to replica stream
maprcli stream create -path ${dst} -copymetafrom ${src}
echo maprcli stream create -path ${dst} -copymetafrom ${src}

#Step 4) Register the replica as a replica of the source stream by running the maprcli stream replica add command
maprcli stream replica add -path ${src} -replica ${dst} -paused true
echo maprcli stream replica add -path ${src} -replica ${dst} -paused true

#Step 4a) Verify that you specified the correct replica by running the maprcli stream replica list command
maprcli stream replica list -path ${src} -json
echo maprcli stream replica list -path ${src} -json

#Step 5) Authorize replication between the streams by defining the source stream as the upstream stream for the replica
maprcli stream upstream add -path ${dst} -upstream ${src}
echo maprcli stream upstream add -path ${dst} -upstream ${src}

#Step 5a) Verify that you specified the correct source stream by running the maprcli stream upstream list command 
maprcli stream upstream list -path ${dst} -json
echo maprcli stream upstream list -path ${dst} -json

#Step 6) Load the replica with data from the source stream by using the mapr copystream utility.
mapr copystream -src ${src} -dst ${dst}
echo mapr copystream -src ${src} -dst ${dst}

echo "Hello, World" | /opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ${src}:test

#Step 7) Resume replica
maprcli stream replica resume -path ${src} -replica ${dst}
echo maprcli stream replica resume -path ${src} -replica ${dst}

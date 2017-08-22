#!/bin/bash

srcCluster=/mapr/FiveTwo
dstCluster=/mapr/test
srcDir=srcDir
dstDir=dstDir
srcStream=srcStream
dstStream=dstStream

src=${srcCluster}/${srcDir}/${srcStream}
dst=${dstCluster}/${dstDir}/${dstStream}


maprcli stream delete -path ${src}
maprcli stream delete -path ${dst}

hadoop fs -rmdir ${srcCluster}/${srcDir}
hadoop fs -rmdir ${dstCluster}/${dstDir}

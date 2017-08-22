#!/bin/bash

#Check that no other sysstat processes are already running, no need to let duplicates run
if [ `ps -ef | grep -w "mpstat" | grep -v -e grep | wc -l` -ne 0 ]; then
        echo Found an \"mpstat\" already running, stop it first before running this script
        exit
elif [ `ps -ef | grep -w "vmstat" | grep -v -e grep | wc -l` -ne 0 ]; then
        echo Found a \"vmstat\" already running, stop it first before running this script
        exit
elif [ `ps -ef | grep -w "iostat" | grep -v -e grep | wc -l` -ne 0 ]; then
        echo Found an \"iostat\" already running, stop it first before running this script
        exit
elif [ `ps -ef | grep -w “top” | grep -v -e grep | wc -l` -ne 0 ]; then
        echo Found a \”top\” already running, stop it first before running this script
        exit
elif [ `ps -ef | grep -w “netstat” | grep -v -e grep | wc -l` -ne 0 ]; then
        echo Found a \”netstat\” already running, stop it first before running this script
        exit
elif [ `ps -ef | grep -w “sar” | grep -v -e grep | wc -l` -ne 0 ]; then
        echo Found a \”sar\” already running, stop it first before running this script
        exit
fi


logDir=/opt/mapr/logs
if [ ! -d $logDir ]; then
  echo ERROR: Not a directory: $logDir
  exit 1
fi

LongInterval=10
ShortPollingInterval=2

# I/O details for devices, partitions and network filesystems

iostat -cdnmx $ShortPollingInterval  | awk '{now=strftime("%Y-%m-%d %H:%M:%S "); print now $0}' >> $logDir/iostat.$HOSTNAME.out 2>&1 &

# collect detailed processor related statistics

mpstat -P ALL $ShortPollingInterval | awk '{now=strftime("%Y-%m-%d %H:%M:%S "); print now $0}' >> $logDir/mpstat.$HOSTNAME.out 2>&1 &

# quick snapshot of the system

vmstat -n -SM $ShortPollingInterval | awk '{now=strftime("%Y-%m-%d %H:%M:%S "); print now $0}' >> $logDir/vmstat.$HOSTNAME.out 2>&1 &

#Top-Threads for Every process

top -b -H -d $LongInterval | awk '{now=strftime("%Y-%m-%d %H:%M:%S "); print now $0}' >> $logDir/top.threads.$HOSTNAME.out 2>&1 &

#Top for Every process

top -b -d $LongInterval | awk '{now=strftime("%Y-%m-%d %H:%M:%S "); print now $0}' | grep -v -e " 0\.0 *0\.0 " >> $logDir/top.processes.$HOSTNAME.out 2>&1 &

#Gather details of Network devices with the stats .

sar -n DEV $ShortPollingInterval >>  $logDir/sar.dev.$HOSTNAME.out 2>&1 &
sar -n EDEV $ShortPollingInterval >>  $logDir/sar.error.dev.$HOSTNAME.out 2>&1 &

#Begin loop to periodically check the system and gather misc diagnostics
while [ 1 ]; do

#Details of every tcp/udp connection

netstat -pan | egrep -w 'tcp|udp'  | tr -s '  ' ' ' | awk '{now=strftime("%Y-%m-%d %H:%M:%S "); print now $0}'  >>  $logDir/netstat.pan.$HOSTNAME.out 2>&1 &
ps aux | awk '{now=strftime("%Y-%m-%d %T "); print now $0}'>> $logDir/psOutput.$HOSTNAME.log 2>&1 &
#/usr/bin/gstack  $MfsPid | awk '{now=strftime("%Y-%m-%d %H:%M:%S "); print now $0}'  >> $logDir/gstack.mfsprocesses.$HOSTNAME.out 2>&1 &
#jstat -gcutil 11464 | awk '{now=strftime("%Y-%m-%d %T "); print now $0}'>> $logDir/gstat.$HOSTNAME.log 2>&1 &
 #Sleep before taking next sample
     sleep $LongInterval
done

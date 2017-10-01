#!/bin/bash
for i in `ps -ef| grep -w mpstat  | grep -v grep | awk '{print $2}'`; do kill -9 $i; done
for i in `ps -ef| grep -w sar | grep -v grep | awk '{print $2}'`; do kill -9 $i; done
for i in `ps -ef| grep -w iostat  | grep -v grep | awk '{print $2}'`; do kill -9 $i; done
for i in `ps -ef| grep -w vmstat | grep -v grep | awk '{print $2}'`; do kill -9 $i; done
for i in `ps -ef| grep -w netstat | grep -v grep | awk '{print $2}'`; do kill -9 $i; done
for i in `ps -ef| grep -w top | grep -v grep | awk '{print $2}'`; do kill -9 $i; done

#!/bin/bash
# starts a spark slave node for a given master

export SPARK_HOME=/home/ubuntu/spark
until sudo /home/ubuntu/spark/sbin/start-slave.sh spark://${master_address}:7077
do
  sleep 1.0
done

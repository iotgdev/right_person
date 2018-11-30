#!/bin/bash
# starts a spark slave node for a given master

export SPARK_HOME=/home/ubuntu/spark
export SPARK_WORKER_MEMORY=$(echo `free -m | awk '/^Mem:/{print $2}'`*0.8 | bc | cut -f1 -d'.' | awk '{print $1"m"}')
until sudo /home/ubuntu/spark/sbin/start-slave.sh spark://${master_address}:7077 -m $SPARK_WORKER_MEMORY
do
  sleep 1.0
done

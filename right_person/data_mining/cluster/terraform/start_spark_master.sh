#!/bin/bash
# Starts a spark master node

export SPARK_HOME=/home/ubuntu/spark
export SPARK_WORKER_MEMORY=$(echo `free -m | awk '/^Mem:/{print $2}'`*0.8 | bc | cut -f1 -d'.' | awk '{print $1"m"}')
swapoff -a
source /home/ubuntu/spark/sbin/start-master.sh

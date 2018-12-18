#!/bin/bash
# starts a spark slave node for a given master

export SPARK_HOME=/home/ubuntu/spark
export SPARK_WORKER_MEMORY=${spark_max_mem_gb}
swapoff -a
until sudo /home/ubuntu/spark/sbin/start-slave.sh spark://${master_address}:7077 -m $SPARK_WORKER_MEMORY
do
  sleep 1.0
done

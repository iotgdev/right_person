#!/usr/bin/env python
# -*- coding: utf-8 -*-

SPARK_PORT = 7077
WEB_UI_PORT = 8080
TASK_SCHEDULER_PORT = 45523
BLOCK_MANAGER_PORT = 50070


PORT_PURPOSES = {
    WEB_UI_PORT: 'Web UI',
    SPARK_PORT: 'spark',
    BLOCK_MANAGER_PORT: 'block manager',
    TASK_SCHEDULER_PORT: 'task scheduler'
}


MASTER_SG_PORTS = {WEB_UI_PORT, }


NODE_SG_PORTS = {SPARK_PORT, TASK_SCHEDULER_PORT, BLOCK_MANAGER_PORT}

MASTER_USER_DATA = """#!/bin/bash
set -x
exec > >(tee /var/log/user-data.log|logger -t user-data ) 2>&1
echo BEGIN
date '+%Y-%m-%d %H:%M:%S'
export SPARK_HOME=/home/ubuntu/spark
swapoff -a
source /home/ubuntu/spark/sbin/start-master.sh
"""

NODE_USER_DATA = """#!/bin/bash
set -x
exec > >(tee /var/log/user-data.log|logger -t user-data ) 2>&1
echo BEGIN
date '+%Y-%m-%d %H:%M:%S'
export SPARK_HOME=/home/ubuntu/spark
swapoff -a
until sudo /home/ubuntu/spark/sbin/start-slave.sh spark://{master_address}:{spark_port}
do
  sleep 1.0
done
"""

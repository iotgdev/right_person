#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time

import boto3

from spark_data_miner.cluster.components.ec2.constants import PORT_PURPOSES


logger = logging.getLogger('spark_data_miner.cluster.components.ec2.utils')


def ec2_client(region):
    """gets a boto3 ec2 client"""
    return boto3.client('ec2', region_name=region)


def ip_rule_template(cidr, port):
    """formats an ip rule for boto3 security groups"""
    return {
        'FromPort': port,
        'IpProtocol': 'tcp',
        'IpRanges': [
            {
                'CidrIp': cidr,
                'Description': 'ip range from ' + PORT_PURPOSES.get(port, 'UNKNOWN')
            },
        ],
        'ToPort': port
    }


def sg_rule_template(security_group, port):
    """formats a security group rule for boto3 security groups"""
    return {
        'ToPort': port,
        'FromPort': port,
        'IpProtocol': 'tcp',
        'UserIdGroupPairs': [
            {
                'GroupId': security_group,
            }
        ],
    }


def get_ingress_rules(ip_ranges, security_groups, ports):
    """gets all security group rules for given ip ranges, security groups and ports"""
    ip_rules = get_ip_rules(ip_ranges, ports)
    sg_rules = get_security_group_rules(security_groups, ports)
    return ip_rules + sg_rules


def get_ip_rules(ip_ranges, ports):
    """gets all ip rules for a security group"""
    return [ip_rule_template(cidr, port) for cidr in ip_ranges for port in ports]


def get_security_group_rules(security_groups, ports):
    """gets all security group rules for a security group"""
    return [sg_rule_template(security_group, port) for port in ports for security_group in security_groups]


def get_instance(region, instance_id):
    """
    gets an instance response from boto.
    :type region: str
    :type instance_id: str
    :return: dict
    """
    resp = ec2_client(region).describe_instances(InstanceIds=[instance_id])
    instance = resp['Reservations'][0]['Instances'][0]
    return instance


def wait_for_instance(region, instance, max_retry=12):
    """
    waits for an instance to be in a "running" state
    returns info about the running instance (data that's not available until the state has been achieved)
    :type region: str
    :type instance: dict
    :type max_retry: int
    :rtype: dict
    """
    while instance['State']['Name'] != 'running' and max_retry > 0:
        logger.info('Waiting for instance.')
        max_retry -= 1
        time.sleep(10)
        instance = get_instance(region, instance['InstanceId'])
    return instance

#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import logging
import sys
import time
from contextlib import contextmanager

import boto3

from spark_data_miner.cluster.ami.constants import BASE_IMAGE, NAME_FORMAT, COMMANDS
from spark_data_miner.cluster.components.ec2.utils import wait_for_instance, ec2_client
from spark_data_miner.cluster.utils import describe_ec2_properties_from_instance

logger = logging.getLogger('spark_data_miner.ami.utils')


def get_base(region):
    """
    gets a base image for the AMI
    any AMI with automatic installation of AWS ServiceManager is required
    see: https://docs.aws.amazon.com/systems-manager/latest/userguide/ssm-agent.html
    :type region: str
    :rtype: dict
    """
    images = ec2_client(region).describe_images(**BASE_IMAGE)['Images']
    return sorted(images, key=lambda x: x['CreationDate'], reverse=True)[0]


def create_ami_instance(region, subnet_id, profile_arn, image_id, instance_type='t3.small'):
    """
    creates an ami instance. This instance acts as the template of the AMI
    :type region: str
    :type subnet_id: str
    :type profile_arn: str
    :type image_id: str
    :type instance_type: str
    :rtype: dict
    """
    instance_name = datetime.datetime.now().strftime('spark-data-miner-ami-builder-%s')
    tag_specs = [{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': instance_name}]}]
    return ec2_client(region).run_instances(
        ImageId=image_id, SubnetId=subnet_id, InstanceType=instance_type, MaxCount=1, MinCount=1, KeyName='labs',
        IamInstanceProfile={'Arn': profile_arn}, TagSpecifications=tag_specs
    )['Instances'][0]


@contextmanager
def temporary_ami_instance(region, subnet_id, profile_arn):
    """
    allows access to a temporary instance with no ssh keys.
    destroys the instance when finished
    :type region: str
    :type subnet_id: str
    :type profile_arn: str
    :yields instance: dict
    """
    base_image = get_base(region)
    instance = wait_for_instance(region, create_ami_instance(region, subnet_id, profile_arn, base_image['ImageId']))
    try:
        yield instance
    finally:
        ec2_client(region).terminate_instances(InstanceIds=[instance['InstanceId']])


def wait_for_ssm(region, instance_id, max_retry=6):
    """
    waits for an instance to become "ssm ready" (recognised by the AWS Systems Manager service)
    :type region: str
    :type instance_id: str
    :type max_retry: int
    """
    client = boto3.client('ssm', region_name=region)
    filters = [{'Key': 'InstanceIds', 'Values': [instance_id]}]

    while not bool(client.describe_instance_information(Filters=filters)['InstanceInformationList']) and max_retry > 0:
        logger.info('Waiting for ssm.')
        max_retry -= 1
        time.sleep(10)


def wait_for_ami(region, image_id, max_retry=30):
    """
    waits for an ami to be registered as available
    :type region: str
    :type image_id: str
    :type max_retry: int
    """
    while ec2_client(region).describe_images(ImageIds=[image_id])['Images'][0]['State'] != 'available' and \
            max_retry > 0:
        logger.info('Waiting for ami.')
        time.sleep(10)
        max_retry -= 1


def ami_exists(region):
    """
    check if an AMI exists that the spark_data_miner can use
    :type region: str
    :rtype: bool
    """
    filters = [{'Name': 'name', 'Values': [NAME_FORMAT.format('*')]}]
    return bool(ec2_client(region).describe_images(Filters=filters)['Images'])


def get_ami(region):
    """
    gets the most recent AMI that the spark_data_miner can use
    :type region: str
    :rtype: dict
    """
    filters = [{'Name': 'name', 'Values': [NAME_FORMAT.format('*')]}]
    images = ec2_client(region).describe_images(Filters=filters)['Images']
    return sorted(images, key=lambda x: x['CreationDate'], reverse=True)[0]


def run_commands(region, instance_id):
    """
    runs commands against an ubuntu instance. Requires instance to be valid under aws ssm.
    :type region: str
    :type instance_id: str
    """
    client = boto3.client('ssm', region_name=region)
    for i, cmd in enumerate(COMMANDS, start=1):
        logger.info('Issuing command {}/{}.'.format(i, len(COMMANDS)))
        params = {'commands': [cmd]}
        resp = client.send_command(InstanceIds=[instance_id], DocumentName="AWS-RunShellScript", Parameters=params)
        command = resp['Command']
        count = 0
        while command['Status'] != 'Success' or count >= 30:
            logger.info('Running command {}: waiting...'.format(i))
            # noinspection PyBroadException
            try:
                command = client.get_command_invocation(CommandId=command['CommandId'], InstanceId=instance_id)
            except Exception:
                logger.info('Pending command {}: AWS not ready.'.format(i))
            if command['Status'] in {'Cancelled', 'TimedOut', 'Failed', 'Cancelling'}:
                raise ValueError('Command "{}" Failed:\n'.format(cmd) + command['StandardErrorContent'])
            count += 1
            time.sleep(10)


def create_ami(region, subnet_id, profile_arn):
    """
    creates an ami with the relevant specs for spark_data_miner
    :type region: str
    :type subnet_id: str
    :type profile_arn: str
    """
    with temporary_ami_instance(region, subnet_id, profile_arn) as instance:
        wait_for_ssm(region, instance['InstanceId'])
        run_commands(region, instance['InstanceId'])
        image_name = NAME_FORMAT.format(datetime.datetime.now().strftime('%s'))
        image = ec2_client(region).create_image(InstanceId=instance['InstanceId'], Name=image_name)
        wait_for_ami(region, image['ImageId'])
        logger.info('New ami is ready {}'.format(image['ImageId']))


def create_ami_from_instance():
    """
    creates an ami with the relevant spec for spark_data_miner, using properties of the executing instance
    """
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(message)s')
    properties = describe_ec2_properties_from_instance()
    create_ami(properties.region, properties.subnet_id, properties.profile['Arn'])

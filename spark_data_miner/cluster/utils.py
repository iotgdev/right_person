import os
import shutil
import sys
import tempfile
import ujson
from collections import namedtuple

import boto3
import requests

from spark_data_miner.cluster.components.ec2.utils import ec2_client


EC2Properties = namedtuple(
    'EC2Properties', 'region vpc_id subnet_id security_groups key_name public_ip private_ip profile')


def add_package_to_spark(session, package_name):  # todo: refocus around the class rather than the function
    """
    adds a python package to the spark context by package import
    :type session: pyspark.SparkSession
    :param package_name: a python imported package name
    """
    tar_name = os.path.join(tempfile.mkdtemp(), package_name)  # todo: clean up after process exit
    package_location = os.path.abspath(os.path.join(os.path.dirname(sys.modules[package_name].__file__), os.pardir))
    package_tar = shutil.make_archive(tar_name, 'zip', package_location)
    session.sparkContext.addPyFile(package_tar)


def describe_ec2_properties_from_instance():
    properties = [
        'VpcId', 'SubnetId', 'SecurityGroups', 'KeyName', 'PublicIpAddress', 'PrivateIpAddress', 'IamInstanceProfile']
    instance_id = requests.get('http://169.254.169.254/latest/meta-data/instance-id').text
    region = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document").json()['region']
    resp = ec2_client(region).describe_instances(InstanceIds=[instance_id])
    details = resp['Reservations'][0]['Instances'][0]
    return EC2Properties(*[region] + [details[prop] for prop in properties])


def get_instance_memory(instance_type):
    """
    Gets the maximum instance memory for a given instance_type
    :param str instance_type: e.g. r5.2xlarge
    :rtype: int
    """
    client = boto3.client('pricing', region_name='us-east-1')  # only available in us-east-1 and ap-northeast-1
    filters = {
        'instanceType': instance_type,
        'operatingSystem': 'Linux',
        'tenancy': 'Host',
        'location': 'US East (N. Virginia)'
    }
    filter_args = [{'Type': 'TERM_MATCH', 'Field': x, 'Value': y} for x, y in filters.items()]
    instance = ujson.loads(client.get_products(ServiceCode='AmazonEC2', Filters=filter_args)['PriceList'][0])['product']
    return int(instance['attributes']['memory'].replace(' GiB', ''))

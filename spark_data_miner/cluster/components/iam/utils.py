
import boto3

from spark_data_miner.cluster.components.iam.constants import _GENERIC_DESCRIBE, _S3_READ_WRITE, _ASSUME_ROLE


def iam_client(region='eu-west-1'):
    """
    get a client for iam resources, default region: eu-west-1
    :type region: str
    :rtype botocore.client.EC2:
    """
    return boto3.client('iam', region_name=region)


def get_policy_documents():
    """
    returns the iam policy document templates
    :rtype: list[dict, dict]
    """
    return [_S3_READ_WRITE, _GENERIC_DESCRIBE]


def get_assume_role():
    """
    returns the assume role policy for an ec2 instance
    :rtype: dict
    """
    return _ASSUME_ROLE

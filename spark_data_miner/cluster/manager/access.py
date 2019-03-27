#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
import datetime
import logging
import time
import ujson
import uuid
from collections import namedtuple

from botocore.exceptions import ClientError

from spark_data_miner.cluster.ami.utils import get_ami
from spark_data_miner.cluster.components.ec2.constants import MASTER_SG_PORTS, NODE_SG_PORTS, MASTER_USER_DATA, \
    NODE_USER_DATA, SPARK_PORT
from spark_data_miner.cluster.components.ec2.utils import ec2_client, \
    get_ingress_rules, wait_for_instance
from spark_data_miner.cluster.utils import describe_ec2_properties_from_instance
from spark_data_miner.cluster.components.iam.utils import get_policy_documents, iam_client, get_assume_role


logger = logging.getLogger('spark_data_miner.cluster.manager.access')


ClusterPlan = namedtuple('ClusterPlan', ['master_type', 'node_type', 'node_count'])


class ClusterManager(object):
    """Manages AWS EC2 instance resources"""

    def __init__(self, plan, cluster_id=None):
        self.__registry = {}
        self.__plan = plan
        self.cluster_id = cluster_id or str(uuid.uuid4())

    def __enter__(self):
        """Context manager entry; create the cluster and provide a copy of the registry"""
        self.create()
        return self.registry

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit; destroy the cluster"""
        self.destroy()

    def __del__(self):
        """Object deletion; destroy the cluster"""
        self.destroy()

    @property
    def instance_properties(self):
        """Gathers properties about the current instance"""
        if not hasattr(self, '_instance_properties'):
            setattr(self, '_instance_properties', describe_ec2_properties_from_instance())
        return getattr(self, '_instance_properties')

    @property
    def ami(self):
        return get_ami(self.instance_properties.region)

    @property
    def plan(self):
        """Provides the plan for the cluster"""
        return self.__plan

    @property
    def registry(self):
        """Returns a read only copy of the registry"""
        return copy.deepcopy(self.__registry)

    def create(self):
        """creates the cluster"""
        if not self.__registry:
            self.__create_policies()
            self.__create_role()
            self.__create_instance_profile()
            self.__create_security_groups()
            self.__create_master()
            self.__create_nodes()

    def destroy(self):
        """destroys the cluster"""
        if self.__registry:
            self.__destroy_nodes()
            self.__destroy_master()
            self.__destroy_security_groups()
            self.__destroy_instance_profile()
            self.__destroy_role()
            self.__destroy_policies()

    def __create_security_groups(self):
        """creates the security groups (with necessary access) for the cluster"""
        region, vpc_id, subnet, security_groups, key_name, public_ip, private_ip, profile = self.instance_properties

        access_cidrs = map('{}/32'.format, [public_ip, private_ip])
        security_group_ids = [g['GroupId'] for g in security_groups]

        master_name = 'spark-data-miner-master-{}-{}'.format(self.cluster_id, datetime.datetime.now().strftime('%s'))
        master = ec2_client(region).create_security_group(
            Description=master_name, GroupName=master_name, VpcId=vpc_id)
        master['GroupName'] = master_name
        self.__add_access_rules(master['GroupId'], access_cidrs, security_group_ids, MASTER_SG_PORTS)

        node_name = 'spark-data-miner-node-{}-{}'.format(self.cluster_id, datetime.datetime.now().strftime('%s'))
        node = ec2_client(region).create_security_group(Description=node_name, GroupName=node_name, VpcId=vpc_id)
        node['GroupName'] = node_name
        self.__add_access_rules(node['GroupId'], access_cidrs, security_group_ids, NODE_SG_PORTS)

        cluster_security_groups = [master, node]
        self.__registry['security_groups'] = cluster_security_groups

    def __add_access_rules(self, input_group_id, ip_addresses, extra_group_ids, ports):
        """adds required access rules to the security groups of the cluster"""
        region = self.instance_properties.region
        total_group_access = [input_group_id] + extra_group_ids
        ingress_rules = get_ingress_rules(ip_addresses, total_group_access, ports)
        ec2_client(region).authorize_security_group_ingress(GroupId=input_group_id, IpPermissions=ingress_rules)

    def __create_master(self):
        """creates the cluster's master node"""
        instance_name = 'spark-data-miner-master-{}-{}'.format(self.cluster_id, datetime.datetime.now().strftime('%s'))
        tag_specs = [{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': instance_name}]}]
        region, vpc_id, subnet, security_groups, key_name, public_ip, private_ip, profile = self.instance_properties

        instance_profile = self.registry['instance_profile']
        security_group_ids = [sg['GroupId'] for sg in security_groups + self.registry['security_groups']]
        instance = None
        max_retrys = 5
        while instance is None:
            try:
                instance = ec2_client(region).run_instances(
                    ImageId=self.ami['ImageId'], SubnetId=subnet, InstanceType=self.plan.master_type, MaxCount=1,
                    UserData=MASTER_USER_DATA, IamInstanceProfile={'Name': instance_profile['InstanceProfileName']},
                    KeyName=key_name, SecurityGroupIds=security_group_ids, TagSpecifications=tag_specs, MinCount=1,
                )['Instances'][0]
            except ClientError:
                if max_retrys <= 0:
                    raise
                max_retrys -= 1
                time.sleep(10)
        instance = wait_for_instance(region, instance)
        self.__registry['cluster_master'] = instance

    def __create_nodes(self):
        """creates the non-master cluster nodes"""
        instance_name = 'spark-data-miner-node-{}-{}'.format(self.cluster_id, datetime.datetime.now().strftime('%s'))
        tag_specs = [{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': instance_name}]}]
        region, vpc_id, subnet, security_groups, key_name, public_ip, private_ip, profile = self.instance_properties
        image = get_ami(region)
        instance_profile = self.registry['instance_profile']['Arn']
        security_group_ids = [sg['GroupId'] for sg in security_groups + self.registry['security_groups'][-1:]]
        master_host = self.registry['cluster_master']['PrivateIpAddress']
        instances = ec2_client(region).run_instances(
            ImageId=image['ImageId'], SubnetId=subnet, InstanceType=self.plan.node_type, KeyName=key_name,
            MaxCount=self.plan.node_count, MinCount=self.plan.node_count, SecurityGroupIds=security_group_ids,
            UserData=NODE_USER_DATA.format(master_address=master_host, spark_port=SPARK_PORT),
            IamInstanceProfile={'Arn': instance_profile}, TagSpecifications=tag_specs,
        )['Instances']
        instance_ids = [instance['InstanceId'] for instance in instances]
        retries = max(len(instances), 6)
        while not all(instance['State']['Name'] == 'running' for instance in instances) and retries > 0:
            retries -= 1
            instances = ec2_client(region).describe_instances(
                InstanceIds=instance_ids)['Reservations'][0]['Instances']
            time.sleep(10)
        self.__registry['cluster_nodes'] = instances

    def __destroy_nodes(self):
        """destroys the non-master cluster nodes"""
        region = self.instance_properties.region
        instance_ids = [i['InstanceId'] for i in self.registry['cluster_nodes']]
        terminated = ec2_client(region).terminate_instances(InstanceIds=instance_ids)['TerminatingInstances']
        max_retrys = 18
        while any(i['CurrentState']['Name'] != 'terminated' for i in terminated) and max_retrys > 0:
            time.sleep(10)
            max_retrys -= 1
            terminated = ec2_client(region).terminate_instances(InstanceIds=instance_ids)['TerminatingInstances']
        del self.__registry['cluster_nodes']

    def __destroy_master(self):
        """destroys the master cluster node"""
        region = self.instance_properties.region
        instance_id = self.registry['cluster_master']['InstanceId']
        terminated = ec2_client(region).terminate_instances(InstanceIds=[instance_id])['TerminatingInstances']
        max_retrys = 18
        while any(i['CurrentState']['Name'] != 'terminated' for i in terminated) and max_retrys > 0:
            time.sleep(10)
            max_retrys -= 1
            terminated = ec2_client(region).terminate_instances(InstanceIds=[instance_id])['TerminatingInstances']
        del self.__registry['cluster_master']

    def __destroy_security_groups(self):
        """destroys the security groups"""
        region = self.instance_properties.region
        for group in self.__registry['security_groups'][::-1]:
            ec2_client(region).delete_security_group(GroupId=group['GroupId'])
        del self.__registry['security_groups']

    def __create_policies(self):
        """creates the iam policies for the spark_data_miner instances"""
        policies = []
        for i, doc in enumerate(get_policy_documents()):
            policy_name = 'spark-data-miner-{}-{}-{}'.format(i, self.cluster_id, datetime.datetime.now().strftime('%s'))
            policy = iam_client().create_policy(PolicyName=policy_name, PolicyDocument=ujson.dumps(doc))
            policies.append(policy['Policy'])
        self.__registry['policies'] = policies

    def __create_role(self):
        """creates the iam role for the spark data miner"""
        role_name = 'spark-data-miner-{}-{}'.format(self.cluster_id, datetime.datetime.now().strftime('%s'))
        policy_document = ujson.dumps(get_assume_role())
        resp = iam_client().create_role(RoleName=role_name, AssumeRolePolicyDocument=policy_document)
        for policy in self.registry['policies']:
            iam_client().attach_role_policy(RoleName=role_name, PolicyArn=policy['Arn'])
        self.__registry['role'] = resp['Role']

    def __create_instance_profile(self):
        """creates the instance profile for the spark data miner"""
        profile_name = '-'.join(('spark-data-miner', self.cluster_id, datetime.datetime.now().strftime('%s')))
        resp = iam_client().create_instance_profile(InstanceProfileName=profile_name)
        profile_name = resp['InstanceProfile']['InstanceProfileName']
        role_name = self.registry['role']['RoleName']
        iam_client().add_role_to_instance_profile(InstanceProfileName=profile_name, RoleName=role_name)
        self.__registry['instance_profile'] = resp['InstanceProfile']

    def __destroy_instance_profile(self):
        """destroys the instance profile for the spark data miner"""
        profile_name = self.__registry['instance_profile']['InstanceProfileName']
        role_name = self.__registry['role']['RoleName']
        # noinspection PyBroadException
        try:
            iam_client().remove_role_from_instance_profile(InstanceProfileName=profile_name, RoleName=role_name)
        except Exception:
            logger.warning(
                'could not detach role ({}) from instance_profile ({})'.format(role_name, profile_name))
        iam_client().delete_instance_profile(InstanceProfileName=profile_name)
        del self.__registry['instance_profile']

    def __destroy_role(self):
        """destroys the role for the spark data miner"""
        role_name = self.__registry['role']['RoleName']
        for policy in self.__registry['policies']:
            # noinspection PyBroadException
            try:
                iam_client().detach_role_policy(RoleName=role_name, PolicyArn=policy['Arn'])
            except Exception:
                logger.warning('could not detach policy ({}) from role ({})'.format(policy['PolicyName'], role_name))
        iam_client().delete_role(RoleName=role_name)
        del self.__registry['role']

    def __destroy_policies(self):
        """destroys the policies for the spark data miner"""
        for policy in self.__registry['policies']:
            iam_client().delete_policy(PolicyArn=policy['Arn'])
        del self.__registry['policies']

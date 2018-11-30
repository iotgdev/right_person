#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Commands for creating and destroying a right_person spark cluster.

A global cluster manager class handles the cleanup of temporary resources.
A cluster does not need to be created by the current process to be managed by the current process.
This code is not multiprocess/thread safe.

usage:
>>> cluster_ip = create_right_person_cluster('cluster-1')
>>> # do some work with the cluster ip, connect to it etc.
>>> destroy_right_person_cluster('cluster-1')
"""
from __future__ import unicode_literals

import logging
import os
import shutil
import tempfile
import ujson

from right_person.data_mining.cluster.utils import get_current_ipv4, run_system_shell_process, get_terraform_vars, \
    TERRAFORM_DIRECTORY, TERRAFORM_VARS


logger = logging.getLogger('right_person.data_mining.cluster.manager')


_TERRAFORM_INIT_FUNCTION = """yes no |
terraform init
-backend-config 'bucket={cluster_state_bucket}'
-backend-config 'region={cluster_region}'
-backend-config 'key={cluster_region}/right_person/{cluster_id}/tf.state'
-input=false
-no-color
-upgrade
-from-module={input_location}
{terraform_state_location}
"""


_TERRAFORM_PLAN_FUNCTION = """
terraform plan
-input=false
-no-color
-out={plan_output_location}
-var-file={terraform_tfvars_json_file}
-var 'ip_whitelist={extended_ip_whitelist}'
-var 'cluster_id={cluster_id}'
{terraform_state_location}
"""


_TERRAFORM_APPLY_FUNCTION = """
terraform apply
-auto-approve
-input=false
-no-color
{plan_output_location}
"""


_TERRAFORM_DESTROY_FUNCTION = """
terraform destroy
-auto-approve
-input=false
-no-color
-var-file={terraform_tfvars_json_file}
-var 'cluster_id={cluster_id}'
{terraform_state_location}
"""


class TerraformManager(dict):
    """Cleans up temporary directories used to create terraform resources"""

    def __del__(self):
        for cluster_id, (cluster_ip, cluster_dir) in self.items():
            try:
                shutil.rmtree(cluster_dir)
            except (Exception, ):
                logger.warning('Could not delete cluster {}: master_ip: {}, tmp_dir: {}'.format(
                    cluster_id, cluster_ip, cluster_dir
                ))


def create_right_person_cluster(cluster_id):
    """
    Creates a right person cluster as specified by the config
    :type cluster_id: str
    :rtype: str
    :returns: the ip address of the master instance on the cluster
    """

    global _CLUSTER_MANAGER

    if _CLUSTER_MANAGER.get(cluster_id):
        cluster_ip, cluster_dir = _CLUSTER_MANAGER[cluster_id]
        return cluster_ip

    current_ip = get_current_ipv4()
    master_ip = None
    terraform_state_location = tempfile.mkdtemp()
    plan_output_location = os.path.join(terraform_state_location, 'plan')
    terraform_vars = get_terraform_vars()
    state_s3_path, region = terraform_vars['cluster_state_bucket'], terraform_vars['cluster_region']
    ip_whitelist = terraform_vars.get('ip_whitelist', []) + [current_ip]

    try:

        run_system_shell_process(str(_TERRAFORM_INIT_FUNCTION.format(
            cluster_state_bucket=state_s3_path, cluster_region=region, cluster_id=cluster_id,
            input_location=TERRAFORM_DIRECTORY, terraform_state_location=terraform_state_location
        ).replace('\n', ' ').strip()))

        run_system_shell_process(str(_TERRAFORM_PLAN_FUNCTION.format(
            plan_output_location=plan_output_location,
            terraform_tfvars_json_file=TERRAFORM_VARS,
            extended_ip_whitelist=ujson.dumps(ip_whitelist),
            cluster_id=cluster_id,
            terraform_state_location=os.path.join(terraform_state_location, '')
        ).replace('\n', ' ').strip()))

        output = run_system_shell_process(str(_TERRAFORM_APPLY_FUNCTION.format(
            plan_output_location=plan_output_location
        ).replace('\n', ' ').strip()))

        # currently, terraform does not support operating the output
        # command from a remote location. This is the workaround
        master_ip = output.strip().split('\n')[-1].split('=')[-1].strip()
    except:
        raise
    finally:
        _CLUSTER_MANAGER[cluster_id] = master_ip, terraform_state_location
    return master_ip


def destroy_right_person_cluster(cluster_id):
    """
    destroy a right person cluster.
    The cluster must be managed by the current process.
    :type cluster_id: str
    """

    global _CLUSTER_MANAGER

    try:
        master_ip, state_location = _CLUSTER_MANAGER.pop(cluster_id)
    except:
        raise RuntimeError('This process isn\'t managing that cluster!')

    run_system_shell_process(str(_TERRAFORM_DESTROY_FUNCTION.format(
        terraform_tfvars_json_file=TERRAFORM_VARS,
        terraform_state_location=state_location,
        cluster_id=cluster_id
    ).replace('\n', ' ').strip()))

    shutil.rmtree(state_location)


_CLUSTER_MANAGER = TerraformManager()

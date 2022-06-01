#!/usr/bin/env python3

#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import os

import boto3
from python_terraform import Terraform


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--region',
        required=False,
        default='us-west-2',
        help='The default region to manage resources in.')
    parser.add_argument(
        '--availability-zone1',
        required=False,
        default='us-west-2a',
        help='The default availability zone to manage resources in.')
    parser.add_argument(
        '--availability-zone2',
        required=False,
        default='us-west-2b',
        help='The secondary availability zone.')
    parser.add_argument(
        '--benchmarks-bucket-name',
        required=True,
        help='The name of the AWS S3 bucket that will be used to store benchmark data.')
    parser.add_argument(
        '--source-bucket-name',
        required=False,
        default='devrel-delta-datasets',
        help='The name of the AWS S3 bucket that will be used to store benchmark data.')
    parser.add_argument(
        '--mysql-user',
        required=False,
        default='benchmark',
        help='MySQL database user.')
    parser.add_argument(
        '--mysql-password',
        required=False,
        default='benchmark',
        help='MySQL database password.')
    parser.add_argument(
        '--emr-public-key-path',
        required=False,
        default='~/.ssh/id_rsa.pub',
        help='The path to the public key in the typical format, specified in RFC4716. '
             'The key is necessary to SSH to EMR cluster nodes.')
    parser.add_argument(
        '--emr-workers',
        required=False,
        default='1',
        help='The number of worker nodes in EMR cluster.')
    return parser.parse_args()


def create_infrastructure(args, working_dir='terraform/'):
    parameter_names = ['availability_zone1', 'availability_zone2', 'benchmarks_bucket_name', 'emr_public_key_path',
                       'emr_workers', 'mysql_password', 'mysql_password', 'region', 'source_bucket_name']
    parameters = {}
    for parameter_name in parameter_names:
        parameters[parameter_name] = getattr(args, parameter_name)

    parameters['user_ip_address'] = '95.51.74.181'

    print(f'Creating terraform infrastructure: {parameters}')
    tf = Terraform(working_dir=working_dir)
    return_code, stdout, stderr = tf.init()
    handle_tf_command_output(return_code, stdout, stderr)
    tf.plan()

    # skip_plan=True --> auto-approve=True
    # https://github.com/beelit94/python-terraform/blob/99950cb03c37abadb0d7e136452e43f4f17dd4e1/python_terraform/__init__.py#L109
    return_code, stdout, stderr = tf.apply(var=parameters, skip_plan=True)
    handle_tf_command_output(return_code, stdout, stderr)
    print('Infrastructure created.')


def destroy_infrastructure(working_dir='terraform/'):
    tf = Terraform(working_dir=working_dir)
    tf.destroy()


def handle_tf_command_output(return_code, stdout, stderr):
    print(stdout)
    if return_code != 0:
        print(stdout)
        print(stderr)
        raise RuntimeError('Failed to create infrastructure.')


def upload_directory(path, bucket_name):
    client = boto3.client('s3')
    for root, dirs, files in os.walk(path):
        for file in files:
            client.upload_file(os.path.join(root, file), bucket_name, file)


# python run-end-to-end-tests.py --benchmarks-bucket-name "databricks-performance-benchmarks-data" --emr-workers 1
if __name__ == '__main__':
    args = parse_arguments()
    print(f'Arguments: {args}')
    try:
        create_infrastructure(args)
        upload_directory('test-data/', args.benchmarks_bucket_name)
    finally:
        # destroy_infrastructure()
        pass

#  Copyright 2020 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from typing import Optional

import boto3


class AwsCredentials:

    def __init__(self,
                 access_key_id: Optional[str] = None,
                 secret_access_key: Optional[str] = None,
                 role_arn: Optional[str] = None,
                 session_token: Optional[str] = None,
                 region_name: Optional[str] = 'eu-west-1'):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.role_arn = role_arn
        self.session_token = session_token
        self.region_name = region_name

    @classmethod
    def from_configuration(cls, configuration: dict):
        """
        An AwsCredentials if there is an access_key_id specified, None otherwise
        """
        access_key_id = configuration.get('access_key_id')
        if not isinstance(access_key_id, str):
            return None
        return AwsCredentials(
            access_key_id=access_key_id,
            secret_access_key=configuration.get('secret_access_key'),
            role_arn=configuration.get('role_arn'),
            session_token=configuration.get('session_token'),
            region_name=configuration.get('region', 'eu-west-1'))

    def resolve_role(self, role_session_name: str):
        if self.has_role():
            return self.assume_role(role_session_name)
        return self

    def has_role(self):
        return isinstance(self.role_arn, str)

    def assume_role(self, role_session_name: str):
        self.region_name = self.region_name
        self.sts_client = boto3.client(
            'sts',
            region_name=self.region_name,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            aws_session_token=self.session_token)

        assumed_role_object = self.sts_client.assume_role(
            RoleArn=self.role_arn,
            RoleSessionName=role_session_name
        )
        credentials_dict = assumed_role_object['Credentials']
        return AwsCredentials(
            region_name=self.region_name,
            access_key_id=credentials_dict['AccessKeyId'],
            secret_access_key=credentials_dict['SecretAccessKey'],
            session_token=credentials_dict['SessionToken'])

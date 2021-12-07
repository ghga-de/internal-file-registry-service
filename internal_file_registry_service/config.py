# Copyright 2021 Universität Tübingen, DKFZ and EMBL
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Config Parameter Modeling and Parsing"""

from ghga_service_chassis_lib.config import config_from_yaml
from ghga_service_chassis_lib.postgresql import PostgresqlConfigBase
from ghga_service_chassis_lib.pubsub import PubSubConfigBase
from ghga_service_chassis_lib.s3 import S3ConfigBase


@config_from_yaml(prefix="internal_file_registry_service")
class Config(PubSubConfigBase, PostgresqlConfigBase, S3ConfigBase):
    """Config parameters and their defaults."""

    # stage for outgoing objects (download):
    s3_out_stage_bucket_id: str
    # stage for incomming objects (registration):
    s3_in_stage_bucket_id: str

    service_name: str = "internal_file_registry"
    topic_name_non_staged_file_requested: str = "non_staged_file_requested"
    topic_name_file_staged_for_download: str = "file_staged_for_download"


CONFIG = Config()

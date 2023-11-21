# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

from ghga_service_commons.utils.multinode_storage import S3ObjectStoragesConfig
from hexkit.config import config_from_yaml
from hexkit.providers.akafka import KafkaConfig
from hexkit.providers.mongodb import MongoDbConfig

from ifrs.adapters.inbound.event_sub import EventSubTranslatorConfig
from ifrs.adapters.outbound.event_pub import EventPubTranslatorConfig


# pylint: disable=too-many-ancestors
@config_from_yaml(prefix="ifrs")
class Config(
    MongoDbConfig,
    KafkaConfig,
    EventSubTranslatorConfig,
    EventPubTranslatorConfig,
    S3ObjectStoragesConfig,
):
    """Config parameters and their defaults."""

    service_name: str = "internal_file_registry"


CONFIG = Config()  # type: ignore

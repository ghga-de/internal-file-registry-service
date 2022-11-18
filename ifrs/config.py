# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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

from hexkit.config import config_from_yaml
from hexkit.providers.akafka import KafkaConfig
from hexkit.providers.mongodb import MongoDbConfig
from hexkit.providers.s3 import S3Config

from ifrs.adapters.inbound.event_sub import EventSubTranslatorConfig
from ifrs.adapters.outbound.event_pub import EventPubTranslatorConfig
from ifrs.core.content_copy import StorageEnitiesConfig


@config_from_yaml(prefix="ifrs")
class Config(
    S3Config,
    MongoDbConfig,
    KafkaConfig,
    EventSubTranslatorConfig,
    EventPubTranslatorConfig,
    StorageEnitiesConfig,
):
    """Config parameters and their defaults."""

    service_name: str = "internal_file_registry"


CONFIG = Config()

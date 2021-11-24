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

"""Tests business-functionality of `core` subpackage"""

import pytest
from testcontainers.localstack import LocalStackContainer
from ghga_service_chassis_lib.s3_testing import config_from_localstack_container
from internal_file_registry_service.core.main import copy_file_to_stage

import fixtures
from tests.unit.fixtures.config import get_config


def test_copy_file():
    """Test copying of file"""

    with LocalStackContainer() as localstack:
        s3_config = config_from_localstack_container(localstack)
        config = get_config(**s3_config.dict())

        copy_file_to_stage()

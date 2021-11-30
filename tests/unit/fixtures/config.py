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

"""Test config"""

from pathlib import Path
from typing import Dict, Optional

from ghga_service_chassis_lib.postgresql_testing import config_from_psql_container
from ghga_service_chassis_lib.s3_testing import config_from_localstack_container
from testcontainers.localstack import LocalStackContainer
from testcontainers.postgres import PostgresContainer

from internal_file_registry_service.config import Config

from .utils import BASE_DIR

TEST_CONFIG_YAML = BASE_DIR / "test_config.yaml"


def get_config(
    config_yaml: Path = TEST_CONFIG_YAML,
    localstack_container: Optional[LocalStackContainer] = None,
    psql_container: Optional[PostgresContainer] = None,
):
    """Merges parameters from the default TEST_CONFIG_YAML with params inferred
    from testcontainers."""
    params: Dict[str, object] = {}

    if localstack_container is not None:
        s3_config = config_from_localstack_container(localstack_container)
        params.update(**s3_config.dict())

    if psql_container is not None:
        psql_config = config_from_psql_container(psql_container)
        params.update(**psql_config.dict())

    return Config(config_yaml=config_yaml, **params)

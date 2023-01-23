# Copyright 2021 - 2023 Universität Tübingen, DKFZ and EMBL
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

"""Join the functionality of all fixtures for API-level integration testing."""

__all__ = [
    "joint_fixture",
    "JointFixture",
    "mongodb_fixture",
    "s3_fixture",
    "kafka_fixture",
]

import socket
from dataclasses import dataclass
from typing import AsyncGenerator

import pytest_asyncio
from hexkit.providers.akafka.testutils import KafkaFixture, kafka_fixture
from hexkit.providers.mongodb.testutils import MongoDbFixture  # F401
from hexkit.providers.mongodb.testutils import mongodb_fixture
from hexkit.providers.s3.testutils import S3Fixture, s3_fixture

from ifrs.config import Config
from ifrs.container import Container
from ifrs.main import get_configured_container
from tests.fixtures.config import get_config


def get_free_port() -> int:
    """Finds and returns a free port on localhost."""
    sock = socket.socket()
    sock.bind(("", 0))
    return int(sock.getsockname()[1])


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    config: Config
    container: Container
    mongodb: MongoDbFixture
    s3: S3Fixture
    kafka: KafkaFixture


@pytest_asyncio.fixture
async def joint_fixture(
    mongodb_fixture: MongoDbFixture, s3_fixture: S3Fixture, kafka_fixture: KafkaFixture
) -> AsyncGenerator[JointFixture, None]:
    """A fixture that embeds all other fixtures for API-level integration testing"""

    # merge configs from different sources with the default one:
    config = get_config(
        sources=[mongodb_fixture.config, s3_fixture.config, kafka_fixture.config]
    )

    # create a DI container instance:translators
    async with get_configured_container(config=config) as container:

        # create storage entities:
        await s3_fixture.populate_buckets(
            buckets=[
                config.outbox_bucket,
                config.staging_bucket,
                config.permanent_bucket,
            ]
        )

        yield JointFixture(
            config=config,
            container=container,
            mongodb=mongodb_fixture,
            s3=s3_fixture,
            kafka=kafka_fixture,
        )

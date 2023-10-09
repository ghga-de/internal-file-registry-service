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

"""Join the functionality of all fixtures for API-level integration testing."""

__all__ = [
    "JointFixture",
    "get_joint_fixture",
]

import socket
from asyncio import sleep
from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest_asyncio
from hexkit.providers.akafka.testutils import KafkaFixture
from hexkit.providers.mongodb.testutils import MongoDbFixture
from hexkit.providers.s3.testutils import S3Fixture
from pytest_asyncio.plugin import _ScopeName

from ifrs.config import Config
from ifrs.container import Container
from ifrs.main import get_configured_container
from tests.fixtures.config import get_config

OUTBOX_BUCKET = "outbox"
STAGING_BUCKET = "staging"


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
    outbox_bucket: str
    staging_bucket: str

    async def reset_state(self):
        """Completely reset fixture states"""
        await self.s3.empty_buckets()
        self.mongodb.empty_collections()
        self.kafka.delete_topics()


async def joint_fixture_function(
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
                OUTBOX_BUCKET,
                STAGING_BUCKET,
                config.permanent_bucket,
            ]
        )

        yield JointFixture(
            config=config,
            container=container,
            mongodb=mongodb_fixture,
            s3=s3_fixture,
            kafka=kafka_fixture,
            outbox_bucket=OUTBOX_BUCKET,
            staging_bucket=STAGING_BUCKET,
        )

        # prevent teardown happening before reset_state has finished to prevent hanging
        await sleep(2)


def get_joint_fixture(scope: _ScopeName = "function"):
    """Produce a joint fixture with desired scope"""
    return pytest_asyncio.fixture(joint_fixture_function, scope=scope)

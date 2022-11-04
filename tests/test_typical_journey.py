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

"""Tests typical user journeys"""

from unittest.mock import AsyncMock

import pytest
import requests
from hexkit.providers.mongodb.testutils import mongodb_fixture  # noqa: F401
from hexkit.providers.mongodb.testutils import MongoDbFixture
from hexkit.providers.s3.testutils import file_fixture  # noqa: F401
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401
from hexkit.providers.s3.testutils import FileObject, S3Fixture

from ifrs.adapters.outbound.dao import FileMetadataDaoConstructor
from ifrs.core.content_copy import ContentCopyService, StorageEnitiesConfig
from ifrs.core.file_registry import FileRegistry
from ifrs.ports.outbound.event_broadcast import EventBroadcasterPort
from tests.fixtures.example_data import EXAMPLE_FILE


@pytest.mark.asyncio
async def test_happy(
    s3_fixture: S3Fixture,  # noqa: F811
    mongodb_fixture: MongoDbFixture,  # noqa: F811
    file_fixture: FileObject,  # noqa: F811
):
    """Simulates a typical, successful API journey."""

    # populate the storage with storage entities and place example content in the inbox:
    config = StorageEnitiesConfig(
        outbox_bucket="test-outbox",
        inbox_bucket="test-inbox",
        permanent_bucket="test-permanent",
    )
    await s3_fixture.populate_buckets(
        buckets=[config.outbox_bucket, config.inbox_bucket, config.permanent_bucket]
    )
    file_object = file_fixture.copy(
        update={"bucket_id": config.inbox_bucket, "object_id": EXAMPLE_FILE.file_id}
    )
    await s3_fixture.populate_file_objects(file_objects=[file_object])

    # Setup FileRegistry:
    content_copy_svc = ContentCopyService(
        config=config, object_storage=s3_fixture.storage
    )
    file_metadata_dao = await FileMetadataDaoConstructor.construct(
        dao_factory=mongodb_fixture.dao_factory
    )
    event_broadcaster = AsyncMock(spec=EventBroadcasterPort)
    file_registry = FileRegistry(
        content_copy_svc=content_copy_svc,
        file_metadata_dao=file_metadata_dao,
        event_broadcaster=event_broadcaster,
    )

    # register new file from the inbox:
    await file_registry.register_file(file=EXAMPLE_FILE)

    # check that the file content is now in both the inbox and the permanent storage:
    assert await s3_fixture.storage.does_object_exist(
        bucket_id=config.inbox_bucket, object_id=EXAMPLE_FILE.file_id
    )
    assert await s3_fixture.storage.does_object_exist(
        bucket_id=config.permanent_bucket, object_id=EXAMPLE_FILE.file_id
    )

    # check if an event informing about the new registration has been published:
    event_broadcaster.file_internally_registered.assert_awaited_once_with(
        file=EXAMPLE_FILE
    )

    # request a stage to the outbox:
    await file_registry.stage_registered_file(
        file_id=EXAMPLE_FILE.file_id, decrypted_sha256=EXAMPLE_FILE.decrypted_sha256
    )

    # check that the file content is now in all three storage entities:
    assert await s3_fixture.storage.does_object_exist(
        bucket_id=config.inbox_bucket, object_id=EXAMPLE_FILE.file_id
    )
    assert await s3_fixture.storage.does_object_exist(
        bucket_id=config.permanent_bucket, object_id=EXAMPLE_FILE.file_id
    )
    assert await s3_fixture.storage.does_object_exist(
        bucket_id=config.outbox_bucket, object_id=EXAMPLE_FILE.file_id
    )

    # check that the file content in the outbox is identical to the content in the
    # inbox:
    download_url = await s3_fixture.storage.get_object_download_url(
        bucket_id=config.outbox_bucket, object_id=EXAMPLE_FILE.file_id
    )
    response = requests.get(download_url)
    response.raise_for_status()
    assert response.content == file_object.content

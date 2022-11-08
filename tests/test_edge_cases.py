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

"""Tests edge cases not covered by the typical journey test."""

from unittest.mock import AsyncMock

import pytest
from hexkit.providers.mongodb.testutils import mongodb_fixture  # noqa: F401
from hexkit.providers.mongodb.testutils import MongoDbFixture
from hexkit.providers.s3.testutils import file_fixture  # noqa: F401
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401
from hexkit.providers.s3.testutils import FileObject, S3Fixture

from ifrs.adapters.outbound.dao import FileMetadataDaoConstructor
from ifrs.core.content_copy import ContentCopyService, StorageEnitiesConfig
from ifrs.core.file_registry import FileRegistry
from ifrs.ports.inbound.file_registry import FileRegistryPort
from ifrs.ports.outbound.event_broadcast import EventBroadcasterPort
from tests.fixtures.example_data import EXAMPLE_FILE


@pytest.mark.asyncio
async def test_register_with_empty_inbox(
    s3_fixture: S3Fixture,  # noqa: F811
    mongodb_fixture: MongoDbFixture,  # noqa: F811
):
    """Test registration of a file when the file content is missing from the inbox."""

    # populate the storage with empty storage entities:
    config = StorageEnitiesConfig(
        outbox_bucket="test-outbox",
        inbox_bucket="test-inbox",
        permanent_bucket="test-permanent",
    )
    await s3_fixture.populate_buckets(
        buckets=[config.outbox_bucket, config.inbox_bucket, config.permanent_bucket]
    )

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
    with pytest.raises(FileRegistryPort.FileContentNotInInboxError):
        await file_registry.register_file(file=EXAMPLE_FILE)


@pytest.mark.asyncio
async def test_reregistration(
    s3_fixture: S3Fixture,  # noqa: F811
    mongodb_fixture: MongoDbFixture,  # noqa: F811
    file_fixture: FileObject,  # noqa: F811
):
    """Test the re-registration of a file with identical metadata (should not result in
    an exception)."""

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

    # re-register the file with the exact same metadata:
    await file_registry.register_file(file=EXAMPLE_FILE)

    # The event informing about the new registration should have only been published
    # once:
    event_broadcaster.file_internally_registered.assert_awaited_once()


@pytest.mark.asyncio
async def test_reregistration_with_updated_metadata(
    s3_fixture: S3Fixture,  # noqa: F811
    mongodb_fixture: MongoDbFixture,  # noqa: F811
    file_fixture: FileObject,  # noqa: F811
):
    """Check that a re-registration of a file with updated metadata fails with the
    exptected exception."""

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

    # re-register the file with updated metadata:
    file_update = EXAMPLE_FILE.copy(update={"decrypted_size": 4321})
    with pytest.raises(FileRegistryPort.FileUpdateError):
        await file_registry.register_file(file=file_update)


@pytest.mark.asyncio
async def test_stage_non_existing_file(
    s3_fixture: S3Fixture,  # noqa: F811
    mongodb_fixture: MongoDbFixture,  # noqa: F811
):
    """Check that requesting to stage a non-registered file fails with the expected
    exception."""

    # populate the storage with empty storage entities:
    config = StorageEnitiesConfig(
        outbox_bucket="test-outbox",
        inbox_bucket="test-inbox",
        permanent_bucket="test-permanent",
    )
    await s3_fixture.populate_buckets(
        buckets=[config.outbox_bucket, config.inbox_bucket, config.permanent_bucket]
    )

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

    # request a stage for a non-registered file to the outbox:
    with pytest.raises(FileRegistryPort.FileNotInRegistryError):
        await file_registry.stage_registered_file(
            file_id="notregisteredfile001",
            decrypted_sha256=EXAMPLE_FILE.decrypted_sha256,
        )


@pytest.mark.asyncio
async def test_stage_checksum_missmatch(
    s3_fixture: S3Fixture,  # noqa: F811
    mongodb_fixture: MongoDbFixture,  # noqa: F811
    file_fixture: FileObject,  # noqa: F811
):
    """Check that requesting to stage a registered file by specifying the wrong checksum
    fails with the expected exception."""

    # populate the storage with the expected storage entities and place the content for
    # an example file in the permanent storage:
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

    # populate the database with a corresponding file metadata entry:
    file_metadata_dao = await FileMetadataDaoConstructor.construct(
        dao_factory=mongodb_fixture.dao_factory
    )
    await file_metadata_dao.insert(EXAMPLE_FILE)

    # Setup FileRegistry:
    content_copy_svc = ContentCopyService(
        config=config, object_storage=s3_fixture.storage
    )
    event_broadcaster = AsyncMock(spec=EventBroadcasterPort)
    file_registry = FileRegistry(
        content_copy_svc=content_copy_svc,
        file_metadata_dao=file_metadata_dao,
        event_broadcaster=event_broadcaster,
    )

    # request a stage for the registered file by specifying a wrong checksum:
    with pytest.raises(FileRegistryPort.ChecksumMissmatchError):
        await file_registry.stage_registered_file(
            file_id=EXAMPLE_FILE.file_id,
            decrypted_sha256=(
                "e6da6d6d05cc057964877aad8a3e9ad712c8abeae279dfa2f89b07eba7ef8abe"
            ),
        )


@pytest.mark.asyncio
async def test_storage_db_inconsistency(
    s3_fixture: S3Fixture,  # noqa: F811
    mongodb_fixture: MongoDbFixture,  # noqa: F811
):
    """Check that an inconsistency between the database and the storage, whereby the
    database contains a file metadata registration but the storage is missing the
    corresponding content, results in the expected exception."""

    # populate the storage with empty entities:
    config = StorageEnitiesConfig(
        outbox_bucket="test-outbox",
        inbox_bucket="test-inbox",
        permanent_bucket="test-permanent",
    )
    await s3_fixture.populate_buckets(
        buckets=[config.outbox_bucket, config.inbox_bucket, config.permanent_bucket]
    )

    # populate the database with metadata on an example file even though the storage is
    # empty:
    file_metadata_dao = await FileMetadataDaoConstructor.construct(
        dao_factory=mongodb_fixture.dao_factory
    )
    await file_metadata_dao.insert(EXAMPLE_FILE)

    # Setup FileRegistry:
    content_copy_svc = ContentCopyService(
        config=config, object_storage=s3_fixture.storage
    )
    event_broadcaster = AsyncMock(spec=EventBroadcasterPort)
    file_registry = FileRegistry(
        content_copy_svc=content_copy_svc,
        file_metadata_dao=file_metadata_dao,
        event_broadcaster=event_broadcaster,
    )

    # request a stage for the registered file by specifying a wrong checksum:
    with pytest.raises(FileRegistryPort.FileInRegistryButNotInStorageError):
        await file_registry.stage_registered_file(
            file_id=EXAMPLE_FILE.file_id, decrypted_sha256=EXAMPLE_FILE.decrypted_sha256
        )

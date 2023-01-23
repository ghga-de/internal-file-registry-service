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

"""Tests edge cases not covered by the typical journey test."""

import json

import pytest
from hexkit.providers.akafka.testutils import ExpectedEvent
from hexkit.providers.s3.testutils import file_fixture  # noqa: F401
from hexkit.providers.s3.testutils import FileObject

from ifrs.ports.inbound.file_registry import FileRegistryPort
from tests.fixtures.example_data import EXAMPLE_FILE
from tests.fixtures.joint import *  # noqa: F403


@pytest.mark.asyncio
async def test_register_with_empty_staging(
    joint_fixture: JointFixture,  # noqa: F811, F405
):
    """Test registration of a file when the file content is missing from the staging."""

    file_registry = await joint_fixture.container.file_registry()
    with pytest.raises(FileRegistryPort.FileContentNotInstagingError):
        await file_registry.register_file(file=EXAMPLE_FILE)


@pytest.mark.asyncio
async def test_reregistration(
    joint_fixture: JointFixture,  # noqa: F811, F405
    file_fixture: FileObject,  # noqa: F811
):
    """Test the re-registration of a file with identical metadata (should not result in
    an exception)."""

    # place example content in the staging:
    file_object = file_fixture.copy(
        update={
            "bucket_id": joint_fixture.config.staging_bucket,
            "object_id": EXAMPLE_FILE.file_id,
        }
    )
    await joint_fixture.s3.populate_file_objects(file_objects=[file_object])

    # register new file from the staging:
    # (And check if an event informing about the new registration has been published.)
    file_registry = await joint_fixture.container.file_registry()
    async with joint_fixture.kafka.expect_events(
        events=[
            ExpectedEvent(
                payload=json.loads(EXAMPLE_FILE.json()),
                type_=joint_fixture.config.file_registered_event_type,
            )
        ],
        in_topic=joint_fixture.config.file_registered_event_topic,
    ):
        await file_registry.register_file(file=EXAMPLE_FILE)

    # re-register the same file from the staging:
    # (A second event is not expected.)
    async with joint_fixture.kafka.expect_events(
        events=[],
        in_topic=joint_fixture.config.file_registered_event_topic,
    ):
        await file_registry.register_file(file=EXAMPLE_FILE)


@pytest.mark.asyncio
async def test_reregistration_with_updated_metadata(
    joint_fixture: JointFixture,  # noqa: F811, F405
    file_fixture: FileObject,  # noqa: F811
):
    """Check that a re-registration of a file with updated metadata fails with the
    exptected exception."""

    # place example content in the staging:
    file_object = file_fixture.copy(
        update={
            "bucket_id": joint_fixture.config.staging_bucket,
            "object_id": EXAMPLE_FILE.file_id,
        }
    )
    await joint_fixture.s3.populate_file_objects(file_objects=[file_object])

    # register new file from the staging:
    # (And check if an event informing about the new registration has been published.)
    file_registry = await joint_fixture.container.file_registry()
    async with joint_fixture.kafka.expect_events(
        events=[
            ExpectedEvent(
                payload=json.loads(EXAMPLE_FILE.json()),
                type_=joint_fixture.config.file_registered_event_type,
            )
        ],
        in_topic=joint_fixture.config.file_registered_event_topic,
    ):
        await file_registry.register_file(file=EXAMPLE_FILE)

    # try to re-register the same file with updated metadata:
    # (Expect and exception and no second event.)
    file_update = EXAMPLE_FILE.copy(update={"decrypted_size": 4321})
    async with joint_fixture.kafka.expect_events(
        events=[],
        in_topic=joint_fixture.config.file_registered_event_topic,
    ):
        with pytest.raises(FileRegistryPort.FileUpdateError):
            await file_registry.register_file(file=file_update)


@pytest.mark.asyncio
async def test_stage_non_existing_file(joint_fixture: JointFixture):  # noqa: F811, F405
    """Check that requesting to stage a non-registered file fails with the expected
    exception."""

    file_registry = await joint_fixture.container.file_registry()
    with pytest.raises(FileRegistryPort.FileNotInRegistryError):
        await file_registry.stage_registered_file(
            file_id="notregisteredfile001",
            decrypted_sha256=EXAMPLE_FILE.decrypted_sha256,
        )


@pytest.mark.asyncio
async def test_stage_checksum_missmatch(
    joint_fixture: JointFixture,  # noqa: F811, F405
    file_fixture: FileObject,  # noqa: F811
):
    """Check that requesting to stage a registered file by specifying the wrong checksum
    fails with the expected exception."""

    # place the content for an example file in the permanent storage:
    file_object = file_fixture.copy(
        update={
            "bucket_id": joint_fixture.config.staging_bucket,
            "object_id": EXAMPLE_FILE.file_id,
        }
    )
    await joint_fixture.s3.populate_file_objects(file_objects=[file_object])

    # populate the database with a corresponding file metadata entry:
    file_metadata_dao = await joint_fixture.container.file_metadata_dao()
    await file_metadata_dao.insert(EXAMPLE_FILE)

    # request a stage for the registered file by specifying a wrong checksum:
    file_registry = await joint_fixture.container.file_registry()
    with pytest.raises(FileRegistryPort.ChecksumMissmatchError):
        await file_registry.stage_registered_file(
            file_id=EXAMPLE_FILE.file_id,
            decrypted_sha256=(
                "e6da6d6d05cc057964877aad8a3e9ad712c8abeae279dfa2f89b07eba7ef8abe"
            ),
        )


@pytest.mark.asyncio
async def test_storage_db_inconsistency(
    joint_fixture: JointFixture,  # noqa: F811, F405
):
    """Check that an inconsistency between the database and the storage, whereby the
    database contains a file metadata registration but the storage is missing the
    corresponding content, results in the expected exception."""

    # populate the database with metadata on an example file even though the storage is
    # empty:
    file_metadata_dao = await joint_fixture.container.file_metadata_dao()
    await file_metadata_dao.insert(EXAMPLE_FILE)

    # request a stage for the registered file by specifying a wrong checksum:
    file_registry = await joint_fixture.container.file_registry()
    with pytest.raises(FileRegistryPort.FileInRegistryButNotInStorageError):
        await file_registry.stage_registered_file(
            file_id=EXAMPLE_FILE.file_id, decrypted_sha256=EXAMPLE_FILE.decrypted_sha256
        )

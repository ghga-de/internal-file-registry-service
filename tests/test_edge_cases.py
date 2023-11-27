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

"""Tests edge cases not covered by the typical journey test."""


import pytest
from hexkit.providers.s3.testutils import FileObject, file_fixture  # noqa: F401

from ifrs.ports.inbound.file_registry import FileRegistryPort
from tests.fixtures.example_data import EXAMPLE_METADATA, EXAMPLE_METADATA_BASE
from tests.fixtures.module_scope_fixtures import (  # noqa: F401
    JointFixture,
    event_loop,
    joint_fixture,
    kafka_fixture,
    mongodb_fixture,
    reset_state,
    s3_fixture,
    second_s3_fixture,
)


@pytest.mark.asyncio
async def test_register_with_empty_staging(joint_fixture: JointFixture):  # noqa: F811
    """Test registration of a file when the file content is missing from the staging."""
    with pytest.raises(FileRegistryPort.FileContentNotInStagingError):
        await joint_fixture.file_registry.register_file(
            file_without_object_id=EXAMPLE_METADATA_BASE,
            source_object_id="missing",
            source_bucket_id=joint_fixture.staging_bucket,
        )


@pytest.mark.asyncio
async def test_reregistration(
    joint_fixture: JointFixture,  # noqa: F811
    file_fixture: FileObject,  # noqa: F811
):
    """Test the re-registration of a file with identical metadata (should not result in
    an exception).
    """
    for s3, s3_endpoint_alias in (
        (joint_fixture.s3, joint_fixture.endpoint_aliases.node1),
        (joint_fixture.second_s3, joint_fixture.endpoint_aliases.node2),
    ):
        # place example content in the staging:
        file_object = file_fixture.model_copy(
            update={
                "bucket_id": joint_fixture.staging_bucket,
                "object_id": EXAMPLE_METADATA.object_id,
            }
        )
        await s3.populate_file_objects(file_objects=[file_object])

        # register new file from the staging:
        # (And check if an event informing about the new registration has been published.)
        file_metadata_base = EXAMPLE_METADATA_BASE.model_copy(
            update={"s3_endpoint_alias": s3_endpoint_alias}
        )

        async with joint_fixture.kafka.record_events(
            in_topic=joint_fixture.config.file_registered_event_topic
        ) as recorder:
            await joint_fixture.file_registry.register_file(
                file_without_object_id=file_metadata_base,
                source_object_id=EXAMPLE_METADATA.object_id,
                source_bucket_id=joint_fixture.staging_bucket,
            )

        assert len(recorder.recorded_events) == 1
        event = recorder.recorded_events[0]
        assert event.payload["object_id"] != ""
        assert event.type_ == joint_fixture.config.file_registered_event_type

        # re-register the same file from the staging:
        # (A second event is not expected.)
        async with joint_fixture.kafka.expect_events(
            events=[],
            in_topic=joint_fixture.config.file_registered_event_topic,
        ):
            await joint_fixture.file_registry.register_file(
                file_without_object_id=file_metadata_base,
                source_object_id=EXAMPLE_METADATA.object_id,
                source_bucket_id=joint_fixture.staging_bucket,
            )

        await joint_fixture.reset_state()


@pytest.mark.asyncio
async def test_reregistration_with_updated_metadata(
    joint_fixture: JointFixture,  # noqa: F811
    file_fixture: FileObject,  # noqa: F811
):
    """Check that a re-registration of a file with updated metadata fails with the
    expected exception.
    """
    for s3, s3_endpoint_alias in (
        (joint_fixture.s3, joint_fixture.endpoint_aliases.node1),
        (joint_fixture.second_s3, joint_fixture.endpoint_aliases.node2),
    ):
        # place example content in the staging:
        file_object = file_fixture.model_copy(
            update={
                "bucket_id": joint_fixture.staging_bucket,
                "object_id": EXAMPLE_METADATA.object_id,
            }
        )
        await s3.populate_file_objects(file_objects=[file_object])

        # register new file from the staging:
        # (And check if an event informing about the new registration has been published.)
        file_metadata_base = EXAMPLE_METADATA_BASE.model_copy(
            update={"s3_endpoint_alias": s3_endpoint_alias}
        )

        async with joint_fixture.kafka.record_events(
            in_topic=joint_fixture.config.file_registered_event_topic,
        ) as recorder:
            await joint_fixture.file_registry.register_file(
                file_without_object_id=file_metadata_base,
                source_object_id=EXAMPLE_METADATA.object_id,
                source_bucket_id=joint_fixture.staging_bucket,
            )

        assert len(recorder.recorded_events) == 1
        event = recorder.recorded_events[0]
        assert event.payload["object_id"] != ""
        assert event.type_ == joint_fixture.config.file_registered_event_type

        # try to re-register the same file with updated metadata:
        # (Expect an exception and no second event.)
        file_update = file_metadata_base.model_copy(update={"decrypted_size": 4321})
        async with joint_fixture.kafka.expect_events(
            events=[],
            in_topic=joint_fixture.config.file_registered_event_topic,
        ):
            with pytest.raises(FileRegistryPort.FileUpdateError):
                await joint_fixture.file_registry.register_file(
                    file_without_object_id=file_update,
                    source_object_id=EXAMPLE_METADATA.object_id,
                    source_bucket_id=joint_fixture.staging_bucket,
                )
        await joint_fixture.reset_state()


@pytest.mark.asyncio
async def test_stage_non_existing_file(joint_fixture: JointFixture):  # noqa: F811
    """Check that requesting to stage a non-registered file fails with the expected
    exception.
    """
    with pytest.raises(FileRegistryPort.FileNotInRegistryError):
        await joint_fixture.file_registry.stage_registered_file(
            file_id="notregisteredfile001",
            decrypted_sha256=EXAMPLE_METADATA_BASE.decrypted_sha256,
            target_object_id=EXAMPLE_METADATA.object_id,
            target_bucket_id=joint_fixture.outbox_bucket,
        )


@pytest.mark.asyncio
async def test_stage_checksum_mismatch(
    joint_fixture: JointFixture,  # noqa: F811
    file_fixture: FileObject,  # noqa: F811
):
    """Check that requesting to stage a registered file to the outbox by specifying the
    wrong checksum fails with the expected exception.
    """
    # populate the database with a corresponding file metadata entry:
    await joint_fixture.file_metadata_dao.insert(EXAMPLE_METADATA)

    for s3, s3_endpoint_alias in (
        (joint_fixture.s3, joint_fixture.endpoint_aliases.node1),
        (joint_fixture.second_s3, joint_fixture.endpoint_aliases.node2),
    ):
        bucket_id = joint_fixture.config.object_storages[s3_endpoint_alias].bucket
        # place the content for an example file in the permanent storage:
        file_object = file_fixture.model_copy(
            update={
                "bucket_id": bucket_id,
                "object_id": EXAMPLE_METADATA.object_id,
            }
        )
        await s3.populate_file_objects(file_objects=[file_object])

        # request a stage for the registered file to the outbox by specifying a wrong checksum:
        with pytest.raises(FileRegistryPort.ChecksumMismatchError):
            await joint_fixture.file_registry.stage_registered_file(
                file_id=EXAMPLE_METADATA_BASE.file_id,
                decrypted_sha256=(
                    "e6da6d6d05cc057964877aad8a3e9ad712c8abeae279dfa2f89b07eba7ef8abe"
                ),
                target_object_id=EXAMPLE_METADATA.object_id,
                target_bucket_id=joint_fixture.outbox_bucket,
            )


@pytest.mark.asyncio
async def test_storage_db_inconsistency(
    joint_fixture: JointFixture,  # noqa: F811
):
    """Check that an inconsistency between the database and the storage, whereby the
    database contains a file metadata registration but the storage is missing the
    corresponding content, results in the expected exception.
    """
    # populate the database with metadata on an example file even though the storage is
    # empty:
    await joint_fixture.file_metadata_dao.insert(EXAMPLE_METADATA)

    # request a stage for the registered file by specifying a wrong checksum:
    with pytest.raises(FileRegistryPort.FileInRegistryButNotInStorageError):
        await joint_fixture.file_registry.stage_registered_file(
            file_id=EXAMPLE_METADATA_BASE.file_id,
            decrypted_sha256=EXAMPLE_METADATA_BASE.decrypted_sha256,
            target_object_id=EXAMPLE_METADATA.object_id,
            target_bucket_id=joint_fixture.outbox_bucket,
        )

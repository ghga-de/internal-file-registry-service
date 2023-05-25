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

"""Tests typical user journeys"""

import json

import pytest
import requests
from hexkit.providers.akafka.testutils import ExpectedEvent
from hexkit.providers.s3.testutils import file_fixture  # noqa: F401
from hexkit.providers.s3.testutils import FileObject

from tests.fixtures.example_data import EXAMPLE_FILE
from tests.fixtures.joint import *  # noqa: F403


@pytest.mark.asyncio
async def test_happy_journey(
    joint_fixture: JointFixture,  # noqa: F811, F405
    file_fixture: FileObject,  # noqa: F811
):
    """Simulates a typical, successful journey for upload and download."""

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

    # check that the file content is now in both the staging and the permanent storage:
    assert await joint_fixture.s3.storage.does_object_exist(
        bucket_id=joint_fixture.config.staging_bucket, object_id=EXAMPLE_FILE.file_id
    )
    assert await joint_fixture.s3.storage.does_object_exist(
        bucket_id=joint_fixture.config.permanent_bucket, object_id=EXAMPLE_FILE.file_id
    )

    # request a stage to the outbox:
    async with joint_fixture.kafka.expect_events(
        events=[
            ExpectedEvent(
                payload={
                    "file_id": EXAMPLE_FILE.file_id,
                    "decrypted_sha256": EXAMPLE_FILE.decrypted_sha256,
                },
                type_=joint_fixture.config.file_staged_event_type,
            )
        ],
        in_topic=joint_fixture.config.file_staged_event_topic,
    ):
        await file_registry.stage_registered_file(
            file_id=EXAMPLE_FILE.file_id, decrypted_sha256=EXAMPLE_FILE.decrypted_sha256
        )

    # check that the file content is now in all three storage entities:
    assert await joint_fixture.s3.storage.does_object_exist(
        bucket_id=joint_fixture.config.staging_bucket, object_id=EXAMPLE_FILE.file_id
    )
    assert await joint_fixture.s3.storage.does_object_exist(
        bucket_id=joint_fixture.config.permanent_bucket, object_id=EXAMPLE_FILE.file_id
    )
    assert await joint_fixture.s3.storage.does_object_exist(
        bucket_id=joint_fixture.config.outbox_bucket, object_id=EXAMPLE_FILE.file_id
    )

    # check that the file content in the outbox is identical to the content in the
    # staging:
    download_url = await joint_fixture.s3.storage.get_object_download_url(
        bucket_id=joint_fixture.config.outbox_bucket, object_id=EXAMPLE_FILE.file_id
    )
    response = requests.get(download_url)
    response.raise_for_status()
    assert response.content == file_object.content


@pytest.mark.asyncio
async def test_happy_deletion(
    joint_fixture: JointFixture,  # noqa: F811, F405
    file_fixture: FileObject,  # noqa: F811
):
    """Simulates a typical, successful journey for file deletion."""

    # place example content in the staging:
    file_object = file_fixture.copy(
        update={
            "bucket_id": joint_fixture.config.permanent_bucket,
            "object_id": EXAMPLE_FILE.file_id,
        }
    )
    await joint_fixture.s3.populate_file_objects(file_objects=[file_object])

    file_registry = await joint_fixture.container.file_registry()

    # request a stage to the outbox:
    async with joint_fixture.kafka.expect_events(
        events=[
            ExpectedEvent(
                payload={
                    "file_id": EXAMPLE_FILE.file_id,
                },
                type_=joint_fixture.config.file_deleted_event_type,
            )
        ],
        in_topic=joint_fixture.config.file_deleted_event_topic,
    ):
        await file_registry.delete_file(file_id=EXAMPLE_FILE.file_id)

    assert not await joint_fixture.s3.storage.does_object_exist(
        bucket_id=joint_fixture.config.permanent_bucket, object_id=EXAMPLE_FILE.file_id
    )

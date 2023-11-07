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

"""Specialized service managing copy operations of files between buckets."""

from pydantic import Field
from pydantic_settings import BaseSettings

from ifrs.core import models
from ifrs.ports.inbound.content_copy import ContentCopyServicePort
from ifrs.ports.outbound.storage import ObjectStoragePort


class StorageEntitiesConfig(BaseSettings):
    """A config for specifying the location of major storage entities."""

    permanent_bucket: str = Field(
        ...,
        description=(
            "The ID of the object storage bucket that is serving as permanent storage."
        ),
        examples=["permanent"],
    )


class ContentCopyService(ContentCopyServicePort):
    """A service that copies the content of a file between storage
    entities.
    """

    def __init__(
        self,
        *,
        config: StorageEntitiesConfig,
        object_storage: ObjectStoragePort,
    ):
        """Initialize with essential config params and outbound adapters."""
        self._config = config
        self._object_storage = object_storage

    async def staging_to_permanent(
        self, *, file: models.FileMetadata, source_object_id: str, source_bucket_id: str
    ) -> None:
        """Copy a file from an staging stage to the permanent storage."""
        if await self._object_storage.does_object_exist(
            bucket_id=self._config.permanent_bucket, object_id=file.object_id
        ):
            # the content is already where it should go, there is nothing to do
            return

        if not await self._object_storage.does_object_exist(
            bucket_id=source_bucket_id, object_id=source_object_id
        ):
            raise self.ContentNotInStagingError(file_id=file.file_id)

        await self._object_storage.copy_object(
            source_bucket_id=source_bucket_id,
            source_object_id=source_object_id,
            dest_bucket_id=self._config.permanent_bucket,
            dest_object_id=file.object_id,
        )

    async def permanent_to_outbox(
        self, *, file: models.FileMetadata, target_object_id: str, target_bucket_id: str
    ) -> None:
        """Copy a file from an staging stage to the permanent storage."""
        if await self._object_storage.does_object_exist(
            bucket_id=target_bucket_id, object_id=target_object_id
        ):
            # the content is already where it should go, there is nothing to do
            return

        if not await self._object_storage.does_object_exist(
            bucket_id=self._config.permanent_bucket, object_id=file.object_id
        ):
            raise self.ContentNotInPermanentStorageError(file_id=file.file_id)

        await self._object_storage.copy_object(
            source_bucket_id=self._config.permanent_bucket,
            source_object_id=file.object_id,
            dest_bucket_id=target_bucket_id,
            dest_object_id=target_object_id,
        )

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

"""Application core-internal interfaces."""

from pydantic import BaseSettings, Field

from ifrs.core import models
from ifrs.core.interfaces import IContentCopyService
from ifrs.ports.outbound.storage import ObjectStoragePort


class StorageEnitiesConfig(BaseSettings):
    """A config for specifying the location of the major storage enities."""

    outbox_bucket: str = Field(
        ...,
        description=(
            "The ID of the object storage bucket that is serving as download area."
        ),
        example="outbox",
    )
    inbox_bucket: str = Field(
        ...,
        description=(
            "The ID of the object storage bucket that is serving as upload area."
        ),
        example="inbox",
    )
    permanent_bucket: str = Field(
        ...,
        description=(
            "The ID of the object storage bucket that is serving as permanent storage."
        ),
        example="permanent",
    )


class ContentCopyService(IContentCopyService):
    """A service that copies the content of a file between storage
    entities."""

    def __init__(
        self,
        *,
        config: StorageEnitiesConfig,
        object_storage: ObjectStoragePort,
    ):
        """Initialize with essential config params and outbound adapters."""

        self._config = config
        self._object_storage = object_storage

    async def inbox_to_permanent(self, *, file: models.FileMetadata) -> None:
        """Copy a file from an inbox stage to the permanent storage."""

        if await self._object_storage.does_object_exist(
            bucket_id=self._config.permanent_bucket, object_id=file.file_id
        ):
            # the content is already where it should go, there is nothing to do
            return

        if not await self._object_storage.does_object_exist(
            bucket_id=self._config.inbox_bucket, object_id=file.file_id
        ):
            raise self.ContentNotInInboxError(file_id=file.file_id)

        await self._object_storage.copy_object(
            source_bucket_id=self._config.inbox_bucket,
            source_object_id=file.file_id,
            dest_bucket_id=self._config.permanent_bucket,
            dest_object_id=file.file_id,
        )

    async def permanent_to_outbox(self, *, file: models.FileMetadata) -> None:
        """Copy a file from an inbox stage to the permanent storage."""

        if await self._object_storage.does_object_exist(
            bucket_id=self._config.outbox_bucket, object_id=file.file_id
        ):
            # the content is already where it should go, there is nothing to do
            return

        if not await self._object_storage.does_object_exist(
            bucket_id=self._config.permanent_bucket, object_id=file.file_id
        ):
            raise self.ContentNotInPermanentStorageError(file_id=file.file_id)

        await self._object_storage.copy_object(
            source_bucket_id=self._config.permanent_bucket,
            source_object_id=file.file_id,
            dest_bucket_id=self._config.outbox_bucket,
            dest_object_id=file.file_id,
        )

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

"""Main business-logic of this service"""
import uuid
from contextlib import suppress

from ghga_service_commons.utils.multinode_storage import ObjectStorages

from ifrs.config import Config
from ifrs.core import models
from ifrs.ports.inbound.file_registry import FileRegistryPort
from ifrs.ports.outbound.dao import FileMetadataDaoPort, ResourceNotFoundError
from ifrs.ports.outbound.event_pub import EventPublisherPort


class FileRegistry(FileRegistryPort):
    """A service that manages a registry files stored on a permanent object storage."""

    def __init__(
        self,
        *,
        file_metadata_dao: FileMetadataDaoPort,
        event_publisher: EventPublisherPort,
        object_storages: ObjectStorages,
        config: Config,
    ):
        """Initialize with essential config params and outbound adapters."""
        self._event_publisher = event_publisher
        self._file_metadata_dao = file_metadata_dao
        self._object_storages = object_storages
        self._config = config

    async def _is_file_registered(
        self, *, file_without_object_id: models.FileMetadataBase
    ) -> bool:
        """Checks if the specified file is already registered. There are three possible
        outcomes:
            - Yes, the file has been registered with metadata that is identical to the
              provided one => returns `True`
            - Yes, however, the metadata differs => raises self.FileUpdateError
            - No, the file has not been registered, yet => returns `False`
        """
        try:
            registered_file = await self._file_metadata_dao.get_by_id(
                file_without_object_id.file_id
            )
        except ResourceNotFoundError:
            return False

        # object ID is a UUID generated upon registration, so cannot compare those
        registered_file_without_object_id = registered_file.model_dump(
            exclude={"object_id"}
        )

        if file_without_object_id.model_dump() == registered_file_without_object_id:
            return True

        raise self.FileUpdateError(file_id=file_without_object_id.file_id)

    async def register_file(
        self,
        *,
        file_without_object_id: models.FileMetadataBase,
        source_object_id: str,
        source_bucket_id: str,
    ) -> None:
        """Registers a file and moves its content from the staging into the permanent
        storage. If the file with that exact metadata has already been registered,
        nothing is done.

        Args:
            file_without_object_id: metadata on the file to register.
            source_object_id:
                The S3 object ID for the staging bucket.
            source_bucket_id:
                The S3 bucket ID for staging.

        Raises:
            self.FileUpdateError:
                When the file already been registered but its metadata differs from the
                provided one.
            self.FileContentNotInStagingError:
                When the file content is not present in the storage staging.
        """
        if await self._is_file_registered(
            file_without_object_id=file_without_object_id
        ):
            # There is nothing to do:
            return

        # Generate & assign object ID to metadata
        object_id = str(uuid.uuid4())
        file = models.FileMetadata(
            **file_without_object_id.model_dump(), object_id=object_id
        )

        permanent_bucket_id, object_storage = self._object_storages.for_alias(
            file.s3_endpoint_alias
        )

        if not await object_storage.does_object_exist(
            bucket_id=source_bucket_id, object_id=source_object_id
        ):
            raise self.FileContentNotInStagingError(
                file_id=file_without_object_id.file_id
            )

        await object_storage.copy_object(
            source_bucket_id=source_bucket_id,
            source_object_id=source_object_id,
            dest_bucket_id=permanent_bucket_id,
            dest_object_id=object_id,
        )

        await self._file_metadata_dao.insert(file)

        permanent_bucket, _ = self._object_storages.for_alias(file.s3_endpoint_alias)

        await self._event_publisher.file_internally_registered(
            file=file, bucket_id=permanent_bucket
        )

    async def stage_registered_file(
        self,
        *,
        file_id: str,
        decrypted_sha256: str,
        target_object_id: str,
        target_bucket_id: str,
    ) -> None:
        """Stage a registered file to the outbox.

        Args:
            file_id:
                The identifier of the file.
            decrypted_sha256:
                The checksum of the decrypted content. This is used to make sure that
                this service and the outside client are talking about the same file.
            target_object_id:
                The S3 object ID for the outbox bucket.
            target_bucket_id:
                The S3 bucket ID for the outbox.
            s3_endpoint_alias:
                The label of the object storage configuration to use

        Raises:
            self.FileNotInRegistryError:
                When a file is requested that has not (yet) been registered.
            self.ChecksumMismatchError:
                When the provided checksum did not match the expectations.
            self.FileInRegistryButNotInStorageError:
                When encountering inconsistency between the registry (the database) and
                the permanent storage. This is an internal service error, which should
                not happen, and not the fault of the client.
        """
        try:
            file = await self._file_metadata_dao.get_by_id(file_id)
        except ResourceNotFoundError as error:
            raise self.FileNotInRegistryError(file_id=file_id) from error

        if decrypted_sha256 != file.decrypted_sha256:
            raise self.ChecksumMismatchError(
                file_id=file_id,
                provided_checksum=decrypted_sha256,
                expected_checksum=file.decrypted_sha256,
            )

        permanent_bucket_id, object_storage = self._object_storages.for_alias(
            file.s3_endpoint_alias
        )

        if await object_storage.does_object_exist(
            bucket_id=target_bucket_id, object_id=target_object_id
        ):
            # the content is already where it should go, there is nothing to do
            return

        if not await object_storage.does_object_exist(
            bucket_id=permanent_bucket_id, object_id=file.object_id
        ):
            raise self.FileInRegistryButNotInStorageError(file_id=file_id)

        await object_storage.copy_object(
            source_bucket_id=permanent_bucket_id,
            source_object_id=file.object_id,
            dest_bucket_id=target_bucket_id,
            dest_object_id=target_object_id,
        )

        await self._event_publisher.file_staged_for_download(
            file_id=file_id,
            decrypted_sha256=decrypted_sha256,
            target_object_id=target_object_id,
            target_bucket_id=target_bucket_id,
            s3_endpoint_alias=file.s3_endpoint_alias,
        )

    async def delete_file(self, *, file_id: str) -> None:
        """Deletes a file from the permanent storage and the internal database.
        If no file with that id exists, do nothing.

        Args:
            file_id:
                id for the file to delete.
        """
        try:
            file = await self._file_metadata_dao.get_by_id(file_id)
        except ResourceNotFoundError:
            # resource not in database, nothing to do
            return

        # Get object ID and storage instance
        object_id = file.object_id
        bucket_id, object_storage = self._object_storages.for_alias(
            file.s3_endpoint_alias
        )

        # Try to remove file from S3
        with suppress(object_storage.ObjectNotFoundError):
            # If file does not exist anyways, we are done.
            await object_storage.delete_object(bucket_id=bucket_id, object_id=object_id)

        # Try to remove file from database
        with suppress(ResourceNotFoundError):
            # If file does not exist anyways, we are done.
            await self._file_metadata_dao.delete(id_=file_id)

        await self._event_publisher.file_deleted(file_id=file_id)

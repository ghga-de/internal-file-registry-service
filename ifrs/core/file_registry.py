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

"""Main business-logic of this service"""

from ifrs.core import models
from ifrs.core.interfaces import IContentCopyService
from ifrs.ports.inbound.file_registry import FileRegistryPort
from ifrs.ports.outbound.dao import FileMetadataDaoPort, ResourceNotFoundError
from ifrs.ports.outbound.event_pub import EventPublisherPort


class FileRegistry(FileRegistryPort):
    """A service that manages a registry files stored on a permanent object storage."""

    def __init__(
        self,
        *,
        content_copy_svc: IContentCopyService,
        file_metadata_dao: FileMetadataDaoPort,
        event_publisher: EventPublisherPort,
    ):
        """Initialize with essential config params and outbound adapters."""

        self._content_copy_svc = content_copy_svc
        self._event_publisher = event_publisher
        self._file_metadata_dao = file_metadata_dao

    async def _is_file_registered(self, *, file: models.FileMetadata) -> bool:
        """Checks if the specified file is already registered. There are three possible
        outcomes:
            - Yes, the file has been registered with metadata that is identical to the
              provided one => returns `True`
            - Yes, however, the metadata differs => raises self.FileUpdateError
            - No, the file has not been registered, yet => returns `False`
        """

        try:
            registered_file = await self._file_metadata_dao.get_by_id(file.file_id)
        except ResourceNotFoundError:
            return False

        if file == registered_file:
            return True

        raise self.FileUpdateError(file_id=file.file_id)

    async def register_file(self, *, file: models.FileMetadata) -> None:
        """Registers a file and moves its content from the staging into the permanent
        storage. If the file with that exact metadata has already been registered,
        nothing is done.

        Args:
            file: metadata on the file to register.

        Raises:
            self.FileUpdateError:
                When the file already been registered but its metadata differes from the
                provided one.
            self.FileContentNotInstagingError:
                When the file content is not present in the storage staging.
        """

        if await self._is_file_registered(file=file):
            # There is nothing to do:
            return

        try:
            await self._content_copy_svc.staging_to_permanent(file=file)
        except IContentCopyService.ContentNotInstagingError as error:
            raise self.FileContentNotInstagingError(file_id=file.file_id) from error

        await self._file_metadata_dao.insert(file)

        await self._event_publisher.file_internally_registered(file=file)

    async def stage_registered_file(
        self, *, file_id: str, decrypted_sha256: str
    ) -> None:
        """Stage a registered file to the outbox.

        Args:
            file_id:
                The identifier of the file.
            decrypted_sha256:
                The checksum of the decrypted content. This is used to make sure that
                this service and the outside client are talking about the same file.

        Raises:
            self.FileNotInRegistryError:
                When a file is requested that has not (yet) been registered.
            self.ChecksumMissmatchError:
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
            raise self.ChecksumMissmatchError(
                file_id=file_id,
                provided_checksum=decrypted_sha256,
                expected_checksum=file.decrypted_sha256,
            )

        try:
            await self._content_copy_svc.permanent_to_outbox(file=file)

            await self._event_publisher.file_staged_for_download(
                file_id=file_id, decrypted_sha256=decrypted_sha256
            )

        except IContentCopyService.ContentNotInPermanentStorageError as error:
            raise self.FileInRegistryButNotInStorageError(file_id=file_id) from error

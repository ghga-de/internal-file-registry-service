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

"""Main business-logic of this service"""

from ifrs.core import models
from ifrs.ports.inbound.file_registry import FileRegistryPort


class DataRepositoryConfig(BaseSettings):
    """Config parameters needed for the DataRepository."""

    outbox_bucket: str
    inbox_bucket: str
    permanent_bucket: str


class FileRegistryPort(FileRegistryPort):
    """A service that manages a registry files stored on a permanent object storage."""

    def __init__(
        self,
        *,
        config: DataRepositoryConfig,
        drs_object_dao: DrsObjectDaoPort,
        object_storage: ObjectStoragePort,
        event_broadcaster: DrsEventBroadcasterPort,
    ):
        """Initialize with essential config params and outbound adapters."""

        self._config = config
        self._event_broadcaster = event_broadcaster
        self._drs_object_dao = drs_object_dao
        self._object_storage = object_storage

    async def register_file(self, *, file: models.FileMetadata):
        """Registers a file and moves its content from the inbox into the permanent
        storage. If the file with that exact metadata has already been registered,
        nothing is done.

        Args:
            file: metadata on the file to register.

        Raises:
            self.FileUploadError:
                When the file already been registered but its metadata differes from the
                provided one.
            self.FileNotInInboxError:
                When the content of the file to be registered cannot be found in the
                storage inbox.
        """
        ...

    async def stage_registered_file(self, *, file_id: str, decrypted_sha256: str):
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
            self.FileInRegistryButNotInStorageError:
                When encountering inconsistency between the registry (the database) and
                the permanent storage. This is an internal service error, which should
                not happen, and not the fault of the client.
        """
        ...


def stage_file(external_file_id: str, config: Config = CONFIG) -> FileInfoInitial:
    """Copies a file into the stage bucket."""

    # get file info from the database:
    with Database(config=config) as database:
        try:
            file_info = database.get_file_info(external_file_id)
        except FileInfoNotFoundError as error:
            raise FileNotInRegistryError(external_file_id=external_file_id) from error

    # copy file object to the stage bucket:
    with ObjectStorage(config=config) as storage:
        try:
            storage.copy_object(
                source_bucket_id=file_info.grouping_label,
                source_object_id=external_file_id,
                dest_bucket_id=config.s3_outbox_bucket_id,
                dest_object_id=external_file_id,
            )
        except ObjectNotFoundError as error:
            raise FileInDbButNotInStorageError(
                external_file_id=external_file_id
            ) from error
        except ObjectAlreadyExistsError as error:
            raise FileAlreadyInOutboxError(external_file_id=external_file_id) from error

    return file_info


def register_file(file_info: FileInfoInitial, config: Config = CONFIG) -> None:
    """Register a new file to the database and copy it from a stage bucket (e.g. inbox)
    to the permanent file storage."""

    # get file info from the database:
    # (will throw an error if file not in registry)
    with Database(config=config) as database:
        with ObjectStorage(config=config) as storage:
            try:
                database.register_file_info(file_info)
            except FileInfoAlreadyExistsError as error:
                if not storage.does_object_exist(
                    bucket_id=file_info.grouping_label, object_id=file_info.file_id
                ):
                    raise FileInDbButNotInStorageError(
                        external_file_id=file_info.file_id
                    ) from error
                raise FileAlreadyInRegistryError(
                    external_file_id=file_info.file_id
                ) from error

            # copy file object to the stage bucket:

            if not storage.does_bucket_exist(bucket_id=file_info.grouping_label):
                storage.create_bucket(bucket_id=file_info.grouping_label)

            try:
                storage.copy_object(
                    source_bucket_id=config.s3_inbox_bucket_id,
                    source_object_id=file_info.file_id,
                    dest_bucket_id=file_info.grouping_label,
                    dest_object_id=file_info.file_id,
                )
            except ObjectNotFoundError as error:
                raise FileNotInInboxError(external_file_id=file_info.file_id) from error
            except ObjectAlreadyExistsError as error:
                raise FileInStorageButNotInDbError(
                    external_file_id=file_info.file_id
                ) from error

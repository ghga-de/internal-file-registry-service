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

from ghga_service_chassis_lib.object_storage_dao import ObjectNotFoundError

from ..config import CONFIG, Config
from ..dao import (
    Database,
    FileInfoAlreadyExistsError,
    FileInfoNotFoundError,
    ObjectAlreadyExistsError,
    ObjectStorage,
)
from .models import FileInfoInitial


class FileAlreadyInOutboxError(RuntimeError):
    """Thrown when there was a stage request for an already staged file."""

    def __init__(self, external_file_id: str):
        message = f"The file with external id {external_file_id} is already staged."
        super().__init__(message)


class FileAlreadyInRegistryError(RuntimeError):
    """Thrown when trying to register file that is already in storage."""

    def __init__(self, external_file_id: str):
        message = (
            f"The file with external id {external_file_id} is already in the"
            + " internal file registry."
        )
        super().__init__(message)


class FileNotInRegistryError(RuntimeError):
    """Thrown when a file is requested that is not (yet) in the database."""

    def __init__(self, external_file_id: str):
        message = (
            f"The file with external id {external_file_id} does not exist in the"
            + " internal file registry."
        )
        super().__init__(message)


class FileNotInInboxError(RuntimeError):
    """Thrown when a file is expected in the inbox but isn't there."""

    def __init__(self, external_file_id: str):
        message = (
            f"The file with external id {external_file_id} does not exist in the"
            + " inbox."
        )
        super().__init__(message)


class DbAndStorageInconsistencyError(RuntimeError):
    """Thrown when the database and the storage are in an inconsistent state that
    needs manual intervention."""

    pass  # pylint: disable=unnecessary-pass


class FileInDbButNotInStorageError(DbAndStorageInconsistencyError):
    """Thrown if file is registered to the DB but is not in the storage."""

    def __init__(self, external_file_id: str):
        message = (
            f"File with id {external_file_id} has been registered to the database but"
            + " does not exist in the permanent object storage: "
        )
        super().__init__(message)


class FileInStorageButNotInDbError(DbAndStorageInconsistencyError):
    """Thrown if file is present in storage but is not registered to the DB."""

    def __init__(self, external_file_id: str):
        message = (
            f"File with id {external_file_id} does exist in the storage but has not"
            + " been registered to the database."
        )
        super().__init__(message)


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

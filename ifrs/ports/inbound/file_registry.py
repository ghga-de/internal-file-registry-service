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

"""Interface for managing a internal registry of files."""

from abc import ABC, abstractmethod

from ifrs.core import models


class FileRegistryPort(ABC):
    """The interface of a service that manages a registry files stored on a permanent
    object storage."""

    class FileUpdateError(RuntimeError):
        """Thrown when attempting to update metadata of an existing file."""

        def __init__(self, file_id: str):
            message = (
                f"The file with the ID '{file_id}' has already been registered and the "
                + " provided metadata is not identical to the existing one. Updates are"
                + " not permitted."
            )
            super().__init__(message)

    class FileNotInInboxError(RuntimeError):
        """Thrown when a file is unexpectedly not in the inbox storage."""

        def __init__(self, file_id: str):
            message = f"The file with id '{file_id}' does not exist in the inbox."
            super().__init__(message)

    class FileNotInRegistryError(RuntimeError):
        """Thrown when a file is requested that has not (yet) been registered."""

        def __init__(self, file_id: str):
            message = f"The file with the ID '{file_id}' has not (yet) been registered."
            super().__init__(message)

    class FileInRegistryButNotInStorageError(RuntimeError):
        """Thrown if a file is registered (metadata is present in the database) but its
        content is not present in the permanent storage."""

        def __init__(self, file_id: str):
            message = (
                f"The file with the ID '{file_id}' has been registered but its content"
                + " does not exist in the permanent object storage."
            )
            super().__init__(message)

    @abstractmethod
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

    @abstractmethod
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

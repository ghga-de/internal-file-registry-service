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

from abc import ABC, abstractmethod

from ifrs.core import models


class IContentCopyService(ABC):
    """Interface for a service that copies the content of a file between storage
    entities."""

    class ContentNotInInboxError(RuntimeError):
        """Thrown when the content of a file is unexpectedly not in the inbox storage."""

        def __init__(self, file_id: str):
            message = (
                f"The content of the file with id '{file_id}' does not exist in the"
                + " inbox."
            )
            super().__init__(message)

    class ContentNotInPermanentStorageError(RuntimeError):
        """Thrown when the content of a file is unexpectedly not in the permanent
        storage."""

        def __init__(self, file_id: str):
            message = (
                f"The content of the file with id '{file_id}' does not exist in the"
                + " inbox."
            )
            super().__init__(message)

    @abstractmethod
    async def inbox_to_permanent(self, *, file: models.FileMetadata) -> None:
        """Copy a file from an inbox stage to the permanent storage."""
        ...

    @abstractmethod
    async def permanent_to_outbox(self, *, file: models.FileMetadata) -> None:
        """Copy a file from an inbox stage to the permanent storage."""
        ...

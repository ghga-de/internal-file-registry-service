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

"""Adapter for receiving events providing metadata on files"""

from ghga_event_schemas import pydantic_ as event_schemas
from ghga_event_schemas.validation import get_validated_payload
from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.eventsub import EventSubscriberProtocol
from pydantic import BaseSettings, Field

from ifrs.core import models
from ifrs.ports.inbound.file_registry import FileRegistryPort


class EventSubTranslatorConfig(BaseSettings):
    """Config for receiving events providing metadata on new files to register."""

    files_to_register_topic: str = Field(
        ...,
        description="The name of the topic to receive events informing about new files "
        + "to register.",
        example="file_interogation",
    )
    files_to_register_type: str = Field(
        ...,
        description="The type used for events informing about new files to register.",
        example="files_interrogation_success",
    )

    files_to_stage_topic: str = Field(
        ...,
        description="The name of the topic to receive events informing about files to stage.",
        example="file_downloads",
    )
    files_to_stage_type: str = Field(
        ...,
        description="The type used for events informing about a file to be staged.",
        example="file_stage_requested",
    )


class EventSubTranslator(EventSubscriberProtocol):
    """A triple hexagonal translator compatible with the EventSubscriberProtocol that
    is used to receive metadata on new files to register."""

    def __init__(
        self,
        config: EventSubTranslatorConfig,
        file_registry: FileRegistryPort,
    ):
        """Initialize with config parameters and core dependencies."""

        self.topics_of_interest = [
            config.files_to_register_topic,
            config.files_to_stage_topic,
        ]
        self.types_of_interest = [
            config.files_to_register_type,
            config.files_to_stage_type,
        ]

        self._file_registry = file_registry
        self._config = config

    async def _consume_files_to_register(self, *, payload: JsonObject) -> None:
        """Consume file registration events."""

        validated_payload = get_validated_payload(
            payload=payload, schema=event_schemas.FileUploadValidationSuccess
        )

        file = models.FileMetadata(
            file_id=validated_payload.file_id,
            decrypted_sha256=validated_payload.decrypted_sha256,
            decrypted_size=validated_payload.decrypted_size,
            upload_date=validated_payload.upload_date,
            decryption_secret_id=validated_payload.decryption_secret_id,
            encrypted_part_size=validated_payload.encrypted_part_size,
            encrypted_parts_md5=validated_payload.encrypted_parts_md5,
            encrypted_parts_sha256=validated_payload.encrypted_parts_sha256,
            content_offset=validated_payload.content_offset,
        )

        await self._file_registry.register_file(file=file)

    async def _consume_file_downloads(self, *, payload: JsonObject) -> None:
        """Consume file download events."""

        validated_payload = get_validated_payload(
            payload=payload, schema=event_schemas.NonStagedFileRequested
        )

        await self._file_registry.stage_registered_file(
            file_id=validated_payload.file_id,
            decrypted_sha256=validated_payload.decrypted_sha256,
        )

    async def _consume_validated(
        self,
        *,
        payload: JsonObject,
        type_: Ascii,
        topic: Ascii,  # pylint: disable=unused-argument
    ) -> None:
        """Consume events from the topics of interest."""

        if type_ == self._config.files_to_register_type:
            await self._consume_files_to_register(payload=payload)
        if type_ == self._config.files_to_stage_type:
            await self._consume_file_downloads(payload=payload)
        else:
            raise RuntimeError(f"Unexpected event of type: {type_}")

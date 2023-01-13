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

"""Adapter for publishing events to other services."""

import json

from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.protocols.eventpub import EventPublisherProtocol
from pydantic import BaseSettings, Field

from ifrs.core import models
from ifrs.ports.outbound.event_pub import EventPublisherPort


class EventPubTranslatorConfig(BaseSettings):
    """Config for publishing internal events to the outside."""

    file_registered_event_topic: str = Field(
        "internal_file_registry",
        description=(
            "Name of the topic used for events indicating that a new file has"
            + " been internally registered."
        ),
    )
    file_registered_event_type: str = Field(
        "file_registered",
        description=(
            "The type used for events indicating that a new file has"
            + " been internally registered."
        ),
    )
    file_staged_event_topic: str = Field(
        "internal_file_registry",
        description=(
            "Name of the topic used for events indicating that a new file has"
            + " been internally registered."
        ),
    )
    file_staged_event_type: str = Field(
        "file_staged_for_download",
        description=(
            "The type used for events indicating that a new file has"
            + " been internally registered."
        ),
    )


class EventPubTranslator(EventPublisherPort):
    """A translator according to  the triple hexagonal architecture implementing
    the EventPublisherPort."""

    def __init__(
        self, *, config: EventPubTranslatorConfig, provider: EventPublisherProtocol
    ):
        """Initialize with configs and a provider of the EventPublisherProtocol."""

        self._config = config
        self._provider = provider

    async def file_internally_registered(self, *, file: models.FileMetadata) -> None:
        """Communicates the event that a new file has been internally registered."""

        payload = event_schemas.FileInternallyRegistered(
            file_id=file.file_id,
            decrypted_sha256=file.decrypted_sha256,
            decrypted_size=file.decrypted_size,
            decryption_secret_id=file.decryption_secret_id,
            content_offset=file.content_offset,
            encrypted_part_size=file.encrypted_part_size,
            encrypted_parts_md5=file.encrypted_parts_md5,
            encrypted_parts_sha256=file.encrypted_parts_sha256,
            upload_date=file.upload_date,
        )
        payload_dict = json.loads(payload.json())

        await self._provider.publish(
            payload=payload_dict,
            type_=self._config.file_registered_event_type,
            topic=self._config.file_registered_event_topic,
            key=file.file_id,
        )

    async def file_staged_for_download(
        self, *, file_id: str, decrypted_sha256: str
    ) -> None:
        """Communicates the event that a new file has been internally registered."""

        payload = event_schemas.FileStagedForDownload(
            file_id=file_id,
            decrypted_sha256=decrypted_sha256,
        )
        payload_dict = json.loads(payload.json())

        await self._provider.publish(
            payload=payload_dict,
            type_=self._config.file_staged_event_type,
            topic=self._config.file_staged_event_topic,
            key=file_id,
        )

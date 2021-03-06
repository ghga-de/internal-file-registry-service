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

"""Publish messages/events to async messaging topics."""

from ghga_message_schemas import schemas
from ghga_service_chassis_lib.pubsub import AmqpTopic

from .. import models
from ..config import CONFIG, Config


def publish_file_info_generic(
    topic_name: str,
    message_schema: dict,
    file_info: models.FileInfoInitial,
    config: Config = CONFIG,
):
    """A generic function to publish file infos as message to specified topic name."""

    topic = AmqpTopic(
        config=config,
        topic_name=topic_name,
        json_schema=message_schema,
    )

    message = {
        "file_id": file_info.file_id,
        "grouping_label": file_info.grouping_label,
        "md5_checksum": file_info.md5_checksum,
        "creation_date": file_info.creation_date.isoformat(),
        "update_date": file_info.update_date.isoformat(),
        "size": file_info.size,
        "format": file_info.format,
    }

    topic.publish(message)


def publish_upon_file_stage(file_info: models.FileInfoInitial, config: Config = CONFIG):
    """Publish an event/message informing that a new file was staged."""

    publish_file_info_generic(
        topic_name=config.topic_name_staged_to_outbox,
        message_schema=schemas.SCHEMAS["file_staged_for_download"],
        file_info=file_info,
        config=config,
    )


def publish_upon_registration(
    file_info: models.FileInfoInitial, config: Config = CONFIG
):
    """Publish an event/message informing that a new file was successfully registered."""

    publish_file_info_generic(
        topic_name=config.topic_name_registered,
        message_schema=schemas.SCHEMAS["file_internally_registered"],
        file_info=file_info,
        config=config,
    )

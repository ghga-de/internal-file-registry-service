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

"""Subscribe to async messaging topics to receive messages/events."""

from typing import Any, Dict

from ghga_message_schemas import schemas
from ghga_service_chassis_lib.pubsub import AmqpTopic

from .. import models
from ..config import CONFIG, Config
from ..core import FileAlreadyInOutboxError, register_file, stage_file
from .pub import publish_upon_file_stage, publish_upon_registration


def message_to_file_info(message: Dict[str, Any]) -> models.FileInfoExternal:
    """Convert message to models.FileInfoExternal"""

    return models.FileInfoExternal(
        file_id=message["file_id"],
        grouping_label=message["grouping_label"],
        md5_checksum=message["md5_checksum"],
        creation_date=message["creation_date"],
        update_date=message["update_date"],
        size=message["size"],
        format=message["format"],
    )


def handle_stage_request(message: Dict[str, Any], config: Config = CONFIG) -> None:
    """Work on a stage request coming from the corresponding messaging topic."""

    file_info = message_to_file_info(message)

    try:
        stage_file(external_file_id=file_info.file_id, config=config)
    except FileAlreadyInOutboxError:
        # This is not really an error (it uccurs when multiple stage requests are
        # comming in shortly after each other) and there is nothing to do.
        return

    publish_upon_file_stage(
        file_info=file_info,
        config=config,
    )


def handle_registration_request(
    message: Dict[str, Any], config: Config = CONFIG
) -> None:
    """Work on a registration request coming from the corresponding messaging topic."""

    file_info = message_to_file_info(message)

    register_file(file_info=file_info, config=config)

    publish_upon_registration(
        file_info=file_info,
        config=config,
    )


def subscribe_stage_requests(config: Config = CONFIG, run_forever: bool = True) -> None:
    """
    Subscribe to topic that informs whenever a file needs to be staged.
    This function is blocking and infinitily waits for new messages.
    """

    topic = AmqpTopic(
        config=config,
        topic_name=config.topic_name_stage_request,
        json_schema=schemas.SCHEMAS["non_staged_file_requested"],
    )

    topic.subscribe(
        exec_on_message=lambda message: handle_stage_request(message, config=config),
        run_forever=run_forever,
    )


def subscribe_registration_request(
    config: Config = CONFIG, run_forever: bool = True
) -> None:
    """
    Subscribe to topic that informs whenever a new file should be registered.
    """

    topic = AmqpTopic(
        config=config,
        topic_name=config.topic_name_reg_request,
        json_schema=schemas.SCHEMAS["file_internally_registered"],
    )

    topic.subscribe(
        exec_on_message=lambda message: handle_registration_request(
            message, config=config
        ),
        run_forever=run_forever,
    )

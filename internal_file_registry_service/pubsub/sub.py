# Copyright 2021 Universität Tübingen, DKFZ and EMBL
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

import pika
from ghga_service_chassis_lib.pubsub import AmqpTopic

from ..config import CONFIG, Config
from ..core.main import FileAlreadyOnStageError, stage_file
from . import schemas
from .pub import publish_file_staged


def handle_stage_request(message: Dict[str, Any], config: Config = CONFIG) -> None:
    """Work on a stage request coming from the corresponding messaging topic."""

    try:
        stage_file(external_file_id=message["file_id"], config=config)
    except FileAlreadyOnStageError:
        # This is not really an error and there is nothing to do.
        return

    publish_file_staged(
        request_id=message["request_id"],
        external_file_id=message["file_id"],
        grouping_label=message["grouping_label"],
    )


def subscribe_stage_requests(config: Config = CONFIG) -> None:
    """
    Subscribe to topic that informs whenever a file needs to be staged.
    This function is blocking and infinitily waits for new messages.
    """

    topic = AmqpTopic(
        connection_params=pika.ConnectionParameters(
            host=config.rabbitmq_host, port=config.rabbitmq_port
        ),
        topic_name=config.topic_name_non_staged_file_requested,
        service_name=config.service_name,
        json_schema=schemas.NON_STAGED_FILE_REQUESTED,
    )

    topic.subscribe_for_ever(
        exec_on_message=lambda message: handle_stage_request(message, config=config)
    )

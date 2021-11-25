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

"""Publish messages/events to async messaging topics."""

from datetime import datetime

import pika
from ghga_service_chassis_lib.pubsub import AmqpTopic

from ..config import CONFIG, Config
from . import schemas


def publish_file_staged(
    request_id: str, external_file_id: str, grouping_label: str, config: Config = CONFIG
):
    """Publish an event/message informing that a new file was staged."""

    topic = AmqpTopic(
        connection_params=pika.ConnectionParameters(
            host=config.rabbitmq_host, port=config.rabbitmq_port
        ),
        topic_name=config.topic_name_file_staged_for_download,
        service_name=config.service_name,
        json_schema=schemas.FILE_STAGED_FOR_DOWNLOAD,
    )

    message = {
        "request_id": request_id,
        "file_id": external_file_id,
        "grouping_label": grouping_label,
        "timestamp": datetime.now(),
    }

    topic.publish(message)

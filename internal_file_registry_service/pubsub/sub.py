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
        config=config,
    )


def subscribe_stage_requests(config: Config = CONFIG, run_forever: bool = True) -> None:
    """
    Subscribe to topic that informs whenever a file needs to be staged.
    This function is blocking and infinitily waits for new messages.
    """

    topic = AmqpTopic(
        config=config,
        topic_name=config.topic_name_non_staged_file_requested,
        json_schema=schemas.NON_STAGED_FILE_REQUESTED,
    )

    topic.subscribe(
        exec_on_message=lambda message: handle_stage_request(message, config=config),
        run_forever=run_forever,
    )

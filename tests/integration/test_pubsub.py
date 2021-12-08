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

"""Test the messaging API (pubsub)"""

from ghga_service_chassis_lib.utils import exec_with_timeout

from internal_file_registry_service.pubsub import schemas, subscribe_stage_requests

from ..fixtures import (  # noqa: F401
    FILE_FIXTURES,
    amqp_fixture,
    get_config,
    psql_fixture,
    s3_fixture,
)


def test_subscribe_stage_requests(psql_fixture, s3_fixture, amqp_fixture):  # noqa: F811
    """Test `subscribe_stage_requests` function"""
    config = get_config(
        sources=[psql_fixture.config, s3_fixture.config, amqp_fixture.config]
    )
    upstream_message = FILE_FIXTURES["in_registry"].message

    # initialize upstream and downstream test services that will publish or receive
    # messages to or from this service:
    upstream_publisher = amqp_fixture.get_test_publisher(
        topic_name=config.topic_name_non_staged_file_requested,
        message_schema=schemas.NON_STAGED_FILE_REQUESTED,
    )

    downstream_subscriber = amqp_fixture.get_test_subscriber(
        topic_name=config.topic_name_file_staged_for_download,
        message_schema=schemas.FILE_STAGED_FOR_DOWNLOAD,
    )

    # publish a stage request:
    upstream_publisher.publish(upstream_message)

    # process the stage request:
    exec_with_timeout(
        func=lambda: subscribe_stage_requests(config=config, run_forever=False),
        timeout_after=2,
    )

    # expect stage confirmation message:
    downstream_message = downstream_subscriber.subscribe(timeout_after=2)
    assert downstream_message["file_id"] == upstream_message["file_id"]

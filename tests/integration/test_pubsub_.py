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

from ..fixtures import amqp_fixture  # noqa: F401


def test_pub_sub(amqp_fixture):
    """Test `subscribe_stage_requests` function"""
    topic_name = "test"
    message = {"test": "test"}

    publisher = amqp_fixture.get_test_publisher(topic_name=topic_name)

    publisher.publish(message)

    subscriber = amqp_fixture.get_test_subscriber(topic_name=topic_name)
    subscriber.subscribe(expected_message=message, timeout_after=10)
